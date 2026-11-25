"""Alpaca intraday bars (bar size in config): fetch the session's completed bars so far into raw/, and stage raw -> Parquet.

Every run refetches the whole session so far, so vendor revisions show up as new rows. It also fetches the
previous session's last bar and stores it in the same raw file, so the overnight-split rule needs nothing else.
Usage: python -m pit.alpaca_bars config/alpaca_bars.toml
"""

import json
import os
import sys
import time
import tomllib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd
import polars as pl

from pit.market_hours import calendar
from pit.rules import check_bars
from pit.storage import TS, write_raw, write_staged

SCHEMA = {
    "ingested_at": TS, "symbol": pl.String, "bar_start": TS, "open": pl.Float64, "high": pl.Float64,
    "low": pl.Float64, "close": pl.Float64, "volume": pl.Int64, "trade_count": pl.Int64, "vwap": pl.Float64,
}


def window(now: datetime, open_: datetime, close: datetime, bar: timedelta, lag: timedelta) -> tuple[datetime, datetime]:
    """[first, last] bar starts in the session whose interval ended at least `lag` before `now`."""
    available = now - lag
    step = int(bar.total_seconds())
    last = datetime.fromtimestamp(int(available.timestamp()) // step * step, tz=timezone.utc) - bar
    return open_, min(last, close - bar)


def client() -> httpx.Client:
    return httpx.Client(base_url="https://data.alpaca.markets", timeout=30, headers={
        "APCA-API-KEY-ID": os.environ["APCA_API_KEY_ID"], "APCA-API-SECRET-KEY": os.environ["APCA_API_SECRET_KEY"]})


_last_request = 0.0  # monotonic time of this process's last request, for pacing


def fetch_pages(client: httpx.Client, path: str, params: dict, min_interval: float = 0.0,
                max_rate_limited: int = 5) -> list[dict]:
    """Every page of a paged request. min_interval paces requests process-wide (seconds between two); a 429
    waits until the vendor's rate-limit reset (or a minute) and retries the same page, up to max_rate_limited."""
    global _last_request
    pages, token, limited = [], None, 0
    while True:
        wait = _last_request + min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _last_request = time.monotonic()
        r = client.get(path, params=params | ({"page_token": token} if token else {}))
        if r.status_code == 429 and limited < max_rate_limited:
            limited += 1
            reset = r.headers.get("x-ratelimit-reset")
            time.sleep(min(60.0, max(1.0, float(reset) - time.time())) if reset else 60.0)
            continue
        r.raise_for_status()
        pages.append(r.json())
        token = pages[-1].get("next_page_token")
        if not token:
            return pages


def run(config_path: str, now: datetime | None = None, session: date | None = None) -> Path:
    """Completed bars of `session` (default: the current/last session) so far. A past session gives the
    whole day; ingested_at is always the real fetch time, also for refetches and backfills."""
    cfg = tomllib.loads(Path(config_path).read_text(encoding="utf-8"))["alpaca"]
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    cal = calendar()
    session = pd.Timestamp(session) if session else cal.date_to_session(
        pd.Timestamp(now).tz_convert("America/New_York").date(), "previous")
    bar = timedelta(minutes=cfg["bar_minutes"])
    first, last = window(
        now, cal.session_open(session).to_pydatetime(), cal.session_close(session).to_pydatetime(),
        bar, timedelta(minutes=cfg["lag_minutes"]),
    )
    prev_last = cal.session_close(cal.previous_session(session)).to_pydatetime() - bar
    if last < first:
        raise ValueError(f"no completed bars yet in session {session.date()}")

    params = _params(cfg, f"{cfg['bar_minutes']}Min", first.isoformat(), last.isoformat())  # end inclusive
    prev_params = params | {"start": prev_last.isoformat(), "end": prev_last.isoformat()}
    with client() as c:
        pages = fetch_pages(c, "/v2/stocks/bars", params)
        prev_pages = fetch_pages(c, "/v2/stocks/bars", prev_params)
    payload = {
        "ingested_at": now.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
        "session": str(session.date()), "request": params, "pages": pages, "prev_pages": prev_pages,
    }
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "alpaca", "equity_bars", payload, now)


def run_daily(config_path: str, sessions: list[date], now: datetime | None = None,
              symbols: list[str] | None = None) -> Path:
    """Vendor daily bars for `sessions`: the configured symbols (used to reconcile aggregated 5-min bars in dbt),
    or `symbols` for whole-market fetches (pit.universe), paced by [universe] requests_per_minute."""
    raw_cfg = tomllib.loads(Path(config_path).read_text(encoding="utf-8"))
    cfg = raw_cfg["alpaca"] | ({"symbols": symbols} if symbols is not None else {})
    pace = 60 / raw_cfg["universe"]["requests_per_minute"] if symbols is not None and "universe" in raw_cfg else 0.0
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    # A date-only end means "end of that day", which hits the free plan's 15-minute limit for today.
    end = calendar().session_close(pd.Timestamp(max(sessions))).isoformat()
    params = _params(cfg, "1Day", str(min(sessions)), end)
    with client() as c:
        pages = fetch_pages(c, "/v2/stocks/bars", params, min_interval=pace)
    payload = {"ingested_at": now.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
               "request": params, "pages": pages}
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "alpaca", "daily_bars", payload, now)


def _params(cfg: dict, timeframe: str, start: str, end: str) -> dict:
    return {"symbols": ",".join(cfg["symbols"]), "timeframe": timeframe, "start": start, "end": end,
            "feed": cfg["feed"], "adjustment": "raw", "limit": 10000}


def to_frame(payload: dict, pages_key: str = "pages") -> pl.DataFrame:
    ingested_at = datetime.fromisoformat(payload["ingested_at"])
    rows = [
        {"ingested_at": ingested_at, "symbol": sym, "bar_start": datetime.fromisoformat(b["t"]),
         "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"],
         "trade_count": b["n"], "vwap": b["vw"]}
        for page in payload.get(pages_key, []) for sym, bars in (page.get("bars") or {}).items() for b in bars
    ]
    return pl.DataFrame(rows, schema=SCHEMA)


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    prev_close = to_frame(payload, "prev_pages").select("symbol", prev_close="close")  # absent in early raw files
    staged, quarantined = check_bars(to_frame(payload), prev_close, intraday=raw_path.parent.name == "equity_bars")
    return write_staged(raw_path, staged, quarantined), staged, quarantined


if __name__ == "__main__":
    print(run(sys.argv[1]))
