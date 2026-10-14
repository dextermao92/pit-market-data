"""Alpaca intraday bars (bar size in config): fetch the session's completed bars so far into raw/, and stage raw -> Parquet.

Every run refetches the whole session so far, so vendor revisions show up as new rows.
Usage: python -m pit.alpaca_bars config/alpaca_bars.toml
"""

import json
import os
import sys
import time
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import exchange_calendars as xcals
import httpx
import pandas as pd
import polars as pl

from pit.storage import TS, staged_path, write_parquet, write_raw

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


def fetch_pages(client: httpx.Client, params: dict) -> list[dict]:
    pages, token = [], None
    while True:
        r = client.get("/v2/stocks/bars", params=params | ({"page_token": token} if token else {}))
        r.raise_for_status()
        pages.append(r.json())
        token = pages[-1].get("next_page_token")
        if not token:
            return pages


def run(config_path: str, now: datetime | None = None) -> Path:
    cfg = tomllib.loads(Path(config_path).read_text())["alpaca"]
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    cal = xcals.get_calendar("XNYS")
    session = cal.date_to_session(pd.Timestamp(now).tz_convert("America/New_York").date(), "previous")
    first, last = window(
        now, cal.session_open(session).to_pydatetime(), cal.session_close(session).to_pydatetime(),
        timedelta(minutes=cfg["bar_minutes"]), timedelta(minutes=cfg["lag_minutes"]),
    )
    if last < first:
        raise ValueError(f"no completed bars yet in session {session.date()}")

    params = {
        "symbols": ",".join(cfg["symbols"]), "timeframe": f"{cfg['bar_minutes']}Min",
        "start": first.isoformat(), "end": last.isoformat(),  # end is inclusive on bar start
        "feed": cfg["feed"], "adjustment": "raw", "limit": 10000,
    }
    headers = {"APCA-API-KEY-ID": os.environ["APCA_API_KEY_ID"], "APCA-API-SECRET-KEY": os.environ["APCA_API_SECRET_KEY"]}
    with httpx.Client(base_url=cfg["base_url"], headers=headers, timeout=30) as client:
        pages = fetch_pages(client, params)

    payload = {
        "ingested_at": now.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
        "session": str(session.date()), "request": params, "pages": pages,
    }
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "alpaca", "equity_bars", payload, now)


def to_frame(payload: dict) -> pl.DataFrame:
    ingested_at = datetime.fromisoformat(payload["ingested_at"])
    rows = [
        {"ingested_at": ingested_at, "symbol": sym, "bar_start": datetime.fromisoformat(b["t"]),
         "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"],
         "trade_count": b["n"], "vwap": b["vw"]}
        for page in payload["pages"] for sym, bars in (page.get("bars") or {}).items() for b in bars
    ]
    return pl.DataFrame(rows, schema=SCHEMA)


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame]:
    df = to_frame(json.loads(raw_path.read_text()))
    return write_parquet(df, staged_path(raw_path)), df


if __name__ == "__main__":
    print(run(sys.argv[1]))
