"""Alpaca corporate actions for the whole market (no symbol filter).

Daily: process dates from WINDOW_DAYS ago to a year ahead, so newly announced and recently revised actions show up
as new versions (new ingested_at). With full=True (eod on Fridays): the whole history from 2016, to catch late
revisions of older actions without storing ~130 pages every day. Alpaca filters start/end on process_date, not
ex_date. Each raw file stages twice: `corporate_actions` (splits and dividends, the price adjustments) and
`corporate_events` (every type but cash dividends: renames, mergers, removals, unit splits..., for identity).
"""

import json
import os
import sys
import time
import tomllib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from pit.alpaca_bars import client, fetch_pages
from pit.rules import check_corporate_actions, check_corporate_events
from pit.storage import TS, write_raw, write_staged

HISTORY_START = "2016-01-01"  # Alpaca's SIP bar history starts in 2016
WINDOW_DAYS = 30              # daily fetch: process dates from this many days back
SCHEMA = {
    "ingested_at": TS, "id": pl.String, "symbol": pl.String, "type": pl.String, "ex_date": pl.Date,
    "process_date": pl.Date,
    "price_factor": pl.Float64,  # multiply prices before ex_date by this (splits, stock dividends)
    "cash": pl.Float64,          # cash dividend per share
    "special": pl.Boolean,
}
EVENT_SCHEMA = {
    "ingested_at": TS, "id": pl.String, "type": pl.String, "symbol": pl.String, "new_symbol": pl.String,
    "alt_symbol": pl.String, "cusip": pl.String, "new_cusip": pl.String, "date": pl.Date, "process_date": pl.Date,
    "rate": pl.Float64, "cash": pl.Float64,
}


def run(config_path: str, now: datetime | None = None, full: bool = False) -> Path:
    cfg = tomllib.loads(Path(config_path).read_text(encoding="utf-8"))
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    start = HISTORY_START if full else str(now.date() - timedelta(days=WINDOW_DAYS))
    params = {"start": start, "end": str(now.date() + timedelta(days=365)), "limit": 1000}
    with client() as c:
        pages = fetch_pages(c, "/v1/corporate-actions", params,
                            min_interval=60 / cfg.get("universe", {}).get("requests_per_minute", 150))
    payload = {"ingested_at": now.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
               "scope": "full" if full else "window", "request": params, "pages": pages}
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "alpaca", "corporate_actions", payload, now)


def _d(s):
    return date.fromisoformat(s) if s else None


def to_frame(payload: dict) -> pl.DataFrame:
    ingested_at = datetime.fromisoformat(payload["ingested_at"])
    rows = []
    for page in payload["pages"]:
        acts = page.get("corporate_actions") or {}
        for kind, a in [(k, a) for k in ("forward_splits", "reverse_splits", "stock_dividends", "cash_dividends")
                        for a in acts.get(k, [])]:
            if kind in ("forward_splits", "reverse_splits"):
                factor, cash = a["old_rate"] / a["new_rate"], None   # 10:1 forward -> 0.1; 1:10 reverse -> 10
            elif kind == "stock_dividends":
                factor, cash = 1 / (1 + a["rate"]), None
            else:
                factor, cash = None, a["rate"]
            rows.append({"ingested_at": ingested_at, "id": a["id"], "symbol": a["symbol"], "type": kind[:-1],
                         "ex_date": _d(a.get("ex_date")), "process_date": _d(a.get("process_date")),
                         "price_factor": factor, "cash": cash, "special": a.get("special", False)})
    return pl.DataFrame(rows, schema=SCHEMA)


def _first(a: dict, *keys: str):
    return next((a[k] for k in keys if a.get(k) not in (None, "")), None)


def _ratio(a: dict, num: str, den: str) -> float | None:
    return a[num] / a[den] if a.get(num) is not None and a.get(den) else None


def events_frame(payload: dict) -> pl.DataFrame:
    """Every action type except cash dividends, one row per action, for security identity (pit.identity):
    `symbol` is what the action is about (old symbol, acquiree, spin-off source), `new_symbol` what follows it
    (new symbol, acquirer, spun-off company), `date` when it takes effect. `rate` is new shares per old share,
    `cash` cash per old share. The raw keeps every vendor field."""
    ingested_at = datetime.fromisoformat(payload["ingested_at"])
    rows = []
    for page in payload["pages"]:
        for kind, actions in (page.get("corporate_actions") or {}).items():
            if kind == "cash_dividends":
                continue
            for a in actions:
                rate = (_ratio(a, "new_rate", "old_rate") or _ratio(a, "acquirer_rate", "acquiree_rate")
                        or _ratio(a, "new_rate", "source_rate") or a.get("rate"))
                cash = a.get("cash_rate") if kind != "cash_mergers" else a.get("rate")
                rows.append({
                    "ingested_at": ingested_at, "id": a["id"], "type": kind[:-1],
                    "symbol": _first(a, "old_symbol", "acquiree_symbol", "source_symbol", "symbol"),
                    "new_symbol": _first(a, "new_symbol", "acquirer_symbol"),
                    "alt_symbol": a.get("alternate_symbol"),
                    "cusip": _first(a, "old_cusip", "acquiree_cusip", "source_cusip", "cusip"),
                    "new_cusip": _first(a, "new_cusip", "acquirer_cusip"),
                    "date": _d(_first(a, "effective_date", "ex_date", "process_date")),
                    "process_date": _d(a.get("process_date")),
                    "rate": None if kind == "cash_mergers" else rate,
                    "cash": cash if cash is not None else a.get("price"),
                })
    return pl.DataFrame(rows, schema=EVENT_SCHEMA)


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    events, bad_events = check_corporate_events(events_frame(payload))
    write_staged(raw_path, events, bad_events, dataset="corporate_events")
    staged, quarantined = check_corporate_actions(to_frame(payload))
    return write_staged(raw_path, staged, quarantined), staged, quarantined


if __name__ == "__main__":
    print(run(sys.argv[1]))
