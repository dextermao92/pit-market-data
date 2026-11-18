"""Alpaca corporate actions: the full history for the configured symbols on every run.

A few pages per day, and it means a revised or newly announced action shows up as a new version (new
ingested_at) instead of being missed. Alpaca filters start/end on process_date, not ex_date.
Only splits and dividends are staged; other types (mergers, spin-offs, name changes) stay in raw.
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
from pit.rules import check_corporate_actions
from pit.storage import TS, write_raw, write_staged

HISTORY_START = "2016-01-01"  # Alpaca's SIP bar history starts in 2016
SCHEMA = {
    "ingested_at": TS, "id": pl.String, "symbol": pl.String, "type": pl.String, "ex_date": pl.Date,
    "process_date": pl.Date,
    "price_factor": pl.Float64,  # multiply prices before ex_date by this (splits, stock dividends)
    "cash": pl.Float64,          # cash dividend per share
    "special": pl.Boolean,
}


def run(config_path: str, now: datetime | None = None) -> Path:
    cfg = tomllib.loads(Path(config_path).read_text(encoding="utf-8"))["alpaca"]
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    params = {"symbols": ",".join(cfg["symbols"]), "start": HISTORY_START,
              "end": str(now.date() + timedelta(days=365)), "limit": 1000}
    with client() as c:
        pages = fetch_pages(c, "/v1/corporate-actions", params)
    payload = {"ingested_at": now.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
               "request": params, "pages": pages}
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


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    staged, quarantined = check_corporate_actions(to_frame(json.loads(raw_path.read_text(encoding="utf-8"))))
    return write_staged(raw_path, staged, quarantined), staged, quarantined


if __name__ == "__main__":
    print(run(sys.argv[1]))
