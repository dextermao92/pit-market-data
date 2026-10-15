"""Snapshot index levels (config [[indices]]: VIX term structure, SKEW, VVIX, ...) into raw/ and stage them.

Runs beside the chain snapshot on its own client id (ibkr.client_id + 1); a handful of market-data lines.
"""

import json
import os
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from ib_async import Index

from pit.ibkr_options import clean, connect, wait_for_ticks
from pit.rules import apply
from pit.storage import TS, write_raw, write_staged

FIELDS = ("last", "close", "bid", "ask", "marketDataType")
SCHEMA = {"ingested_at": TS, "symbol": pl.String, "exchange": pl.String, "price": pl.Float64, "last": pl.Float64,
          "close": pl.Float64, "bid": pl.Float64, "ask": pl.Float64, "market_data_type": pl.Int8, "tick_time": TS}


def run(config_path: str) -> Path:
    cfg = tomllib.loads(Path(config_path).read_text())
    ingested_at, t0 = datetime.now(timezone.utc), time.monotonic()
    ib, errors = connect(cfg["ibkr"], client_offset=1)
    try:
        contracts = ib.qualifyContracts(*[Index(i["symbol"], i["exchange"], "USD") for i in cfg["indices"]])
        tickers = [ib.reqMktData(c) for c in contracts]
        wait_for_ticks(ib, cfg["ibkr"], tickers, lambda t: clean(t.marketPrice()) or clean(t.close))
        rows = [{"symbol": t.contract.symbol, "exchange": t.contract.exchange, "price": clean(t.marketPrice()),
                 **{f: clean(getattr(t, f)) for f in FIELDS}, "tick_time": t.time.isoformat() if t.time else None}
                for t in tickers]
        for c in contracts:
            ib.cancelMktData(c)
    finally:
        ib.disconnect()
    payload = {"ingested_at": ingested_at.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
               "requested": [i["symbol"] for i in cfg["indices"]], "indices": rows, "errors": errors}
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "ibkr", "index_snapshots", payload, ingested_at)


def _value(x):
    return None if x in (-1, None) else x  # IBKR's -1 = no value


def to_frame(payload: dict) -> pl.DataFrame:
    at = datetime.fromisoformat(payload["ingested_at"])
    return pl.DataFrame([{
        "ingested_at": at, "symbol": r["symbol"], "exchange": r["exchange"], "price": _value(r["price"]),
        "last": _value(r["last"]), "close": _value(r["close"]), "bid": _value(r["bid"]), "ask": _value(r["ask"]),
        "market_data_type": r["marketDataType"],
        "tick_time": datetime.fromisoformat(r["tick_time"]) if r["tick_time"] else None,
    } for r in payload["indices"]], schema=SCHEMA)


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    staged, quarantined = apply(
        to_frame(json.loads(raw_path.read_text())),
        errors={"required_fields": pl.col("symbol").is_null(), "duplicate_index": pl.col("symbol").is_duplicated()},
        # Indices only calculate in their sessions; outside them only the previous close is there.
        warns={"no_live_price": pl.col("price").is_null() & pl.col("last").is_null()},
    )
    return write_staged(raw_path, staged, quarantined), staged, quarantined


if __name__ == "__main__":
    print(run(sys.argv[1]))
