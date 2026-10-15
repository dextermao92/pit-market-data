"""Alpaca asset attributes per configured symbol, snapshotted daily: the source of the SCD2 security master.

Uses the trading API host (read-only GET of /v2/assets). Keyed by Alpaca's asset id, which survives renames.
"""

import json
import os
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import httpx
import polars as pl

from pit.rules import apply
from pit.storage import TS, write_raw, write_staged

TRADING_API = "https://paper-api.alpaca.markets"  # same keys; assets are identical on paper and live
SCHEMA = {"ingested_at": TS, "asset_id": pl.String, "symbol": pl.String, "name": pl.String, "exchange": pl.String,
          "asset_class": pl.String, "status": pl.String, "tradable": pl.Boolean}


def run(config_path: str, now: datetime | None = None) -> Path:
    symbols = tomllib.loads(Path(config_path).read_text())["alpaca"]["symbols"]
    now = now or datetime.now(timezone.utc)
    t0 = time.monotonic()
    headers = {"APCA-API-KEY-ID": os.environ["APCA_API_KEY_ID"], "APCA-API-SECRET-KEY": os.environ["APCA_API_SECRET_KEY"]}
    assets, missing = [], []
    with httpx.Client(base_url=TRADING_API, headers=headers, timeout=30) as c:
        for s in symbols:
            r = c.get(f"/v2/assets/{s}")
            if r.status_code == 404:
                missing.append(s)  # delisted or renamed away: recorded, not fatal
                continue
            r.raise_for_status()
            assets.append(r.json())
    payload = {"ingested_at": now.isoformat(), "duration_s": round(time.monotonic() - t0, 1),
               "assets": assets, "missing": missing}
    return write_raw(Path(os.environ.get("DATA_DIR", "data")), "alpaca", "assets", payload, now)


def to_frame(payload: dict) -> pl.DataFrame:
    at = datetime.fromisoformat(payload["ingested_at"])
    return pl.DataFrame([{"ingested_at": at, "asset_id": a.get("id"), "symbol": a.get("symbol"), "name": a.get("name"),
                          "exchange": a.get("exchange"), "asset_class": a.get("class"), "status": a.get("status"),
                          "tradable": a.get("tradable")} for a in payload["assets"]], schema=SCHEMA)


def stage_file(raw_path: Path) -> tuple[Path, pl.DataFrame, pl.DataFrame]:
    staged, quarantined = apply(
        to_frame(json.loads(raw_path.read_text())),
        errors={"required_fields": pl.any_horizontal(pl.col(c).is_null() for c in ("asset_id", "symbol"))},
        warns={"not_tradable": (pl.col("status") != "active") | ~pl.col("tradable")},
    )
    return write_staged(raw_path, staged, quarantined), staged, quarantined


if __name__ == "__main__":
    print(run(sys.argv[1]))
