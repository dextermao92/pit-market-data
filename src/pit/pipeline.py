"""One collection run: snapshot -> raw -> staged, recorded in pipeline_runs (success or failure)."""

import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from pit.ibkr_options import run
from pit.stage_options import TS, stage_file, write_parquet

QUOTE_STATUSES = ("two_sided", "ask_only", "bid_only", "no_quote", "crossed")
IBKR_INFO_CODES = {2104, 2106, 2107, 2108, 2119, 2158}  # data-farm status messages, not errors
RUN_SCHEMA = {
    "run_id": pl.String, "dataset": pl.String, "started_at": TS, "finished_at": TS, "status": pl.String,
    "error": pl.String, "raw_path": pl.String, "staged_path": pl.String, "cycle_s": pl.Float64,
    "contracts": pl.Int64, **{s: pl.Int64 for s in QUOTE_STATUSES}, "ibkr_error_codes": pl.List(pl.Int64),
}


def collect_option_snapshots(config_path: str, run_id: str | None = None) -> dict:
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    started = datetime.now(timezone.utc)
    rec = {"run_id": run_id or f"cli__{started:%Y%m%dT%H%M%SZ}", "dataset": "option_snapshots",
           "started_at": started, "status": "failed"}
    try:
        raw = run(config_path)
        staged, df = stage_file(raw)
        payload = json.loads(raw.read_text())
        counts = Counter(df["quote_status"].to_list())
        rec |= {
            "status": "success",
            "raw_path": str(raw),
            "staged_path": str(staged),
            "cycle_s": payload["duration_s"],
            "contracts": df.height,
            **{s: counts[s] for s in QUOTE_STATUSES},
            "ibkr_error_codes": sorted({e["code"] for e in payload["errors"]} - IBKR_INFO_CODES),
        }
        return rec
    except Exception as e:
        rec["error"] = repr(e)[:1000]
        raise
    finally:
        rec["finished_at"] = datetime.now(timezone.utc)
        path = (data_dir / "pipeline_runs" / "dataset=option_snapshots" / f"ingest_date={started:%Y-%m-%d}"
                / f"{started:%Y%m%dT%H%M%S%fZ}.parquet")
        write_parquet(pl.DataFrame([rec], schema=RUN_SCHEMA), path)
