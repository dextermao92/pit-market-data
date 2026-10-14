"""One collection run per dataset: fetch -> raw -> staged, recorded in pipeline_runs (success or failure)."""

import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from pit import alpaca_bars, ibkr_options, stage_options
from pit.storage import TS, write_parquet

QUOTE_STATUSES = ("two_sided", "ask_only", "bid_only", "no_quote", "crossed")
IBKR_INFO_CODES = {2104, 2106, 2107, 2108, 2119, 2158}  # data-farm status messages, not errors
RUN_SCHEMA = {
    "run_id": pl.String, "dataset": pl.String, "started_at": TS, "finished_at": TS, "status": pl.String,
    "error": pl.String, "raw_path": pl.String, "staged_path": pl.String, "cycle_s": pl.Float64, "rows": pl.Int64,
    **{s: pl.Int64 for s in QUOTE_STATUSES}, "ibkr_error_codes": pl.List(pl.Int64),  # option snapshots only
}


def _recorded(dataset: str, run_id: str | None, collect) -> dict:
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    started = datetime.now(timezone.utc)
    rec = {"run_id": run_id or f"cli__{started:%Y%m%dT%H%M%SZ}", "dataset": dataset,
           "started_at": started, "status": "failed"}
    try:
        rec |= collect() | {"status": "success"}
        return rec
    except Exception as e:
        rec["error"] = repr(e)[:1000]
        raise
    finally:
        rec["finished_at"] = datetime.now(timezone.utc)
        path = (data_dir / "pipeline_runs" / f"dataset={dataset}" / f"ingest_date={started:%Y-%m-%d}"
                / f"{started:%Y%m%dT%H%M%S%fZ}.parquet")
        write_parquet(pl.DataFrame([rec], schema=RUN_SCHEMA), path)


def _base(raw: Path, staged: Path, df: pl.DataFrame) -> tuple[dict, dict]:
    payload = json.loads(raw.read_text())
    rec = {"raw_path": str(raw), "staged_path": str(staged), "cycle_s": payload["duration_s"], "rows": df.height}
    return rec, payload


def collect_option_snapshots(config_path: str, run_id: str | None = None) -> dict:
    def collect():
        raw = ibkr_options.run(config_path)
        staged, df = stage_options.stage_file(raw)
        rec, payload = _base(raw, staged, df)
        counts = Counter(df["quote_status"].to_list())
        return rec | {s: counts[s] for s in QUOTE_STATUSES} | {
            "ibkr_error_codes": sorted({e["code"] for e in payload["errors"]} - IBKR_INFO_CODES)}
    return _recorded("option_snapshots", run_id, collect)


def collect_equity_bars(config_path: str, run_id: str | None = None) -> dict:
    def collect():
        raw = alpaca_bars.run(config_path)
        staged, df = alpaca_bars.stage_file(raw)
        return _base(raw, staged, df)[0]
    return _recorded("equity_bars", run_id, collect)
