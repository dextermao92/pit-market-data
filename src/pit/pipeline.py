"""One collection run per dataset: fetch -> raw -> staged (+ quarantine), recorded in pipeline_runs.

Re-stage everything from raw (e.g. after a rule change): python -m pit.pipeline restage data
"""

import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from pit import alpaca_bars, assets, corporate_actions, ibkr_options, stage_options
from pit.rules import missing_bars
from pit.storage import TS, write_parquet

QUOTE_STATUSES = ("two_sided", "ask_only", "bid_only", "no_quote", "crossed")
IBKR_INFO_CODES = {2104, 2106, 2107, 2108, 2119, 2158}  # data-farm status messages, not errors
RUN_SCHEMA = {
    "run_id": pl.String, "dataset": pl.String, "started_at": TS, "finished_at": TS, "status": pl.String,
    "error": pl.String, "raw_path": pl.String, "staged_path": pl.String, "cycle_s": pl.Float64,
    "rows": pl.Int64, "quarantined": pl.Int64, "flagged": pl.Int64,
    "gap_s": pl.Float64,             # since the previous successful run of this dataset (freshness)
    "missing_bars": pl.Int64,        # equity bars only
    **{s: pl.Int64 for s in QUOTE_STATUSES}, "ibkr_error_codes": pl.List(pl.Int64),  # option snapshots only
}
STAGERS = {
    "option_snapshots": stage_options.stage_file,
    "equity_bars": alpaca_bars.stage_file,
    "daily_bars": alpaca_bars.stage_file,
    "corporate_actions": corporate_actions.stage_file,
    "assets": assets.stage_file,
}


def _previous_success(data_dir: Path, dataset: str, now: datetime) -> datetime | None:
    files = [f for d in (now - timedelta(days=1), now)
             for f in (data_dir / "pipeline_runs" / f"dataset={dataset}" / f"ingest_date={d:%Y-%m-%d}").glob("*.parquet")]
    if not files:
        return None
    runs = pl.read_parquet(files, columns=["started_at", "status"]).filter(pl.col("status") == "success")
    return runs["started_at"].max() if runs.height else None


def _recorded(dataset: str, run_id: str | None, collect) -> dict:
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    started = datetime.now(timezone.utc)
    prev = _previous_success(data_dir, dataset, started)
    rec = {"run_id": run_id or f"cli__{started:%Y%m%dT%H%M%SZ}", "dataset": dataset, "started_at": started,
           "status": "failed", "gap_s": (started - prev).total_seconds() if prev else None}
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


def _stage(dataset: str, raw: Path) -> tuple[dict, dict, pl.DataFrame, pl.DataFrame]:
    staged_path, staged, quarantined = STAGERS[dataset](raw)
    payload = json.loads(raw.read_text())
    rec = {"raw_path": str(raw), "staged_path": str(staged_path), "cycle_s": payload["duration_s"],
           "rows": staged.height, "quarantined": quarantined.height,
           "flagged": staged.filter(pl.col("flags").list.len() > 0).height}
    return rec, payload, staged, quarantined


def collect_option_snapshots(config_path: str, run_id: str | None = None) -> dict:
    def collect():
        rec, payload, staged, quarantined = _stage("option_snapshots", ibkr_options.run(config_path))
        counts = Counter(staged["quote_status"].to_list() + quarantined["quote_status"].to_list())
        return rec | {s: counts[s] for s in QUOTE_STATUSES} | {
            "ibkr_error_codes": sorted({e["code"] for e in payload["errors"]} - IBKR_INFO_CODES)}
    return _recorded("option_snapshots", run_id, collect)


def collect_equity_bars(config_path: str, run_id: str | None = None, session=None) -> dict:
    def collect():
        rec, payload, staged, quarantined = _stage("equity_bars", alpaca_bars.run(config_path, session=session))
        req = payload["request"]
        present = pl.concat([staged.select("symbol", "bar_start"), quarantined.select("symbol", "bar_start")])
        return rec | {"missing_bars": missing_bars(
            present, req["symbols"].split(","), datetime.fromisoformat(req["start"]),
            datetime.fromisoformat(req["end"]), timedelta(minutes=int(req["timeframe"].removesuffix("Min"))))}
    return _recorded("equity_bars", run_id, collect)


def collect_daily_bars(config_path: str, sessions: list, run_id: str | None = None) -> dict:
    return _recorded("daily_bars", run_id, lambda: _stage("daily_bars", alpaca_bars.run_daily(config_path, sessions))[0])


def collect_corporate_actions(config_path: str, run_id: str | None = None) -> dict:
    return _recorded("corporate_actions", run_id,
                     lambda: _stage("corporate_actions", corporate_actions.run(config_path))[0])


def collect_assets(config_path: str, run_id: str | None = None) -> dict:
    return _recorded("assets", run_id, lambda: _stage("assets", assets.run(config_path))[0])


def restage(data_dir: Path) -> None:
    for dataset, stage in STAGERS.items():
        for raw in sorted(data_dir.glob(f"raw/source=*/ingest_date=*/{dataset}/*.json")):
            stage(raw)
            print(raw)


if __name__ == "__main__" and sys.argv[1] == "restage":
    restage(Path(sys.argv[2]))
