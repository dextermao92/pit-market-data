"""One collection run per dataset: fetch -> raw -> staged (+ quarantine), recorded in pipeline_runs.

Re-stage everything from raw (e.g. after a rule change): python -m pit.pipeline restage data
Backfill equity bars: python -m pit.pipeline backfill config/alpaca_bars.toml 2025-10-15 2026-10-14
"""

import json
import os
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from pit import alpaca_bars, assets, corporate_actions, ibkr_indices, ibkr_options, stage_options
from pit.market_hours import calendar
from pit.notify import DATASET_ZH, alert_once
from pit.rules import missing_bars
from pit.storage import TS, write_parquet

QUOTE_STATUSES = ("two_sided", "ask_only", "bid_only", "no_quote", "crossed")
IBKR_INFO_CODES = {2104, 2106, 2107, 2108, 2119, 2158}  # data-farm status messages, not errors
NY = ZoneInfo("America/New_York")
IBKR_ALERT_CODES = {  # worth a notification (once per day per code)
    354: "行情未订阅", 10089: "API 行情需要额外订阅", 10090: "部分行情未订阅",
    10197: "与真实账户登录冲突，行情被占用", 1100: "Gateway 与 IBKR 断开连接",
    2103: "行情服务器连接断开", 2105: "历史数据服务器连接断开", 2157: "合约定义服务器连接断开",
}
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
    "index_snapshots": ibkr_indices.stage_file,
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
    # One file at a time: files written before a schema change lack newer columns.
    runs = pl.concat([pl.read_parquet(f, columns=["finished_at", "status"]) for f in files]).filter(
        pl.col("status") == "success")
    return runs["finished_at"].max() if runs.height else None  # when the data actually landed


def _recorded(dataset: str, run_id: str | None, collect) -> dict:
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    started = datetime.now(timezone.utc)
    rec = {"run_id": run_id or f"cli__{started:%Y%m%dT%H%M%SZ}", "dataset": dataset, "started_at": started,
           "status": "failed"}
    try:  # everything inside, so any failure is still recorded
        prev = _previous_success(data_dir, dataset, started)  # stale alerts: the watchdog DAG, not here
        rec["gap_s"] = (started - prev).total_seconds() if prev else None
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


def _ibkr_codes(dataset: str, payload: dict) -> list[int]:
    codes = sorted({e["code"] for e in payload["errors"]} - IBKR_INFO_CODES)
    for code in set(codes) & IBKR_ALERT_CODES.keys():
        msgs = sorted({e["msg"][:200] for e in payload["errors"] if e["code"] == code})
        at = datetime.fromisoformat(payload["ingested_at"]).astimezone(NY)
        alert_once(f"ibkr_{code}", f"⚠️ IBKR 错误 {code}：{IBKR_ALERT_CODES[code]}",
                   f"📍 首次出现：{DATASET_ZH[dataset]}，美东 {at:%H:%M}\n📝 原始信息：\n" + "\n".join(f"• {m}" for m in msgs[:5]),
                   level="error" if code == 1100 else "warn")
    return codes


def collect_option_snapshots(config_path: str, run_id: str | None = None) -> dict:
    def collect():
        rec, payload, staged, quarantined = _stage("option_snapshots", ibkr_options.run(config_path))
        counts = Counter(staged["quote_status"].to_list() + quarantined["quote_status"].to_list())
        return rec | {s: counts[s] for s in QUOTE_STATUSES} | {"ibkr_error_codes": _ibkr_codes("option_snapshots", payload)}
    return _recorded("option_snapshots", run_id, collect)


def collect_index_snapshots(config_path: str, run_id: str | None = None) -> dict:
    def collect():
        rec, payload, _, _ = _stage("index_snapshots", ibkr_indices.run(config_path))
        return rec | {"ibkr_error_codes": _ibkr_codes("index_snapshots", payload)}
    return _recorded("index_snapshots", run_id, collect)


def collect_equity_bars(config_path: str, run_id: str | None = None, session=None) -> dict:
    def collect():
        rec, payload, staged, quarantined = _stage("equity_bars", alpaca_bars.run(config_path, session=session))
        req = payload["request"]
        present = pl.concat([staged.select("symbol", "bar_start"), quarantined.select("symbol", "bar_start")])
        symbols = req["symbols"].split(",")
        missing = missing_bars(present, symbols, datetime.fromisoformat(req["start"]),
                               datetime.fromisoformat(req["end"]), timedelta(minutes=int(req["timeframe"].removesuffix("Min"))))
        if missing >= len(symbols):  # a whole slot's worth: a vendor gap, not one quiet symbol
            alert_once(f"missing_bars_{payload['session']}", f"⚠️ Alpaca 数据缺失：{payload['session']} 缺 {missing} 根 bar",
                       f"📉 {len(symbols)} 个代码在美东 {datetime.fromisoformat(req['start']).astimezone(NY):%H:%M}–"
                       f"{datetime.fromisoformat(req['end']).astimezone(NY):%H:%M} 之间共缺 {missing} 根 5 分钟 bar\n"
                       "🔍 请对照同一时段的 IBKR 快照核实；如果 Alpaca 之后补上，eod 回拉会自动拿到新版本", level="warn")
        return rec | {"missing_bars": missing}
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


def backfill(config_path: str, start: date, end: date, run_id: str | None = None) -> None:
    """Daily bars for the range, then every session's 5-min bars. ingested_at is the real fetch time: point in
    time starts at first ingest, so as_of queries before today don't see backfilled history."""
    run_id = run_id or f"backfill__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    sessions = [s.date() for s in calendar().sessions_in_range(start, end)]
    collect_daily_bars(config_path, sessions, run_id=run_id)
    for s in sessions:
        rec = collect_equity_bars(config_path, run_id=f"{run_id}__{s}", session=s)
        print(s, rec["rows"], "missing", rec["missing_bars"])
        time.sleep(0.6)  # ponytail: crude pacing under Alpaca's 200 calls/min (2 calls per session)


if __name__ == "__main__":
    if sys.argv[1] == "restage":
        restage(Path(sys.argv[2]))
    elif sys.argv[1] == "backfill":
        backfill(sys.argv[2], date.fromisoformat(sys.argv[3]), date.fromisoformat(sys.argv[4]))
