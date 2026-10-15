"""Pre-open health check: Gateway and its US data farm, Alpaca, yesterday's runs, disk. Sent every session day.

Usage: python -m pit.health config/ibkr.toml config/alpaca_bars.toml   (prints the report; --send also notifies)
"""

import glob
import json
import os
import shutil
import sys
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import polars as pl
from ib_async import Index, Stock

from pit.alpaca_bars import client
from pit.ibkr_options import clean, connect, wait_for_ticks
from pit.market_hours import recent_sessions

NY = "America/New_York"
FARM_DOWN = {1100, 2103, 2105, 2110, 2157}  # connectivity lost / data farm broken


def check_gateway(ibkr_cfg: dict) -> tuple[bool, list[str]]:
    lines = []
    try:
        ib, errors = connect(ibkr_cfg, client_offset=3)
    except Exception as e:
        return False, [f"Gateway: cannot connect ({e!r})"]
    try:
        contracts = ib.qualifyContracts(Stock("SPY", "SMART", "USD"), Index("SPX", "CBOE", "USD"), Index("VIX", "CBOE", "USD"))
        tickers = [ib.reqMktData(c) for c in contracts]
        # SPY quotes pre-market only if the US farm is up; indices may only have yesterday's close before the open.
        wait_for_ticks(ib, ibkr_cfg, tickers, lambda t: clean(t.bid) or clean(t.last) or clean(t.close))
        for t in tickers:
            lines.append(f"  {t.contract.symbol:4} last={clean(t.last)} bid={clean(t.bid)} close={clean(t.close)}")
        spy = tickers[0]
        us_farm = clean(spy.bid) is not None or clean(spy.last) is not None
        down = sorted({e["code"] for e in errors} & FARM_DOWN)
    finally:
        ib.disconnect()
    ok = us_farm and not down
    head = "Gateway: OK" if ok else f"Gateway: PROBLEM (US quotes: {'yes' if us_farm else 'NO'}, farm errors: {down or 'none'})"
    return ok, [head, *lines]


def check_alpaca() -> tuple[bool, list[str]]:
    try:
        with client() as c:
            r = c.get("/v2/stocks/bars", params={"symbols": "SPY", "timeframe": "1Day", "limit": 1, "feed": "sip"})
        return r.status_code == 200, [f"Alpaca: {'OK' if r.status_code == 200 else f'HTTP {r.status_code}'}"]
    except Exception as e:
        return False, [f"Alpaca: unreachable ({e!r})"]


def yesterday(data_dir: Path, session) -> tuple[bool, list[str]]:
    # Partitions are UTC dates: the session's runs sit in its own date and, after 20:00 ET, the next one.
    days = (session, session + timedelta(days=1))
    files = [f for d in days for f in glob.glob(str(data_dir / "pipeline_runs" / "dataset=*" / f"ingest_date={d}" / "*.parquet"))]
    runs = pl.concat([pl.read_parquet(f) for f in files], how="diagonal") if files else pl.DataFrame()
    if runs.is_empty():
        return False, [f"{session}: no pipeline_runs found"]
    runs = runs.with_columns(day=pl.col("started_at").dt.convert_time_zone(NY).dt.date()).filter(
        (pl.col("day") == session) & ~pl.col("run_id").str.starts_with("backfill"))
    ok, lines = True, [f"{session}:"]
    for ds in ("option_snapshots", "index_snapshots", "equity_bars", "corporate_actions", "assets", "daily_bars"):
        r = runs.filter(pl.col("dataset") == ds)
        good, bad = r.filter(pl.col("status") == "success").height, r.filter(pl.col("status") != "success").height
        extra = ""
        if ds == "equity_bars" and good:
            last = r.filter(pl.col("status") == "success").sort("started_at")["missing_bars"][-1]
            extra = f", missing bars at close: {last}"
            ok &= not last
        if ds == "option_snapshots" and good:
            et = r.filter(pl.col("status") == "success")["started_at"].dt.convert_time_zone(NY)
            mins = et.dt.hour().cast(pl.Int32) * 60 + et.dt.minute().cast(pl.Int32)
            slots = et.filter(mins.is_between(570, 959)).dt.truncate("5m").n_unique()
            extra = f", RTH 5-min slots covered: {slots}/78"
        ok &= bad == 0 and good > 0
        lines.append(f"  {ds:18} {good} ok, {bad} failed{extra}")
    results = Path(os.environ.get("DBT_TARGET", "dbt/target")) / "run_results.json"
    if results.exists():
        rr = json.loads(results.read_text())
        statuses = [x["status"] for x in rr["results"]]
        failed = [x["unique_id"].split(".")[-1] for x in rr["results"] if x["status"] in ("fail", "error")]
        lines.append(f"  dbt build ({rr['metadata']['generated_at'][:16]}Z): {len(statuses)} nodes, failed: {failed or 'none'}")
        ok &= not failed
    return ok, lines


def report(ibkr_config: str, alpaca_config: str) -> tuple[bool, str]:
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    ibkr_cfg = tomllib.loads(Path(ibkr_config).read_text())["ibkr"]
    session = recent_sessions(datetime.now(timezone.utc), 1)[0]
    disk = shutil.disk_usage(data_dir)
    checks = [check_gateway(ibkr_cfg), check_alpaca(), yesterday(data_dir, session),
              (disk.free > 20e9, [f"Disk: {disk.free / 1e9:.0f} GB free, data dir {sum(f.stat().st_size for f in data_dir.rglob('*') if f.is_file()) / 1e9:.1f} GB"])]
    ok = all(c[0] for c in checks)
    return ok, "\n".join(line for _, lines in checks for line in lines)


if __name__ == "__main__":
    ok, text = report(sys.argv[1], sys.argv[2])
    print(text)
    if "--send" in sys.argv:
        from pit.notify import send
        send(("OK" if ok else "PROBLEM") + " pre-open check", text)
