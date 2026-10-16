"""Status reports, each sent every session day whatever the result (so a missing one means the host is down):

  gateway  09:08 ET  after the 09:05 Gateway auto-restart: login, US data farm, quotes; Alpaca reachable
  warmup   09:28 ET  pre-open runs since 09:05: are the option/index snapshots warm and inside the 5-min budget?
  summary  after eod  the session's runs, data collected, gaps, quality, dbt, disk, alerts sent

Usage: python -m pit.health gateway|warmup|summary [--send]
"""

import glob
import json
import os
import shutil
import sys
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

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


def _runs(data_dir: Path, day) -> pl.DataFrame:
    """pipeline_runs started on New York day `day` (partitions are UTC dates: the day's and the next)."""
    files = [f for d in (day, day + timedelta(days=1))
             for f in glob.glob(str(data_dir / "pipeline_runs" / "dataset=*" / f"ingest_date={d}" / "*.parquet"))]
    if not files:
        return pl.DataFrame()
    runs = pl.concat([pl.read_parquet(f) for f in files], how="diagonal")
    return runs.with_columns(et=pl.col("started_at").dt.convert_time_zone(NY),
                             run_s=(pl.col("finished_at") - pl.col("started_at")).dt.total_seconds()).filter(
        (pl.col("et").dt.date() == day) & ~pl.col("run_id").str.starts_with("backfill"))


def _mins(et: pl.Expr) -> pl.Expr:
    return et.dt.hour().cast(pl.Int32) * 60 + et.dt.minute().cast(pl.Int32)


def day_summary(data_dir: Path, session) -> tuple[bool, list[str]]:
    runs = _runs(data_dir, session)
    if runs.is_empty():
        return False, [f"{session}: no pipeline_runs found"]
    ok, lines = True, [f"Session {session}"]
    for ds in ("option_snapshots", "index_snapshots", "equity_bars", "corporate_actions", "assets", "daily_bars"):
        r = runs.filter(pl.col("dataset") == ds)
        good = r.filter(pl.col("status") == "success")
        bad = r.height - good.height
        extra = f", rows {good['rows'].sum():,}" if good.height else ""
        if good.height and good["quarantined"].sum():
            extra += f", quarantined {good['quarantined'].sum():,}"
        if ds == "equity_bars" and good.height:
            last = good.sort("started_at")["missing_bars"][-1]
            extra += f", missing bars at close {last}"
            ok &= not last
        if ds == "option_snapshots" and good.height:
            slots = good.filter(_mins(pl.col("et")).is_between(570, 959))["et"].dt.truncate("5m").n_unique()
            extra += f", RTH slots {slots}/78, run time median {good['run_s'].median():.0f}s max {good['run_s'].max():.0f}s"
        ok &= bad == 0 and good.height > 0
        lines.append(f"  {ds:18} {good.height} ok, {bad} failed{extra}")
    added = sum(f.stat().st_size for layer in ("raw", "staged") for f in data_dir.glob(f"{layer}/**/ingest_date={session}/**/*") if f.is_file())
    lines.append(f"  data written for {session} (raw + staged): {added / 1e6:.0f} MB")
    results = Path(os.environ.get("DBT_TARGET", "dbt/target")) / "run_results.json"
    if results.exists():
        rr = json.loads(results.read_text())
        failed = [x["unique_id"].split(".")[-1] for x in rr["results"] if x["status"] in ("fail", "error")]
        lines.append(f"  dbt build {rr['metadata']['generated_at'][:16]}Z: {len(rr['results'])} nodes, failed: {failed or 'none'}")
        ok &= not failed
    alerts = sorted(p.name for p in (data_dir / "cache" / "alerts" / str(session)).glob("*"))
    lines.append(f"  alerts sent: {', '.join(alerts) or 'none'}")
    return ok, lines


def warmup(data_dir: Path, now: datetime) -> tuple[bool, list[str]]:
    runs = _runs(data_dir, now.astimezone(ZoneInfo(NY)).date())
    since = runs.filter(_mins(pl.col("et")) >= 9 * 60 + 5) if not runs.is_empty() else runs
    ok, lines = True, []
    for ds in ("option_snapshots", "index_snapshots"):
        r = since.filter(pl.col("dataset") == ds) if not since.is_empty() else since
        good = r.filter(pl.col("status") == "success") if not r.is_empty() else r
        if good.is_empty():
            ok = False
            lines.append(f"  {ds:18} NO successful run since 09:05 ({r.height} failed)")
            continue
        last = good.sort("started_at").row(-1, named=True)
        ok &= last["run_s"] < 270  # warm and inside the 5-min budget
        lines.append(f"  {ds:18} {good.height} ok, {r.height - good.height} failed; last at {last['et']:%H:%M} took "
                     f"{last['run_s']:.0f}s, {last['rows']} rows")
    cache = list((data_dir / "cache" / "ibkr_contracts" / str(now.astimezone(ZoneInfo(NY)).date())).glob("*.json"))
    lines.append(f"  contract cache: {len(cache)} expiries")
    return ok, ["Warm-up: " + ("READY for the open" if ok else "NOT READY"), *lines]


def disk(data_dir: Path) -> tuple[bool, list[str]]:
    free = shutil.disk_usage(data_dir).free
    return free > 20e9, [f"Disk: {free / 1e9:.0f} GB free"]


def report(kind: str) -> tuple[bool, str, str]:
    """(ok, title, text) for kind in gateway | warmup | summary."""
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    now = datetime.now(timezone.utc)
    if kind == "gateway":
        ibkr_cfg = tomllib.loads(Path(os.environ.get("IBKR_CONFIG", "config/ibkr.toml")).read_text())["ibkr"]
        checks, title = [check_gateway(ibkr_cfg), check_alpaca()], "Gateway check (after 09:05 restart)"
    elif kind == "warmup":
        checks, title = [warmup(data_dir, now)], "Warm-up check"
    else:
        checks, title = [day_summary(data_dir, recent_sessions(now, 1)[0]), disk(data_dir)], "Close summary"
    ok = all(c[0] for c in checks)
    return ok, f"{'OK' if ok else 'PROBLEM'} {title}", "\n".join(line for _, ls in checks for line in ls)


if __name__ == "__main__":
    ok, title, text = report(sys.argv[1])
    print(title, text, sep="\n")
    if "--send" in sys.argv:
        from pit.notify import send
        send(title, text)
