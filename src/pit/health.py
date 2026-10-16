"""Status reports (Chinese), each sent every session day whatever the result (a missing one means the host is down):

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
from pit.notify import DATASET_ZH

NY = "America/New_York"
FARM_DOWN = {1100, 2103, 2105, 2110, 2157}  # connectivity lost / data farm broken


def _mark(ok: bool, warn: bool = False) -> str:
    return "✅" if ok else ("⚠️" if warn else "❌")


def _num(x) -> str:
    return "—" if x is None else f"{x:,.2f}".rstrip("0").rstrip(".")


def check_gateway(ibkr_cfg: dict) -> tuple[bool, list[str]]:
    lines = []
    try:
        ib, errors = connect(ibkr_cfg, client_offset=3)
    except Exception as e:
        return False, [f"❌ Gateway：无法连接（{e!r}）"]
    try:
        contracts = ib.qualifyContracts(Stock("SPY", "SMART", "USD"), Index("SPX", "CBOE", "USD"), Index("VIX", "CBOE", "USD"))
        tickers = [ib.reqMktData(c) for c in contracts]
        # SPY quotes pre-market only if the US farm is up; indices may only have yesterday's close before the open.
        wait_for_ticks(ib, ibkr_cfg, tickers, lambda t: clean(t.bid) or clean(t.last) or clean(t.close))
        for t in tickers:
            lines.append(f"　• {t.contract.symbol}：最新 {_num(clean(t.last))}｜买价 {_num(clean(t.bid))}｜昨收 {_num(clean(t.close))}")
        spy = tickers[0]
        us_farm = clean(spy.bid) is not None or clean(spy.last) is not None
        down = sorted({e["code"] for e in errors} & FARM_DOWN)
    finally:
        ib.disconnect()
    ok = us_farm and not down
    head = "✅ Gateway：已登录，美股行情正常" if ok else (
        f"❌ Gateway：异常（美股报价：{'有' if us_farm else '无'}；服务器断开错误码：{down or '无'}）")
    return ok, [head, *lines]


def check_alpaca() -> tuple[bool, list[str]]:
    try:
        with client() as c:
            r = c.get("/v2/stocks/bars", params={"symbols": "SPY", "timeframe": "1Day", "limit": 1, "feed": "sip"})
        return r.status_code == 200, ["✅ Alpaca：接口正常" if r.status_code == 200 else f"❌ Alpaca：HTTP {r.status_code}"]
    except Exception as e:
        return False, [f"❌ Alpaca：无法连接（{e!r}）"]


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
        return False, [f"❌ {session}：没有找到任何运行记录"]
    ok, lines = True, [f"📅 交易日 {session}"]
    for ds in ("option_snapshots", "index_snapshots", "equity_bars", "corporate_actions", "assets", "daily_bars"):
        r = runs.filter(pl.col("dataset") == ds)
        good = r.filter(pl.col("status") == "success")
        bad = r.height - good.height
        line_ok, warn = bad == 0 and good.height > 0, False
        parts = [f"成功 {good.height} 次，失败 {bad} 次"]
        if good.height:
            parts.append(f"{good['rows'].sum():,} 行")
            if good["quarantined"].sum():
                parts.append(f"隔离 {good['quarantined'].sum():,} 行")
        if ds == "equity_bars" and good.height:
            last = good.sort("started_at")["missing_bars"][-1]
            parts.append(f"收盘时缺 {last} 根 bar")
            warn = bool(last)
            line_ok &= not last
        if ds == "option_snapshots" and good.height:
            slots = good.filter(_mins(pl.col("et")).is_between(570, 959))["et"].dt.truncate("5m").n_unique()
            parts.append(f"正规时段覆盖 {slots}/78")
            parts.append(f"耗时中位 {good['run_s'].median():.0f} 秒 / 最长 {good['run_s'].max():.0f} 秒")
            line_ok &= slots == 78
            warn |= bad == 0
        ok &= line_ok
        lines.append(f"{_mark(line_ok, warn)} {DATASET_ZH[ds]}：{'，'.join(parts)}")
    added = sum(f.stat().st_size for layer in ("raw", "staged") for f in data_dir.glob(f"{layer}/**/ingest_date={session}/**/*") if f.is_file())
    lines.append(f"💾 当日写入（raw + staged）：{added / 1e6:,.0f} MB")
    results = Path(os.environ.get("DBT_TARGET", "dbt/target")) / "run_results.json"
    if results.exists():
        rr = json.loads(results.read_text())
        failed = [x["unique_id"].split(".")[-1] for x in rr["results"] if x["status"] in ("fail", "error")]
        at = datetime.fromisoformat(rr["metadata"]["generated_at"].replace("Z", "+00:00")).astimezone(ZoneInfo(NY))
        lines.append(f"{_mark(not failed)} dbt build（美东 {at:%m-%d %H:%M}）：{len(rr['results'])} 个节点，"
                     + ("全部通过" if not failed else f"失败：{', '.join(failed)}"))
        ok &= not failed
    alerts = sorted(p.read_text() for p in (data_dir / "cache" / "alerts" / str(session)).glob("*"))
    lines.append("🔔 当日告警：" + ("无" if not alerts else "\n" + "\n".join(f"　• {a}" for a in alerts)))
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
            lines.append(f"❌ {DATASET_ZH[ds]}：9:05 以来没有成功的运行（失败 {r.height} 次）")
            continue
        last = good.sort("started_at").row(-1, named=True)
        line_ok = last["run_s"] < 270  # warm and inside the 5-min budget
        ok &= line_ok
        lines.append(f"{_mark(line_ok)} {DATASET_ZH[ds]}：成功 {good.height} 次，失败 {r.height - good.height} 次；"
                     f"最近一次 {last['et']:%H:%M}，耗时 {last['run_s']:.0f} 秒，{last['rows']:,} 行")
    cache = list((data_dir / "cache" / "ibkr_contracts" / str(now.astimezone(ZoneInfo(NY)).date())).glob("*.json"))
    lines.append(f"🗂️ 合约缓存：{len(cache)} 个到期日")
    return ok, ["🚀 预热完成，可以开盘" if ok else "🛑 预热未完成，开盘可能受影响", *lines]


def disk(data_dir: Path) -> tuple[bool, list[str]]:
    free = shutil.disk_usage(data_dir).free
    return free > 20e9, [f"{_mark(free > 20e9, True)} 磁盘：剩余 {free / 1e9:,.0f} GB"]


def report(kind: str) -> tuple[bool, str, str]:
    """(ok, title, text) for kind in gateway | warmup | summary."""
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    now = datetime.now(timezone.utc)
    if kind == "gateway":
        ibkr_cfg = tomllib.loads(Path(os.environ.get("IBKR_CONFIG", "config/ibkr.toml")).read_text())["ibkr"]
        checks, title = [check_gateway(ibkr_cfg), check_alpaca()], "Gateway 检查（9:05 重启后）"
    elif kind == "warmup":
        checks, title = [warmup(data_dir, now)], "开盘预热检查"
    else:
        checks, title = [day_summary(data_dir, recent_sessions(now, 1)[0]), disk(data_dir)], "收盘总结"
    ok = all(c[0] for c in checks)
    return ok, f"{'✅' if ok else '⚠️'} {title}{'' if ok else '（有问题）'}", "\n".join(line for _, ls in checks for line in ls)


if __name__ == "__main__":
    ok, title, text = report(sys.argv[1])
    print(title, text, sep="\n")
    if "--send" in sys.argv:
        from pit.notify import send
        send(title, text)
