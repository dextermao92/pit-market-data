"""Status reports (Chinese cards), each sent every session day whatever the result (a missing one means the host
is down):

  gateway  09:08 ET  after the 09:05 Gateway auto-restart: login, US data farm, quotes; Alpaca reachable
  warmup   09:28 ET  pre-open runs since 09:05: are the option/index snapshots warm and inside the 5-min budget?
  summary  after eod  the session's runs, data collected, gaps, quality, dbt, disk, alerts sent

Each check returns (level, fields); level is ok | warn | error (warn: a problem that isn't ours, e.g. a vendor gap).
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
from pit.notify import DATASET_ZH, LEVEL_MARK, Field, as_text

NY = "America/New_York"
FARM_DOWN = {1100, 2103, 2105, 2110, 2157}  # connectivity lost / data farm broken
RANK = {"ok": 0, "warn": 1, "error": 2}
Check = tuple[str, list[Field]]


def _worst(*levels: str) -> str:
    return max(levels, key=RANK.get, default="ok")


def _num(x) -> str:
    return "—" if x is None else f"{x:,.2f}".rstrip("0").rstrip(".")


def check_gateway(ibkr_cfg: dict) -> Check:
    try:
        ib, errors = connect(ibkr_cfg, client_offset=3)
    except Exception as e:
        return "error", [("❌ Gateway", f"无法连接：{e!r}", False)]
    try:
        contracts = ib.qualifyContracts(Stock("SPY", "SMART", "USD"), Index("SPX", "CBOE", "USD"), Index("VIX", "CBOE", "USD"))
        tickers = [ib.reqMktData(c) for c in contracts]
        # SPY quotes pre-market only if the US farm is up; indices may only have yesterday's close before the open.
        wait_for_ticks(ib, ibkr_cfg, tickers, lambda t: clean(t.bid) or clean(t.last) or clean(t.close))
        quotes = [(f"📈 {t.contract.symbol}", f"最新 {_num(clean(t.last))}\n买价 {_num(clean(t.bid))}\n昨收 {_num(clean(t.close))}", True)
                  for t in tickers]
        us_farm = clean(tickers[0].bid) is not None or clean(tickers[0].last) is not None
        down = sorted({e["code"] for e in errors} & FARM_DOWN)
    finally:
        ib.disconnect()
    if us_farm and not down:
        return "ok", [("✅ Gateway", "已登录，美股行情正常", False), *quotes]
    return "error", [("❌ Gateway", f"美股报价：{'有' if us_farm else '无'}；服务器断开错误码：{down or '无'}", False), *quotes]


def check_alpaca() -> Check:
    try:
        with client() as c:
            r = c.get("/v2/stocks/bars", params={"symbols": "SPY", "timeframe": "1Day", "limit": 1, "feed": "sip"})
        return ("ok", [("✅ Alpaca", "接口正常", False)]) if r.status_code == 200 else \
            ("error", [("❌ Alpaca", f"HTTP {r.status_code}", False)])
    except Exception as e:
        return "error", [("❌ Alpaca", f"无法连接：{e!r}", False)]


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


def day_summary(data_dir: Path, session) -> Check:
    runs = _runs(data_dir, session)
    if runs.is_empty():
        return "error", [("❌ 运行记录", f"{session} 没有找到任何运行记录", False)]
    level, fields = "ok", []
    for ds in ("option_snapshots", "index_snapshots", "equity_bars", "corporate_actions", "assets", "daily_bars"):
        r = runs.filter(pl.col("dataset") == ds)
        good = r.filter(pl.col("status") == "success")
        bad = r.height - good.height
        lv = "ok" if bad == 0 and good.height else "error"
        lines = [f"成功 {good.height} · 失败 {bad}"]
        if good.height:
            lines.append(f"{good['rows'].sum():,} 行" + (f" · 隔离 {good['quarantined'].sum():,}" if good["quarantined"].sum() else ""))
        if ds == "option_snapshots" and good.height:
            slots = good.filter(_mins(pl.col("et")).is_between(570, 959))["et"].dt.truncate("5m").n_unique()
            lines += [f"正规时段覆盖 {slots}/78", f"耗时中位 {good['run_s'].median():.0f}s · 最长 {good['run_s'].max():.0f}s"]
            lv = _worst(lv, "ok" if slots == 78 else "warn")
        if ds == "equity_bars" and good.height:
            last = good.sort("started_at")["missing_bars"][-1]
            lines.append(f"收盘时缺 {last} 根 bar")
            lv = _worst(lv, "warn" if last else "ok")
        level = _worst(level, lv)
        fields.append((f"{LEVEL_MARK[lv]} {DATASET_ZH[ds]}", "\n".join(lines), True))
    added = sum(f.stat().st_size for layer in ("raw", "staged") for f in data_dir.glob(f"{layer}/**/ingest_date={session}/**/*") if f.is_file())
    fields.append(("💾 当日写入", f"{added / 1e6:,.0f} MB（raw + staged）", True))
    results = Path(os.environ.get("DBT_TARGET", "dbt/target")) / "run_results.json"
    if results.exists():
        rr = json.loads(results.read_text())
        failed = [x["unique_id"].split(".")[-1] for x in rr["results"] if x["status"] in ("fail", "error")]
        at = datetime.fromisoformat(rr["metadata"]["generated_at"].replace("Z", "+00:00")).astimezone(ZoneInfo(NY))
        lv = "error" if failed else "ok"
        level = _worst(level, lv)
        fields.append((f"{LEVEL_MARK[lv]} dbt build", (f"{len(rr['results'])} 个节点全部通过" if not failed
                                                       else "失败：" + "、".join(failed)) + f"\n美东 {at:%m-%d %H:%M}", True))
    alerts = sorted(p.read_text() for p in (data_dir / "cache" / "alerts" / str(session)).glob("*"))
    fields.append(("🔔 当日告警", "\n".join(f"• {a}" for a in alerts) or "无", False))
    return level, fields


def warmup(data_dir: Path, now: datetime) -> Check:
    runs = _runs(data_dir, now.astimezone(ZoneInfo(NY)).date())
    since = runs.filter(_mins(pl.col("et")) >= 9 * 60 + 5) if not runs.is_empty() else runs
    level, fields = "ok", []
    for ds in ("option_snapshots", "index_snapshots"):
        r = since.filter(pl.col("dataset") == ds) if not since.is_empty() else since
        good = r.filter(pl.col("status") == "success") if not r.is_empty() else r
        if good.is_empty():
            level = "error"
            fields.append((f"❌ {DATASET_ZH[ds]}", f"9:05 以来没有成功的运行\n失败 {r.height} 次", True))
            continue
        last = good.sort("started_at").row(-1, named=True)
        lv = "ok" if last["run_s"] < 270 else "error"  # warm and inside the 5-min budget
        level = _worst(level, lv)
        fields.append((f"{LEVEL_MARK[lv]} {DATASET_ZH[ds]}", f"成功 {good.height} · 失败 {r.height - good.height}\n"
                       f"最近一次 {last['et']:%H:%M}\n耗时 {last['run_s']:.0f}s · {last['rows']:,} 行", True))
    cache = list((data_dir / "cache" / "ibkr_contracts" / str(now.astimezone(ZoneInfo(NY)).date())).glob("*.json"))
    fields.append(("🗂️ 合约缓存", f"{len(cache)} 个到期日", True))
    return level, fields


def disk(data_dir: Path) -> Check:
    free = shutil.disk_usage(data_dir).free
    lv = "ok" if free > 20e9 else "warn"
    return lv, [(f"{LEVEL_MARK[lv]} 磁盘", f"剩余 {free / 1e9:,.0f} GB", True)]


HEADLINE = {  # description per report and level
    "gateway": {"ok": "Gateway 和数据源都正常，可以开始预热", "warn": "有非致命问题，请留意", "error": "Gateway 或数据源异常，开盘采集可能失败"},
    "warmup": {"ok": "🚀 预热完成，可以开盘", "warn": "预热基本完成，但有问题", "error": "🛑 预热未完成，开盘采集可能受影响"},
    "summary": {"ok": "今天一切正常", "warn": "我们这边正常，但数据源有缺口", "error": "今天有运行失败，请查看下面标 ❌ 的项目"},
}


def report(kind: str) -> tuple[str, str, str, list[Field]]:
    """(level, title, description, fields) for kind in gateway | warmup | summary."""
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    now = datetime.now(timezone.utc)
    if kind == "gateway":
        ibkr_cfg = tomllib.loads(Path(os.environ.get("IBKR_CONFIG", "config/ibkr.toml")).read_text())["ibkr"]
        checks, title = [check_gateway(ibkr_cfg), check_alpaca()], "Gateway 检查（9:05 重启后）"
    elif kind == "warmup":
        checks, title = [warmup(data_dir, now)], "开盘预热检查"
    else:
        session = recent_sessions(now, 1)[0]
        checks, title = [day_summary(data_dir, session), disk(data_dir)], f"收盘总结 · {session}"
    level = _worst(*(lv for lv, _ in checks))
    return level, f"{LEVEL_MARK[level]} {title}", HEADLINE[kind][level], [f for _, fs in checks for f in fs]


if __name__ == "__main__":
    level, title, body, fields = report(sys.argv[1])
    print(title, as_text(body, fields), sep="\n")
    if "--send" in sys.argv:
        from pit.notify import send
        send(title, body, fields, level)
