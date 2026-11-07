"""Status reports (Chinese cards), each sent every session day whatever the result (a missing one means the host
is down):

  gateway  08:00, 08:20, 08:40, 09:02 ET  login, contract definitions, subscriptions, US data farm, quotes; Alpaca reachable. Self-heals:
                     RECONNECTDATA, then RESTART (RESTART only when contract definitions are stalled)
  warmup   09:28 ET  pre-open runs since 09:05: are the option/index snapshots warm and inside the 5-min budget?
  progress hourly    10:00-15:00 ET: the session so far (runs, coverage, timings, alerts)
  summary  after eod  the session's runs, data collected, gaps, quality, dbt, disk, alerts sent

Each check returns (level, fields); level is ok | warn | error (warn: a problem that isn't ours, e.g. a vendor gap).
Usage: python -m pit.health gateway|warmup|progress|summary [--send]
"""

import glob
import json
import os
import shutil
import sys
import time
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
from ib_async import Index, Stock

from pit import ibc
from pit.alpaca_bars import client
from pit.ibkr_options import clean, connect, wait_for_ticks
from pit.market_hours import recent_sessions
from pit.notify import DATASET_ZH, LEVEL_MARK, Field, as_text

NY = "America/New_York"
FARM_DOWN = {1100, 2103, 2105, 2110, 2157}  # connectivity lost / data farm broken
NO_DATA = {354, 10089, 10090, 10167, 10168, 10197}  # not subscribed / delayed only / competing live session
RANK = {"ok": 0, "warn": 1, "error": 2}
Check = tuple[str, list[Field]]


def _worst(*levels: str) -> str:
    return max(levels, key=RANK.get, default="ok")


def _price(x):
    """IBKR quote value or None: NaN and -1 (its "no value") both mean nothing."""
    x = clean(x)
    return x if x is not None and x > 0 else None


def _num(x) -> str:
    return "—" if x is None else f"{x:,.2f}".rstrip("0").rstrip(".")


SECDEF_TIMEOUT = 20  # s; a healthy contract-definition server answers in ~1 s
SECDEF_DOWN = "❌ 合约查询"
NO_DATA_FIELD = "❌ 行情权限"


def check_gateway(ibkr_cfg: dict) -> Check:
    try:
        ib, errors = connect(ibkr_cfg, client_offset=3)
    except Exception as e:
        return "error", [("❌ Gateway", f"无法连接：{e!r}", False)]
    # A check, not a cold contract fetch: a hung request means a broken Gateway. Contract definitions get their own
    # line: on 2026-10-27 the secdef server answered nothing while login and farms looked fine; runs then live on the
    # cached contracts, and anything not cached hangs.
    ib.RequestTimeout = SECDEF_TIMEOUT
    try:
        t0 = time.monotonic()
        try:
            contracts = ib.qualifyContracts(Stock("SPY", "SMART", "USD"), Index("SPX", "CBOE", "USD"), Index("VIX", "CBOE", "USD"))
        except TimeoutError:
            return "error", [("✅ Gateway", "已登录", False),
                             (SECDEF_DOWN, f"{SECDEF_TIMEOUT}s 内没有回应：合约定义服务器卡住，采集只能靠当天的合约缓存", False)]
        secdef = ("✅ 合约查询", f"{time.monotonic() - t0:.1f}s 返回", False)
        tickers = [ib.reqMktData(c) for c in contracts]
        # SPY quotes pre-market only if the US farm is up; indices may only have yesterday's close before the open.
        wait_for_ticks(ib, ibkr_cfg, tickers, lambda t: _price(t.bid) or _price(t.last) or _price(t.close))
        quotes = [(f"📈 {t.contract.symbol}", f"最新 {_num(_price(t.last))}\n买价 {_num(_price(t.bid))}\n昨收 {_num(_price(t.close))}", True)
                  for t in tickers]  # side by side
        # Either a live SPY quote, or the Gateway saying the US farm is connected (works off-hours too).
        us_farm = (_price(tickers[0].bid) is not None or _price(tickers[0].last) is not None
                   or any(e["code"] == 2104 and "usfarm" in e["msg"] for e in errors))
        down = sorted({e["code"] for e in errors} & FARM_DOWN)
        no_data = sorted({e["code"] for e in errors} & NO_DATA)
    except Exception as e:  # connected but requests hang (2026-10-26 09:02: 300 s, and the exception skipped healing)
        return "error", [("❌ Gateway", f"已连接，但请求没有响应：{e!r}", False)]
    finally:
        ib.disconnect()
    if no_data:  # farms can look fine while this user gets no data, so the login alone proves nothing
        return "error", [("✅ Gateway", "已登录", False), secdef,
                         (NO_DATA_FIELD, f"错误码 {no_data}：这个 user 的订阅未生效，或行情被另一个登录占用。重启修不好，需要人工处理", False),
                         *quotes]
    if us_farm and not down:
        return "ok", [("✅ Gateway", "已登录，美股行情正常", False), secdef, *quotes]
    return "error", [("❌ Gateway", f"美股报价：{'有' if us_farm else '无'}；服务器断开错误码：{down or '无'}", False),
                     secdef, *quotes]


def heal_window(now: datetime) -> bool:
    """Self-healing only on session days between 04:00 and 20:00 ET (extended hours)."""
    from pit.market_hours import calendar
    et = now.astimezone(ZoneInfo(NY))
    return calendar().is_session(str(et.date())) and 4 <= et.hour < 20


HEAL_STEPS = (("RECONNECTDATA", "重连数据服务器", 20), ("RESTART", "重启 Gateway（不需要 2FA）", 120))


def heal_steps(fields: list[Field]) -> tuple:
    """RECONNECTDATA only reconnects market-data farms; a stalled secdef server goes straight to RESTART
    (on 2026-10-27 a RECONNECTDATA sent into that state hung IBC's command server, blocking RESTART too)."""
    return HEAL_STEPS[1:] if any(f[0] == SECDEF_DOWN for f in fields) else HEAL_STEPS


def gateway_with_healing(ibkr_cfg: dict, check=None, send_command=None, sleep=time.sleep) -> Check:
    """Check; if broken, try IBC's RECONNECTDATA, then RESTART, re-checking after each. Healed -> warn (yellow)."""
    check, send_command = check or check_gateway, send_command or ibc.command
    level, fields = check(ibkr_cfg)
    if level == "ok" or not heal_window(datetime.now(timezone.utc)) or any(f[0] == NO_DATA_FIELD for f in fields):
        return level, fields  # off-hours (weekends, holidays, night) or a subscription problem: report only, never restart
    log = ["发现 Gateway 异常"]
    for cmd, label, wait in heal_steps(fields):
        try:
            reply = send_command(cmd)
            log.append(f"→ {label}（IBC 回复：{reply or '无'}）")
        except ibc.Hung:  # later commands would only queue behind the stuck one
            log.append(f"→ {label}：IBC 收到命令但一直没有回应（命令服务器卡住，之后的命令也不会执行）")
            break
        except ibc.Denied:
            log.append(f"→ {label}失败：IBC 拒绝了命令（检查 ControlFrom）")
            continue
        except OSError as e:
            log.append(f"→ {label}失败：无法连接 IBC 命令服务器（{e!r}）")
            continue
        sleep(wait)
        level, fields = check(ibkr_cfg)
        if level == "ok":
            log.append("→ ✅ 已恢复")
            return "warn", [("🔧 自动修复", "\n".join(log), False), *fields]
    log.append("→ ❌ 自动修复失败，需要人工处理：docker compose restart ib-gateway，再在手机上确认 2FA")
    return "error", [("🔧 自动修复", "\n".join(log), False), *fields]


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
        head = f"成功 {good.height} · 失败 {bad}"  # line 1: counts; line 2: what's specific to the dataset
        if good.height:
            head += f" · {good['rows'].sum():,} 行" + (f" · 隔离 {good['quarantined'].sum():,}" if good["quarantined"].sum() else "")
        detail = ""
        if ds == "option_snapshots" and good.height:
            slots = good.filter(_mins(pl.col("et")).is_between(570, 959))["et"].dt.truncate("5m").n_unique()
            detail = f"正规时段覆盖 {slots}/78 · 耗时中位 {good['run_s'].median():.0f}s / 最长 {good['run_s'].max():.0f}s"
            lv = _worst(lv, "ok" if slots == 78 else "warn")
        if ds == "equity_bars" and good.height:
            last = good.sort("started_at")["missing_bars"][-1]
            detail = f"收盘时缺 {last} 根 bar"
            lv = _worst(lv, "warn" if last else "ok")
        level = _worst(level, lv)
        fields.append((f"{LEVEL_MARK[lv]} {DATASET_ZH[ds]}", "\n".join(x for x in (head, detail) if x), False))
    added = sum(f.stat().st_size for layer in ("raw", "staged") for f in data_dir.glob(f"{layer}/**/ingest_date={session}/**/*") if f.is_file())
    fields.append(("💾 当日写入", f"{added / 1e6:,.0f} MB（raw + staged）", False))
    results = Path(os.environ.get("DBT_RESULTS_DIR", "dbt/target")) / "run_results.json"
    if results.exists():
        rr = json.loads(results.read_text())
        failed = [x["unique_id"].split(".")[-1] for x in rr["results"] if x["status"] in ("fail", "error")]
        at = datetime.fromisoformat(rr["metadata"]["generated_at"].replace("Z", "+00:00")).astimezone(ZoneInfo(NY))
        if at.date() < session:  # dbt didn't run (or crashed before writing results) for this session
            failed = failed or [f"本交易日没有运行结果（最近一次是 {at:%m-%d}）"]
        lv = "error" if failed else "ok"
        level = _worst(level, lv)
        fields.append((f"{LEVEL_MARK[lv]} dbt build", (f"{len(rr['results'])} 个节点全部通过" if not failed
                                                       else "失败：" + "、".join(failed)) + f" · 美东 {at:%m-%d %H:%M}", False))
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
    cache = list((data_dir / "cache" / "ibkr_contracts" / str(now.astimezone(ZoneInfo(NY)).date())).glob("*_" + "[0-9]" * 8 + ".json"))
    fields.append(("🗂️ 合约缓存", f"{len(cache)} 个到期日", True))
    return level, fields


def progress(data_dir: Path, now: datetime) -> Check:
    """Hourly in market hours: the session so far for the intraday datasets."""
    from pit.market_hours import calendar
    et = now.astimezone(ZoneInfo(NY))
    runs = _runs(data_dir, et.date())
    if runs.is_empty():
        return "error", [("❌ 运行记录", "今天还没有任何运行记录", False)]
    cal = calendar()
    open_min, close_min = (_minutes_et(cal.session_open(str(et.date()))), _minutes_et(cal.session_close(str(et.date()))))
    level, fields = "ok", []
    for ds in ("option_snapshots", "index_snapshots", "equity_bars"):
        r = runs.filter(pl.col("dataset") == ds).sort("started_at")
        good = r.filter(pl.col("status") == "success")
        bad = r.height - good.height
        # red only while it's still failing; an earlier failure that recovered is yellow
        lv = "error" if good.is_empty() or r["status"][-1] != "success" else ("warn" if bad else "ok")
        lines = [f"成功 {good.height} · 失败 {bad}" + (f" · {good['rows'].sum():,} 行" if good.height else "")]
        if good.height:
            last = good.row(-1, named=True)
            lines.append(f"最近一次 {last['et']:%H:%M} · 耗时 {last['run_s']:.0f}s · {last['rows']:,} 行")
        if ds == "option_snapshots" and good.height:
            hi = min(et.hour * 60 + et.minute - 10, close_min - 5)  # slots that should have finished by now
            expected = max(0, (hi - open_min) // 5 + 1)
            slots = good.filter(_mins(pl.col("et")).is_between(open_min, hi))["et"].dt.truncate("5m").n_unique()
            lines.append(f"正规时段覆盖 {slots}/{expected} · 耗时中位 {good['run_s'].median():.0f}s / 最长 {good['run_s'].max():.0f}s")
            lv = _worst(lv, "ok" if slots >= expected else "warn")
        if ds == "equity_bars" and good.height and last["missing_bars"]:
            lines.append(f"最近一次缺 {last['missing_bars']} 根 bar")
            lv = _worst(lv, "warn")
        level = _worst(level, lv)
        fields.append((f"{LEVEL_MARK[lv]} {DATASET_ZH[ds]}", "\n".join(lines), False))
    alerts = sorted(p.read_text() for p in (data_dir / "cache" / "alerts" / str(et.date())).glob("*"))
    fields.append(("🔔 今日告警", "\n".join(f"• {a}" for a in alerts) or "无", False))
    return level, fields


def _minutes_et(ts) -> int:
    t = ts.tz_convert(NY)
    return t.hour * 60 + t.minute


STALE_AFTER = timedelta(minutes=15)
WATCH = {  # dataset: (data expected from, until) as offsets from the session open / close
    "option_snapshots": (timedelta(0), timedelta(0)),
    "index_snapshots": (timedelta(0), timedelta(0)),
    "equity_bars": (timedelta(minutes=20), timedelta(minutes=30)),
}


def stale_datasets(data_dir: Path, now: datetime) -> list[tuple[str, int, datetime]]:
    """(dataset, minutes without new data, since) for intraday datasets whose last successful run *finished*
    more than STALE_AFTER ago inside their window. Runs on its own, so a stuck collector can't hide it."""
    from pit.market_hours import calendar
    cal, day = calendar(), now.astimezone(ZoneInfo(NY)).date()
    if not cal.is_session(str(day)):
        return []
    open_, close = cal.session_open(str(day)).to_pydatetime(), cal.session_close(str(day)).to_pydatetime()
    runs = _runs(data_dir, day)
    out = []
    for ds, (start_off, end_off) in WATCH.items():
        start, end = open_ + start_off, close + end_off
        if not (start + STALE_AFTER <= now <= end + STALE_AFTER):
            continue
        done = runs.filter((pl.col("dataset") == ds) & (pl.col("status") == "success")) if not runs.is_empty() else runs
        since = max([start, *([done["finished_at"].max()] if not done.is_empty() else [])])
        if now - since > STALE_AFTER:
            out.append((ds, int((now - since).total_seconds() // 60), since))
    return out


def watchdog(now: datetime | None = None, heal=None) -> list[str]:
    """Alert once per stale episode; for IBKR datasets also self-heal the Gateway once per episode. 2026-10-26:
    its API stopped accepting connections from 09:10 for the whole session; only the 09:02 check could heal."""
    from pit.notify import alert_once, send
    now = now or datetime.now(timezone.utc)
    sent = []
    for ds, minutes, since in stale_datasets(Path(os.environ.get("DATA_DIR", "data")), now):
        if alert_once(f"watchdog_{ds}_{since:%H%M}", f"⏸️ 数据停滞：{DATASET_ZH[ds]}已 {minutes} 分钟没有新数据",
                      f"🕘 上次成功完成：美东 {since.astimezone(ZoneInfo(NY)):%H:%M}\n"
                      "可能原因：Gateway 或数据源断开、采集任务卡住。恢复后会自动继续，这条只提醒一次。", level="error"):
            sent.append(ds)
    if {"option_snapshots", "index_snapshots"} & set(sent):
        heal = heal or (lambda: gateway_with_healing(
            tomllib.loads(Path(os.environ.get("IBKR_CONFIG", "config/ibkr.toml")).read_text())["ibkr"]))
        level, fields = heal()
        send(f"{LEVEL_MARK[level]} 数据停滞后的 Gateway 检查", HEADLINE["gateway"][level], fields, level)
    return sent


def disk(data_dir: Path) -> Check:
    free = shutil.disk_usage(data_dir).free
    lv = "ok" if free > 20e9 else "warn"
    return lv, [(f"{LEVEL_MARK[lv]} 磁盘", f"剩余 {free / 1e9:,.0f} GB", False)]


HEADLINE = {  # description per report and level
    "gateway": {"ok": "Gateway 和数据源都正常，可以开始预热", "warn": "发现异常，已自动修复", "error": "Gateway 或数据源异常，自动修复失败，开盘采集可能失败"},
    "warmup": {"ok": "🚀 预热完成，可以开盘", "warn": "预热基本完成，但有问题", "error": "🛑 预热未完成，开盘采集可能受影响"},
    "progress": {"ok": "采集正常进行中", "warn": "采集在进行，但有缺口或出过错（已恢复）", "error": "有数据集正在失败，请查看标 ❌ 的项目"},
    "summary": {"ok": "今天一切正常", "warn": "我们这边正常，但数据源有缺口", "error": "今天有运行失败，请查看下面标 ❌ 的项目"},
}


def report(kind: str) -> tuple[str, str, str, list[Field]]:
    """(level, title, description, fields) for kind in gateway | warmup | progress | summary."""
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    now = datetime.now(timezone.utc)
    if kind == "gateway":
        ibkr_cfg = tomllib.loads(Path(os.environ.get("IBKR_CONFIG", "config/ibkr.toml")).read_text())["ibkr"]
        checks, title = [gateway_with_healing(ibkr_cfg), check_alpaca()], "Gateway 检查（开盘前）"
    elif kind == "warmup":
        checks, title = [warmup(data_dir, now)], "开盘预热检查"
    elif kind == "progress":
        checks, title = [progress(data_dir, now)], f"盘中进度 · 美东 {now.astimezone(ZoneInfo(NY)):%H:%M}"
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
