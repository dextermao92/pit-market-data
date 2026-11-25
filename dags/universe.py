from datetime import timedelta

import pendulum
import polars as pl
from airflow.sdk import dag, task

from pit.corporate_actions import HISTORY_START
from pit.notify import CALLBACKS, send
from pit.universe import backfill, discover, last_closed_session

CONFIG = "/opt/airflow/config/alpaca_bars.toml"
QUARTER_MONTHS = (1, 4, 7, 10)


@dag(
    # Saturday 20:00 ET: no US session for a day and a half, and it is Sunday daytime in Australia (10:00-12:00
    # Sydney through the DST changes), away from the Australian-night collection hours. Past history never
    # expires, so this has no deadline; it only needs to stay out of the way: discovery and backfill stop before
    # the next collection window (08:00 ET on a session day, pit.market_hours.collection_window).
    schedule="0 20 * * 6",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=CALLBACKS,
)
def universe_refresh():
    """Quarterly whole-market refresh (Stage 2 P1a): rediscover symbols, including delisted ones no list keeps,
    then backfill daily bars incrementally (new symbols in full, known ones from their last backfill). Runs on
    the first Saturday of January, April, July and October; a manual trigger always runs (the first, full
    backfill is one)."""
    @task.short_circuit
    def due(**context) -> bool:
        today = pendulum.now("America/New_York")
        return context["dag_run"].run_type == "manual" or (today.month in QUARTER_MONTHS and today.day <= 7)

    @task(execution_timeout=timedelta(minutes=90))
    def discover_symbols() -> int:
        return pl.read_parquet(discover(CONFIG)).height

    @task(execution_timeout=timedelta(hours=4))
    def backfill_daily_bars() -> dict:
        return backfill(CONFIG, pendulum.parse(HISTORY_START).date(), last_closed_session())

    @task
    def universe_summary(candidates: int, stats: dict) -> str:
        level = "warn" if stats["failed"] or stats["stopped"] else "ok"
        fields = [("🔎 有数据的代码", f"{candidates:,}", True), ("🆕 首次回补的代码", f"{stats['new_symbols']:,}", True),
                  ("📊 新拉日线", f"{stats['rows']:,} 行 · {stats['batches']} 批", True),
                  ("❗ 失败批次", f"{stats['failed']}（重跑会续传）" if stats["failed"] else "0", True)]
        if stats["stopped"]:
            fields.append(("⏸️ 到点暂停", "已在美股采集时段（开盘前 90 分钟起）之前停下；手动重跑会从断点续传", False))
        send("🗂️ 全市场代码刷新" + ("暂停" if stats["stopped"] else "完成"), "代码发现 + 日线增量回补（季度任务）",
             fields, level)
        return level

    candidates = discover_symbols()
    stats = backfill_daily_bars()
    due() >> candidates >> stats
    universe_summary(candidates, stats)


universe_refresh()
