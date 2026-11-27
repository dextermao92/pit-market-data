from pathlib import Path

import pendulum
from airflow.sdk import dag, task

from pit import market_calendar
from pit.market_hours import recent_sessions
from pit.health import report
from pit.notify import CALLBACKS, send
from pit.pipeline import (collect_assets, collect_corporate_actions, collect_equity_bars, collect_sec,
                          collect_symbol_directory)
from pit.universe import collect_recent_daily_bars

CONFIG = "/opt/airflow/config/alpaca_bars.toml"
REFETCH_SESSIONS = 10  # vendor revisions to recent bars show up as new versions
NY = "America/New_York"


def _today():
    return pendulum.now(NY).date()


@dag(
    schedule="30 17 * * 1-5",  # weekdays; on a market holiday only the calendar and a notice run (trading_day)
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=CALLBACKS,  # failure (after retries) and recovery notifications
)
def eod():
    @task
    def calendar_table() -> str:
        # Which days the market is open, its hours, half days and holidays, as data (curated/market_calendar).
        return str(market_calendar.run(Path("/opt/airflow/data")))

    @task.short_circuit  # skips everything downstream (the all_done tasks too) on a market holiday
    def trading_day() -> bool:
        return market_calendar.is_session(_today())

    @task
    def holiday_notice() -> str | None:
        today = _today()
        if market_calendar.is_session(today):
            return None
        ahead = market_calendar.upcoming(today, 14)
        send(f"🗓️ 今日美股休市 · {market_calendar.label(today)}",
             "收盘后的数据任务今天不运行；盘中采集按交易日历本来就没有排。",
             [("📅 下一个交易日", market_calendar.label(market_calendar.next_session(today)), False),
              ("🔜 接下来两周的假期和半日市", "\n".join(ahead) or "无", False)], "ok")
        return today.isoformat()

    @task(retries=2)
    def corporate_actions(**context) -> str:
        # Whole market: the last 30 days and the year ahead daily, the whole history on the week's last session
        # (late revisions).
        full = market_calendar.last_session_of_week(_today())
        return collect_corporate_actions(CONFIG, run_id=context["run_id"], full=full)["staged_path"]

    @task(retries=2)
    def assets(**context) -> str:
        return collect_assets(CONFIG, run_id=context["run_id"])["staged_path"]

    @task(retries=2)
    def symbol_directory(**context) -> str:
        # Nasdaq Trader's listings with names and ETF flags (security type); current only, so daily builds history.
        return collect_symbol_directory(run_id=context["run_id"])["staged_path"]

    @task(retries=2)
    def sec_tickers(**context) -> str | None:
        # Today's ticker -> CIK from SEC, weekly (the week's last session): current only, so the weekly snapshots
        # are its history.
        if not market_calendar.last_session_of_week(_today()):
            return None
        return collect_sec("sec_tickers", run_id=context["run_id"])["staged_path"]

    @task(retries=2, trigger_rule="all_done")  # a failed asset snapshot falls back to the previous one
    def daily_bars(**context) -> list[str]:
        # Whole market (active non-OTC assets + configured symbols), from the asset snapshot just taken.
        sessions = recent_sessions(pendulum.now("UTC"), REFETCH_SESSIONS)
        return [r["staged_path"] for r in collect_recent_daily_bars(CONFIG, sessions, run_id=context["run_id"])]

    @task(retries=2)
    def refetch_recent_bars(**context) -> list[str]:
        return [collect_equity_bars(CONFIG, run_id=f"{context['run_id']}__{s}", session=s)["staged_path"]
                for s in recent_sessions(pendulum.now("UTC"), REFETCH_SESSIONS)]

    # Runs even if a fetch failed: curated refreshes from whatever staged has; failing tests fail this task.
    @task.bash(trigger_rule="all_done")
    def dbt_build() -> str:
        return "cd /opt/airflow && /home/airflow/dbt/bin/dbt build --project-dir dbt --profiles-dir dbt"

    @task(trigger_rule="all_done")
    def close_summary() -> str:
        level, title, body, fields = report("summary")
        send(title, body, fields, level)  # sent whatever happened upstream; failures show up in it
        return level

    gate = trading_day()  # not downstream of calendar_table: a failed table write must not stop the fetches
    holiday_notice()
    snapshot = assets()
    bars = daily_bars()
    snapshot >> bars
    fetches = [corporate_actions(), symbol_directory(), sec_tickers(), snapshot, refetch_recent_bars()]
    gate >> fetches
    [calendar_table(), *fetches, bars] >> dbt_build() >> close_summary()


eod()
