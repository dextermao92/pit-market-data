from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from pit.health import watchdog
from pit.ibkr_options import warm_cache
from pit.market_hours import calendar
from pit.notify import CALLBACKS

START = pendulum.datetime(2026, 10, 1, tz="America/New_York")


def session_today() -> bool:
    return calendar().is_session(pendulum.now("America/New_York").date().isoformat())


@dag(schedule="15 8 * * 1-5", start_date=START, catchup=False, default_args=CALLBACKS)
def contract_cache():
    """Fill today's option contract cache before the 09:10 snapshots (cold: 1-3 min per expiry)."""
    @task.short_circuit
    def is_session() -> bool:
        return session_today()

    @task(retries=1, retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=90))  # refresh may be slow; carry-forward is instant
    def warm() -> dict:
        return warm_cache("/opt/airflow/config/ibkr.toml")

    is_session() >> warm()


@dag(schedule="*/5 9-16 * * 1-5", start_date=START, catchup=False, max_active_runs=1, default_args=CALLBACKS)
def watchdog_check():
    """Independent of the collectors, so a stuck run (which blocks its DAG) is still noticed."""
    @task(execution_timeout=timedelta(minutes=2))
    def check() -> list[str]:
        return watchdog()

    check()


contract_cache()
watchdog_check()
