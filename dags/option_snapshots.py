import exchange_calendars as xcals
import pandas as pd
import pendulum
from airflow.sdk import dag, task

from pit.ibkr_options import run


# ponytail: cron + open-check; replace with a custom market-hours Timetable in P3.
@dag(
    schedule="*/15 9-16 * * 1-5",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,  # a missed snapshot can't be fetched later
    max_active_runs=1,
)
def option_snapshots():
    @task.short_circuit
    def market_open() -> bool:
        return xcals.get_calendar("XNYS").is_open_on_minute(pd.Timestamp.now(tz="UTC"))

    @task(retries=1)
    def snapshot() -> str:
        return str(run("/opt/airflow/config/ibkr_options.toml"))

    market_open() >> snapshot()


option_snapshots()
