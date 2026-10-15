import pendulum
from airflow.sdk import dag, task

from pit.airflow_timetable import MarketHoursTimetable
from pit.pipeline import collect_option_snapshots


@dag(
    schedule=MarketHoursTimetable(every_minutes=5),  # 09:30 .. 15:55 ET on NYSE sessions
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,  # a missed snapshot can't be fetched later
    max_active_runs=1,
)
def option_snapshots():
    @task(retries=1)
    def snapshot(**context) -> str:
        rec = collect_option_snapshots("/opt/airflow/config/ibkr_options.toml", run_id=context["run_id"])
        return rec["staged_path"]

    snapshot()


option_snapshots()
