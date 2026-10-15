import pendulum
from airflow.sdk import dag, task

from pit.airflow_timetable import MarketHoursTimetable
from pit.pipeline import collect_index_snapshots, collect_option_snapshots

CONFIG = "/opt/airflow/config/ibkr.toml"


@dag(
    schedule=MarketHoursTimetable(every_minutes=5),  # 09:30 .. 15:55 ET on NYSE sessions
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,  # a missed snapshot can't be fetched later
    max_active_runs=1,
)
def option_snapshots():
    @task(retries=1)
    def chains(**context) -> str:
        return collect_option_snapshots(CONFIG, run_id=context["run_id"])["staged_path"]

    @task(retries=1)
    def indices(**context) -> str:
        return collect_index_snapshots(CONFIG, run_id=context["run_id"])["staged_path"]

    [chains(), indices()]


option_snapshots()
