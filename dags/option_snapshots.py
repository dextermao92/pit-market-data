import pendulum
from airflow.sdk import dag, task

from pit.notify import CALLBACKS
from pit.airflow_timetable import MarketHoursTimetable
from pit.pipeline import collect_index_snapshots, collect_option_snapshots

CONFIG = "/opt/airflow/config/ibkr.toml"


@dag(
    # 09:10 .. 15:55 ET. The pre-open runs warm the Gateway (it restarts nightly, and a cold first pass over
    # ~2,200 contracts took ~14 min vs ~3 min warm), so the 09:30 snapshot is on time. They are real
    # observations too (mostly no_quote).
    schedule=MarketHoursTimetable(every_minutes=5, start_offset_minutes=-20),
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,  # a missed snapshot can't be fetched later
    max_active_runs=1,
    default_args=CALLBACKS,  # failure (after retries) and recovery notifications
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
