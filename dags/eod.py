import pendulum
from airflow.sdk import dag, task

from pit.pipeline import collect_corporate_actions


# P3 adds daily bars, the 10-day revision refetch and dbt build here.
@dag(
    schedule="30 17 * * 1-5",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
)
def eod():
    @task(retries=2)
    def corporate_actions(**context) -> str:
        return collect_corporate_actions("/opt/airflow/config/alpaca_bars.toml", run_id=context["run_id"])["staged_path"]

    corporate_actions()


eod()
