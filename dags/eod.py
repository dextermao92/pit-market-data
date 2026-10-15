import pendulum
from airflow.sdk import dag, task

from pit.market_hours import recent_sessions
from pit.pipeline import collect_corporate_actions, collect_daily_bars, collect_equity_bars

CONFIG = "/opt/airflow/config/alpaca_bars.toml"
REFETCH_SESSIONS = 10  # vendor revisions to recent bars show up as new versions


@dag(
    schedule="30 17 * * 1-5",  # a holiday run just refetches the previous sessions again
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
)
def eod():
    @task(retries=2)
    def corporate_actions(**context) -> str:
        return collect_corporate_actions(CONFIG, run_id=context["run_id"])["staged_path"]

    @task(retries=2)
    def daily_bars(**context) -> str:
        sessions = recent_sessions(pendulum.now("UTC"), REFETCH_SESSIONS)
        return collect_daily_bars(CONFIG, sessions, run_id=context["run_id"])["staged_path"]

    @task(retries=2)
    def refetch_recent_bars(**context) -> list[str]:
        return [collect_equity_bars(CONFIG, run_id=f"{context['run_id']}__{s}", session=s)["staged_path"]
                for s in recent_sessions(pendulum.now("UTC"), REFETCH_SESSIONS)]

    [corporate_actions(), daily_bars(), refetch_recent_bars()]


eod()
