import pendulum
from airflow.sdk import dag, task

from pit.market_hours import recent_sessions
from pit.health import report
from pit.notify import CALLBACKS, send
from pit.pipeline import collect_assets, collect_corporate_actions, collect_daily_bars, collect_equity_bars

CONFIG = "/opt/airflow/config/alpaca_bars.toml"
REFETCH_SESSIONS = 10  # vendor revisions to recent bars show up as new versions


@dag(
    schedule="30 17 * * 1-5",  # a holiday run just refetches the previous sessions again
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=CALLBACKS,  # failure (after retries) and recovery notifications
)
def eod():
    @task(retries=2)
    def corporate_actions(**context) -> str:
        return collect_corporate_actions(CONFIG, run_id=context["run_id"])["staged_path"]

    @task(retries=2)
    def assets(**context) -> str:
        return collect_assets(CONFIG, run_id=context["run_id"])["staged_path"]

    @task(retries=2)
    def daily_bars(**context) -> str:
        sessions = recent_sessions(pendulum.now("UTC"), REFETCH_SESSIONS)
        return collect_daily_bars(CONFIG, sessions, run_id=context["run_id"])["staged_path"]

    @task(retries=2)
    def refetch_recent_bars(**context) -> list[str]:
        return [collect_equity_bars(CONFIG, run_id=f"{context['run_id']}__{s}", session=s)["staged_path"]
                for s in recent_sessions(pendulum.now("UTC"), REFETCH_SESSIONS)]

    # Runs even if a fetch failed: curated refreshes from whatever staged has; failing tests fail this task.
    @task.bash(trigger_rule="all_done")
    def dbt_build() -> str:
        return "cd /opt/airflow && /home/airflow/dbt/bin/dbt build --project-dir dbt --profiles-dir dbt"

    @task(trigger_rule="all_done")
    def close_summary() -> bool:
        ok, title, text = report("summary")
        send(title, text)  # sent whatever happened upstream; failures show up in it
        return ok

    [corporate_actions(), assets(), daily_bars(), refetch_recent_bars()] >> dbt_build() >> close_summary()


eod()
