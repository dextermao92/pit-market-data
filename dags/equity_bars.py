import exchange_calendars as xcals
import pandas as pd
import pendulum
from airflow.sdk import dag, task

from pit.pipeline import collect_equity_bars


# ponytail: cron + window check; replace with a custom market-hours Timetable in P3.
@dag(
    schedule="*/15 9-16 * * 1-5",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
)
def equity_bars():
    @task.short_circuit
    def bars_available() -> bool:
        # First completed bar (open..open+15) is fetchable at open+30 (15-min free-plan lag);
        # keep running until close+30 so the session's last bars are picked up.
        cal, now = xcals.get_calendar("XNYS"), pd.Timestamp.now(tz="UTC")
        today = now.tz_convert("America/New_York").date()
        if not cal.is_session(today):
            return False
        half_hour = pd.Timedelta(minutes=30)
        return cal.session_open(today) + half_hour <= now <= cal.session_close(today) + half_hour

    @task(retries=2)
    def fetch(**context) -> str:
        return collect_equity_bars("/opt/airflow/config/alpaca_bars.toml", run_id=context["run_id"])["staged_path"]

    bars_available() >> fetch()


equity_bars()
