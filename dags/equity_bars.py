import exchange_calendars as xcals
import pandas as pd
import pendulum
from airflow.sdk import dag, task

from pit.pipeline import collect_equity_bars


# ponytail: cron + window check; replace with a custom market-hours Timetable in P3.
@dag(
    schedule="*/5 9-16 * * 1-5",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
)
def equity_bars():
    @task.short_circuit
    def bars_available() -> bool:
        # First 5-min bar (open..open+5) is fetchable at open+20 (15-min free-plan lag);
        # keep running until close+30 so the session's last bars are picked up.
        cal, now = xcals.get_calendar("XNYS"), pd.Timestamp.now(tz="UTC")
        today = now.tz_convert("America/New_York").date()
        if not cal.is_session(today):
            return False
        m = pd.Timedelta(minutes=1)
        return cal.session_open(today) + 20 * m <= now <= cal.session_close(today) + 30 * m

    @task(retries=2)
    def fetch(**context) -> str:
        return collect_equity_bars("/opt/airflow/config/alpaca_bars.toml", run_id=context["run_id"])["staged_path"]

    bars_available() >> fetch()


equity_bars()
