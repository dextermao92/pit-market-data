import pendulum
from airflow.sdk import dag, task

from pit.airflow_timetable import MarketHoursTimetable
from pit.pipeline import collect_equity_bars


@dag(
    # First 5-min bar (open..open+5) is fetchable at open+20 (15-min free-plan lag); keep going
    # until close+30 so the session's last bars are picked up: 09:50 .. 16:25 ET.
    schedule=MarketHoursTimetable(every_minutes=5, start_offset_minutes=20, end_offset_minutes=30),
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
)
def equity_bars():
    @task(retries=2)
    def fetch(**context) -> str:
        return collect_equity_bars("/opt/airflow/config/alpaca_bars.toml", run_id=context["run_id"])["staged_path"]

    fetch()


equity_bars()
