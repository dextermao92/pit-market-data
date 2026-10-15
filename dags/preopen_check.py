import pendulum
from airflow.sdk import dag, task

from pit.health import report
from pit.market_hours import calendar
from pit.notify import send, task_failed


@dag(
    schedule="15 9 * * 1-5",  # after the 09:05 Gateway auto-restart, alongside the 09:10 pre-open warm-up
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    default_args={"on_failure_callback": task_failed},
)
def preopen_check():
    @task.short_circuit
    def session_today() -> bool:
        return calendar().is_session(pendulum.now("America/New_York").date().isoformat())

    @task
    def check_and_report() -> bool:
        ok, text = report("/opt/airflow/config/ibkr.toml", "/opt/airflow/config/alpaca_bars.toml")
        send(("OK" if ok else "PROBLEM") + " pre-open check", text)  # sent either way
        return ok

    session_today() >> check_and_report()


preopen_check()
