import pendulum
from airflow.sdk import dag, task

from pit.health import report
from pit.market_hours import calendar
from pit.notify import CALLBACKS, send


def status_dag(dag_id: str, cron: str, kind: str):
    """A report sent on every session day whether OK or not: silence itself then means the host is down."""
    @dag(dag_id=dag_id, schedule=cron, start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
         catchup=False, default_args=CALLBACKS)
    def status():
        @task.short_circuit
        def session_today() -> bool:
            return calendar().is_session(pendulum.now("America/New_York").date().isoformat())

        @task
        def check_and_send() -> bool:
            ok, title, text = report(kind)
            send(title, text)
            return ok

        session_today() >> check_and_send()

    return status()


gateway_check = status_dag("gateway_check", "8 9 * * 1-5", "gateway")   # after the 09:05 Gateway auto-restart
warmup_check = status_dag("warmup_check", "28 9 * * 1-5", "warmup")     # pre-open runs started 09:10
