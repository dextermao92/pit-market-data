FROM apache/airflow:3.3.2-python3.12
# Pin apache-airflow so pip never upgrades it while resolving our deps.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "ib_async==2.1.0" "polars==1.44.2" "exchange_calendars==4.13.2"
# dbt in its own venv: its pins would otherwise fight Airflow's constraints.
RUN python -m venv /home/airflow/dbt && /home/airflow/dbt/bin/pip install --no-cache-dir "dbt-core==1.11.15" "dbt-duckdb==1.11.0"
