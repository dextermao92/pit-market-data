FROM apache/airflow:3.3.2-python3.12
# Pin apache-airflow so pip never upgrades it while resolving our deps.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "ib_async==2.1.0" "polars==1.44.2" "exchange_calendars==4.13.2"
