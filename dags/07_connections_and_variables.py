from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SYMBOL = "EUR/USD"
PG_CONN_ID = "db_fx"


def fetch_rate(api_key: str, symbol: str) -> float:
    import requests

    url = "https://api.twelvedata.com/exchange_rate"
    r = requests.get(
        url,
        params={"symbol": symbol, "apikey": api_key},
        timeout=30,
    )
    r.raise_for_status()

    data = r.json()
    return float(data["rate"])


with DAG(
    dag_id="connections_and_variables",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["exercise", "variables", "connections"],
) as dag:

    fetch_exchange_rate = PythonVirtualenvOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_rate,
        requirements=["requests"],
        system_site_packages=False,
        op_kwargs={
            "api_key": "{{ var.value.TWELVEDATA_API_KEY }}",
            "symbol": SYMBOL,
        },
    )

    @task
    def to_postgres(rate: float, symbol: str):
        hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        hook.run(
            """
            INSERT INTO exchange_rates (symbol, rate)
            VALUES (%s, %s);
            """,
            parameters=(symbol, rate),
        )

    to_postgres(fetch_exchange_rate.output, SYMBOL)
