from __future__ import annotations

from airflow.decorators import dag, task
from datetime import timedelta
import pendulum

@dag(
    start_date=pendulum.datetime(2025, 12, 31, tz="UTC"),
    schedule=timedelta(days=7),
    catchup=False,
    tags=["venv"],
)
def twelvedata_venv():
    @task.virtualenv(
        task_id="05_scheduling_with_venvs",
        requirements=[
            "twelvedata",
            "pandas",
            "cloudpickle",
            "pendulum",
            "lazy_object_proxy",
        ],

        serializer="cloudpickle",
        system_site_packages=False,
    )
    def fetch_twelvedata(data_interval_start_iso: str, symbol: str, interval: str):
        import os
        from datetime import datetime
        from twelvedata import TDClient
        api_key = os.getenv("TWELVEDATA_API_KEY")
        td = TDClient(apikey=api_key)
        ts = td.time_series(symbol=symbol, interval=interval, outputsize=3)
        df = ts.as_pandas()
        print(df.head())

    fetch_twelvedata(
        data_interval_start_iso="{{ data_interval_start.isoformat() }}",
        symbol="MSFT",
        interval="1day",
    )

twelvedata_venv()
