from __future__ import annotations

import pendulum
import requests
import polars as pl

from airflow import DAG
from airflow.decorators import task
from airflow.sdk import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


BUCKET = "taxi-data"
AWS_CONN = "aws_default"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def year_month(ctx) -> str:
    return ctx["data_interval_start"].format("YYYY-MM")


with DAG(
    dag_id="taxi_dataset_building",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@monthly",
    catchup=True,
    tags=["s3", "parquet", "taxi dataset"],
) as dag:

    @task
    def download_raw() -> dict:
        ctx = get_current_context()
        ym = year_month(ctx)
        url = f"{BASE_URL}/yellow_tripdata_{ym}.parquet"
        r = requests.get(url, timeout=120)
        r.raise_for_status()

        key = f"raw/yellow/year={ym[:4]}/month={ym[5:]}/yellow_tripdata_{ym}.parquet"

        S3Hook(AWS_CONN).load_bytes(
            bytes_data=r.content,
            bucket_name=BUCKET,
            key=key,
            replace=True,
        )

        return {"ym": ym, "key": key}

    @task
    def daily_aggregate(meta: dict) -> str:
        ym = meta["ym"]
        key = meta["key"]

        s3 = S3Hook(AWS_CONN)
        local_in = f"/tmp/raw_{ym}.parquet"
        local_out = f"/tmp/daily_{ym}.parquet"

        s3.get_key(key, BUCKET).download_file(local_in)

        df = pl.read_parquet(local_in)
        out = (
            df.with_columns(
                pl.col("tpep_pickup_datetime")
                .cast(pl.Datetime)
                .dt.date()
                .alias("date")
            )
            .group_by("date")
            .agg(pl.len().alias("total_rides"))
            .sort("date")
        )

        out.write_parquet(local_out)

        out_key = f"processed/daily/year={ym[:4]}/month={ym[5:]}/daily_rides_{ym}.parquet"

        s3.load_file(
            filename=local_out,
            key=out_key,
            bucket_name=BUCKET,
            replace=True
        )
        return f"s3://{BUCKET}/{out_key}"

    daily_aggregate(download_raw())
