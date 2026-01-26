import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_data() -> dict:
    print("Fetching data from API")

    # New York temperature in 2025
    url = "https://archive-api.open-meteo.com/v1/archive?latitude=40.7143&longitude=-74.006&start_date=2025-01-01&end_date=2025-12-31&hourly=temperature_2m&timezone=auto"

    resp = requests.get(url)
    resp.raise_for_status()

    data = resp.json()
    data = {
        "time": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
    }
    return data


def transform(data: dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df["temperature"] = df["temperature"].clip(lower=-20, upper=50)
    return df


def save_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    s3 = S3Hook(aws_conn_id="aws_default")
    csv_temp = df.to_csv(index=False)
    s3.load_string(string_data=csv_temp, key=key, bucket_name=bucket, replace=True)
    print(f"Data saved to s3://{bucket}/{key}")


with DAG(dag_id="weather_data_classes_api_s3"):
    get_data_op = PythonOperator(task_id="get_data", python_callable=get_data)
    transform_op = PythonOperator(
        task_id="transform",
        python_callable=transform,
        op_kwargs={"data": get_data_op.output},
    )
    load_op = PythonOperator(
        task_id="load",
        python_callable=save_to_s3,
        op_kwargs={
            "df": transform_op.output,
            "bucket": "weather-data",
            "key": "weather_data_{{ data_interval_start | ds }}.csv",
        },
    )
    get_data_op >> transform_op >> load_op
