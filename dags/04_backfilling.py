import pandas as pd
import requests
import pendulum
from airflow.sdk import dag, task
from datetime import timedelta

@dag(
    schedule=timedelta(days=7),
    start_date=pendulum.datetime(2025, 12, 31, tz="UTC"),
    catchup=True,
    tags=["weather"],
)
def weather_api_catchup():
    @task()
    def get_data() -> dict:
        print("Fetching data from API")
        # New York temperature in 2025
        url = "https://api.open-meteo.com/v1/forecast?latitude=40.73&longitude=-73.94&daily=temperature_2m_max,temperature_2m_min&timezone=America%2FNew_York"

        resp = requests.get(url)
        resp.raise_for_status()

        data = resp.json()
        data = {
            "time": data["daily"]["time"],
            "temperature_max": data["daily"]["temperature_2m_max"],
            "temperature_min": data["daily"]["temperature_2m_min"],
        }
        return data

    @task()
    def transform(data: dict) -> pd.DataFrame:
        df = pd.DataFrame(data)
        df["temperature_max"] = df["temperature_max"].clip(lower=-20, upper=50)
        df["temperature_min"] = df["temperature_min"].clip(lower=-20, upper=50)
        return df
    
    @task()
    def save_data(df: pd.DataFrame) -> None:
        print("Saving the data")
        df.to_csv("ny_daily.csv", index=False)

    data = get_data()
    data = transform(data)
    save_data(data)

weather_api_catchup()