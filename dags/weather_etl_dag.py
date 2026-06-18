from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import requests
import pandas as pd
import duckdb


def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 40.71,
        "longitude": -74.01,
        "hourly": "temperature_2m,precipitation"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    hourly = data["hourly"]

    df = pd.DataFrame({
        "time": hourly["time"],
        "temperature": hourly["temperature_2m"],
        "precipitation": hourly["precipitation"]
    })

    output_path = "/opt/airflow/data/weather_staging.csv"

    df.to_csv(output_path, index=False)

    print(f"Saved {len(df)} rows to {output_path}")


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def load_to_duckdb():
    csv_path="/opt/airflow/data/weather_staging.csv"
    db_path="/opt/airflow/data/weather.db"

    con=duckdb.connect(db_path)

    con.execute("""
                CREATE OR REPLACE TABLE weather_hourly AS 
                SELECT *
                FROM read_csv_auto(?)
                """,[csv_path])
    

    row_count = con.execute(
        "SELECT COUNT(*) FROM weather_hourly"
    ).fetchone()[0]
    print(f"Loaded {row_count} rows into weather_hourly")
    con.close()

def validate_data():
    db_path="/opt/airflow/data/weather.db"
    con=duckdb.connect(db_path)
    count=con.execute("""
                          select count(*)
                          from weather_hourly
                          """).fetchone()[0]
    if count == 0:
            raise ValueError("No weather records found")
    print(f"Validation passed: {count} rows")
    con.close()

with DAG(
    dag_id="weather_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["weather", "etl"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,

        
    )
    load_task = PythonOperator(
        task_id='load_to_duckdb',
        python_callable=load_to_duckdb,
        )
    
    validate_task =PythonOperator(
        task_id="validate_task",
        python_callable=validate_data,
    )
    fetch_task >> load_task >> validate_task