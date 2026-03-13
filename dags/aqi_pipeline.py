from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from aqi_etl import run_aqi_etl

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aqi_data_pipeline",
    default_args=default_args,
    description="AQI ETL Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    run_aqi_etl = PythonOperator(
        task_id="run_aqi_etl",
        python_callable=run_aqi_etl,
    )

    run_aqi_etl