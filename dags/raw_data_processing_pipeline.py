import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from raw_data_processing.extract_from_raw_data import extract_from_raw_data

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    "raw_data_processing_pipeline",
    default_args={"depends_on_past": False},
    start_date=pendulum.today("UTC").add(days=-1),
    description="A simple DAG to process raw data",
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    extract_from_raw_data_task = PythonOperator(
        task_id="extract_from_raw_data",
        python_callable=extract_from_raw_data,
    )

    extract_from_raw_data_task
