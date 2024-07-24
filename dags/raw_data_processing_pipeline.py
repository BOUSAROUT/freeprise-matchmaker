import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from raw_data_processing.extract_from_raw_data import extract_from_raw_data
from raw_data_processing.load_to_bucket import load_to_bucket
from api_calls.themuse_api_call import themuse_api_call
from api_calls.clean_themuseapi_data import clean_themuseapi_data

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

# Set start_date to a future date or the current date to prevent immediate execution

with DAG(
    "raw_data_processing_pipeline",
    default_args={"depends_on_past": False},
    start_date=pendulum.today("UTC"),  # Use the current date to prevent backfilling
    description="A simple DAG to process raw data",
    schedule_interval="1 * * * *",  # Schedule interval set to run every hour
    catchup=False,  # Prevent Airflow from running past dates
) as dag:

    extract_from_raw_data_task = PythonOperator(
        task_id="extract_from_raw_data",
        python_callable=extract_from_raw_data,
    )

    themuse_api_call_task = PythonOperator(
        task_id="themuse_api_call",
        python_callable=themuse_api_call,
    )

    clean_themuseapi_data_task = PythonOperator(
        task_id="clean_themuseapi_data",
        python_callable=clean_themuseapi_data,
    )

    load_to_bucket_task = PythonOperator(
        task_id="load_to_bucket",
        python_callable=load_to_bucket,
    )

    extract_from_raw_data_task >> themuse_api_call_task >> clean_themuseapi_data_task >> load_to_bucket_task
