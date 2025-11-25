from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extract.extract_citibike import extract_citibike_data
from src.extract.extract_gbfs import extract_gbfs_once
from src.load.load_to_postgres import load_to_postgres

with DAG(
    "citi_vault",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
) as dag:

    extract_trips = PythonOperator(
        task_id="extract_trips",
        python_callable=extract_citibike_data,
        op_kwargs={"year": 2024, "month": 7},
    )

    extract_stations = PythonOperator(
        task_id="extract_stations",
        python_callable=extract_gbfs_once,
    )

    load_db = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres,
    )

    extract_trips >> extract_stations >> load_db
