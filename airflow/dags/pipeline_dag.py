from src import extract, load, transform

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="Eve_Online_market_data_ETL_pipeline",
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(year=2025, month=5, day=30, hour=12, minute=0, second=0, tzinfo=timezone.utc ),
    schedule=timedelta(days=1),
    catchup=True,
) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract.main)
    transform_task = PythonOperator(task_id='transform', python_callable=transform.main)
    load_task = PythonOperator(task_id='load', python_callable=load.main)

    extract_task >> transform_task >> load_task