from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from datetime import datetime, timedelta, timezone
from docker.types import Mount
from os import getenv

with DAG(
    dag_id="Eve_Online_market_data_ETL_pipeline",
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    },
    start_date=datetime(year=2025, month=5, day=30, hour=13, minute=0, second=0, tzinfo=timezone.utc),
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    extract_task = DockerOperator(task_id='extract',
                                  image='quocphu1905/eve_online_market_data_science:extract_latest',
                                  mounts=[Mount(source=f"{getenv("AIRFLOW__DIRECTORY")}/data", target="/data", type="bind")])
    transform_task = DockerOperator(task_id='transform',
                                    image='quocphu1905/eve_online_market_data_science:transform_latest',
                                    mounts=[Mount(source=f"{getenv("AIRFLOW__DIRECTORY")}/data", target="/data", type="bind")])
    load_task = DockerOperator(task_id='load',
                                    image='quocphu1905/eve_online_market_data_science:load_latest',
                                    mounts=[Mount(source=f"{getenv("AIRFLOW__DIRECTORY")}/data", target="/data", type="bind")])

    extract_task >> transform_task >> load_task