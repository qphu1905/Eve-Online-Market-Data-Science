import os

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.sdk import DAG
from datetime import datetime, date, timedelta, timezone
from docker.types import Mount
from typing_extensions import Literal
from os import getenv


def extract_failure_cleanup():
    filename = f'opt/airflow/data/marketHistory_{date.today()}.csv'
    if os.path.isfile(filename):
        os.remove(filename)


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
                                  mounts=[Mount(source=f"{getenv("AIRFLOW__DIRECTORY")}/data", target="/data", type="bind")],
                                  mount_tmp_dir=False,
                                  force_pull=True,
                                  auto_remove="success")
    transform_task = DockerOperator(task_id='transform',
                                    image='quocphu1905/eve_online_market_data_science:transform_latest',
                                    mounts=[Mount(source=f"{getenv("AIRFLOW__DIRECTORY")}/data", target="/data", type="bind")],
                                    mount_tmp_dir=False,
                                    force_pull=True,
                                    auto_remove="success")
    load_task = DockerOperator(task_id='load',
                               image='quocphu1905/eve_online_market_data_science:load_latest',
                               mounts=[Mount(source=f"{getenv("AIRFLOW__DIRECTORY")}/data", target="/data", type="bind")],
                               mount_tmp_dir=False,
                               force_pull=True,
                               auto_remove="success")
    extract_failure_cleanup_task = PythonOperator(task_id='extract_failure_cleanup',
                                                  python_callable=extract_failure_cleanup,
                                                  trigger_rule='one_failed')
    extract_task >> transform_task >> load_task
    extract_task >> extract_failure_cleanup_task