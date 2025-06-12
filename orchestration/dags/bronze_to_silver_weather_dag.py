from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG('bronze_to_silver_weather',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['silver', 'weather']) as dag:

    run_bronze_to_silver_weather = DockerOperator(
        task_id='run_bronze_to_silver_weather',
        image='spark_bronze_to_silver-spark-silver-weather',
        auto_remove=True,
        command="""
        spark-submit /opt/bitnami/spark/app/bronze_to_silver_weather.py
        """,
        network_mode='data-net',
        docker_url='unix://var/run/docker.sock',
        mounts=[],
    )
