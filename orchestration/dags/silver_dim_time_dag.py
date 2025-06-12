from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG('generate_silver_dim_time',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False,
         tags=['silver', 'dim']) as dag:

    run_generate_dim_time = DockerOperator(
        task_id='run_generate_dim_time',
        image='spark_bronze_to_silver-spark-silver-dim-time',
        auto_remove=True,
        command="""
        spark-submit /opt/bitnami/spark/app/generate_silver_dim_time.py
        """,
        network_mode='data-net',
        docker_url='unix://var/run/docker.sock',
        mounts=[],
    )
