from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('generate_silver_dim_time',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
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
