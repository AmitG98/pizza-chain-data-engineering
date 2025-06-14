from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('generate_silver_dim_store',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['silver', 'dim']) as dag:

    run_generate_dim_store = DockerOperator(
        task_id='run_generate_dim_store',
        image='spark_bronze_to_silver-spark-silver-dim-store',
        auto_remove=True,
        command="""
        spark-submit /opt/bitnami/spark/app/silver_dim_store.py
        """,
        network_mode='data-net',
        docker_url='unix://var/run/docker.sock',
        mounts=[],
    )
