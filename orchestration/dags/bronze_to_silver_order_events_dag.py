from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('bronze_to_silver_order_events',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['silver', 'order_events']) as dag:

    run_bronze_to_silver_order_events = DockerOperator(
        task_id='run_bronze_to_silver_order_events',
        image='spark_bronze_to_silver-spark-silver-order-events',
        auto_remove=True,
        command="""
        spark-submit /opt/bitnami/spark/app/bronze_to_silver_order_events.py
        """,
        network_mode='data-net',
        docker_url='unix://var/run/docker.sock',
        mounts=[],
    )
