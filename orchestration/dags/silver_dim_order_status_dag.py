from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('silver_dim_order_status',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['silver', 'dim', 'order_status']) as dag:

    run_silver_dim_order_status = DockerOperator(
        task_id='run_silver_dim_order_status',
        image='spark_bronze_to_silver-spark-silver-dim-order-status',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/silver_dim_order_status.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
        mounts=[],
    )
