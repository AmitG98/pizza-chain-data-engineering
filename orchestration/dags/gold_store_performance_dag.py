from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG('gold_store_performance',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['gold', 'store']) as dag:

    run_gold_store_performance = DockerOperator(
        task_id='run_gold_store_performance',
        image='spark_silver_to_gold-spark-gold-store-performance',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/store_performance.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
        mounts=[],
    )
