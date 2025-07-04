from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('gold_delivery_summary_daily',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['gold', 'deliveries']) as dag:

    run_gold_delivery_summary_daily = DockerOperator(
        task_id='run_gold_delivery_summary_daily',
        image='spark_silver_to_gold-spark-gold-delivery-summary-daily',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/delivery_summary_daily.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
        mounts=[],
    )
