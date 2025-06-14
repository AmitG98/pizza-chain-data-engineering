from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG('gold_complaints_by_type',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['gold', 'complaints']) as dag:

    run_gold_complaints_by_type = DockerOperator(
        task_id='run_gold_complaints_by_type',
        image='spark_silver_to_gold-spark-gold-complaints-by-type',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/complaints_by_type.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
        mounts=[],
    )
