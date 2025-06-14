from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('gold_peak_hours_analysis',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['gold', 'analysis']) as dag:

    run_gold_peak_hours_analysis = DockerOperator(
        task_id='run_gold_peak_hours_analysis',
        image='spark_silver_to_gold-spark-gold-peak-hours-analysis',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/peak_hours_analysis.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
        mounts=[],
    )
