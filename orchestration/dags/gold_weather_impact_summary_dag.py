from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('gold_weather_impact_summary',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['gold', 'weather']) as dag:

    run_gold_weather_impact_summary = DockerOperator(
        task_id='run_gold_weather_impact_summary',
        image='spark_silver_to_gold-spark-gold-weather-impact-summary',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/weather_impact_summary.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
        mounts=[],
    )
