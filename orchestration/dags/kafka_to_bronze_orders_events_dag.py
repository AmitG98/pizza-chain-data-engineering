from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
}

with DAG('kafka_to_bronze_order_events',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['bronze', 'kafka']) as dag:

    run_kafka_to_bronze_order_events = DockerOperator(
        task_id='run_kafka_to_bronze_order_events',
        image='spark_kafka_to_bronze-spark-order-events-bronze',
        auto_remove=True,
        command="""
        spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1 \
        /opt/bitnami/spark/app/spark_kafka_to_bronze_order_events.py
        """,
        network_mode='data-net',
        docker_url='unix://var/run/docker.sock',
        mounts=[],
    )
