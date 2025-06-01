from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='kafka_to_bronze',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    run_spark = BashOperator(
        task_id='run_spark_kafka',
        bash_command='docker exec spark spark-submit /opt/spark-apps/spark_kafka_streaming.py'
    )
