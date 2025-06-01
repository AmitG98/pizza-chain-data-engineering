from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'amit',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='full_etl_pipeline',
    default_args=default_args,
    description='Kafka → Bronze → Silver → Gold',
    schedule_interval='@once',
    catchup=False
) as dag:

    kafka_to_bronze = BashOperator(
        task_id='kafka_to_bronze',
        bash_command='docker exec spark spark-submit /opt/spark-apps/spark_kafka_streaming.py'
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='docker exec spark spark-submit /opt/spark-apps/bronze_to_silver.py'
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='docker exec spark spark-submit /opt/spark-apps/silver_to_gold.py'
    )

    kafka_to_bronze >> bronze_to_silver >> silver_to_gold
