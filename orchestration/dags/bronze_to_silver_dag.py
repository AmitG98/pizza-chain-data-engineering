from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='bronze_to_silver',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    run_spark = BashOperator(
        task_id='run_bronze_to_silver',
        bash_command='docker exec spark spark-submit /opt/spark-apps/bronze_to_silver.py'
    )
