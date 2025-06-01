from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='silver_to_gold',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
    catchup=False
) as dag:

    run_spark = BashOperator(
        task_id='run_silver_to_gold',
        bash_command='docker exec spark spark-submit /opt/spark-apps/silver_to_gold.py'
    )
