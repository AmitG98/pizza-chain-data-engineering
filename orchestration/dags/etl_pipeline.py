from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'amit',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='test_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline placeholder',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    test_task = BashOperator(
        task_id='print_hello',
        bash_command='echo "Airflow works!"'
    )

    test_task
