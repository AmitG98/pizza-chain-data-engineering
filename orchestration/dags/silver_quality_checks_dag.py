from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('silver_quality_checks',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         is_paused_upon_creation=True,
         tags=['quality', 'silver']) as dag:

    check_orders = DockerOperator(
        task_id='check_silver_orders',
        image='data_quality-spark-quality-check',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/orders_quality_check.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    check_complaints = DockerOperator(
        task_id='check_silver_complaints',
        image='data_quality-spark-quality-complaints',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/complaints_quality_check.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    check_weather = DockerOperator(
        task_id='check_silver_weather',
        image='data_quality-spark-quality-weather',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/weather_quality_check.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    check_dim_time = DockerOperator(
        task_id='check_silver_dim_time',
        image='data_quality-spark-quality-dim-time',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/dim_time_quality_check.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    check_order_status = DockerOperator(
        task_id='check_silver_order_status',
        image='data_quality-spark-quality-order-status',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/dim_order_status_quality_check.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    check_deliveries = DockerOperator(
        task_id='check_silver_deliveries',
        image='data_quality-spark-quality-deliveries',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/deliveries_quality_check.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    # Execution order: everything after orders
    check_orders >> [check_complaints, check_weather, check_dim_time, check_dim_store] >> check_deliveries
