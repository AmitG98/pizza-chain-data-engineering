from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'retries': 1,
}

with DAG('full_etl_pipeline',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['pipeline']) as dag:
    
    # silver_dim_time
    silver_dim_time = DockerOperator(
        task_id='silver_dim_time',
        image='spark_bronze_to_silver-spark-silver-dim-time',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/generate_silver_dim_time.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    # BRONZE
    orders_bronze = DockerOperator(
        task_id='orders_bronze',
        image='spark_kafka_to_bronze-spark-orders-bronze',
        auto_remove=True,
        command="""spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1 /opt/bitnami/spark/app/spark_kafka_to_bronze_orders.py""",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    order_events_bronze = DockerOperator(
        task_id='order_events_bronze',
        image='spark_kafka_to_bronze-spark-order-events-bronze',
        auto_remove=True,
        command="""spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1 /opt/bitnami/spark/app/spark_kafka_to_bronze_order_events.py""",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    complaints_bronze = DockerOperator(
        task_id='complaints_bronze',
        image='spark_kafka_to_bronze-spark-complaints-bronze',
        auto_remove=True,
        command="""spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1 /opt/bitnami/spark/app/spark_kafka_to_bronze_complaints.py""",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    weather_bronze = DockerOperator(
        task_id='weather_bronze',
        image='spark_kafka_to_bronze-spark-weather-bronze',
        auto_remove=True,
        command="""spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.1 /opt/bitnami/spark/app/spark_kafka_to_bronze_weather.py""",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    # SILVER
    silver_orders = DockerOperator(
        task_id='silver_orders_clean',
        image='spark_bronze_to_silver-spark-silver-orders',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/bronze_to_silver_orders.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    silver_order_events = DockerOperator(
        task_id='silver_order_events_clean',
        image='spark_bronze_to_silver-spark-silver-order-events',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/bronze_to_silver_order_events.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    silver_complaints = DockerOperator(
        task_id='silver_complaints_clean',
        image='spark_bronze_to_silver-spark-silver-complaints',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/bronze_to_silver_complaints.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    silver_weather = DockerOperator(
        task_id='silver_weather_enriched',
        image='spark_bronze_to_silver-spark-silver-weather',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/bronze_to_silver_weather.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    dim_order_status = DockerOperator(
        task_id='dim_order_status',
        image='spark_bronze_to_silver-spark-silver-dim-order-status',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/silver_dim_order_status.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    silver_deliveries = DockerOperator(
        task_id='silver_deliveries_enriched',
        image='spark_bronze_to_silver-spark-silver-deliveries',
        auto_remove=True,
        command="spark-submit /opt/bitnami/spark/app/bronze_to_silver_deliveries.py",
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net'
    )

    # GOLD
    gold_complaints = DockerOperator(
        task_id='gold_complaints_by_type',
        image='spark_silver_to_gold-spark-gold-complaints-by-type',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/complaints_by_type.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    gold_metrics = DockerOperator(
        task_id='gold_delivery_metrics_by_region',
        image='spark_silver_to_gold-spark-gold-delivery-metrics',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/delivery_metrics_by_region.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    gold_summary = DockerOperator(
        task_id='gold_delivery_summary_daily',
        image='spark_silver_to_gold-spark-gold-delivery-summary-daily',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/delivery_summary_daily.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    gold_business = DockerOperator(
        task_id='gold_daily_business_summary',
        image='spark_silver_to_gold-spark-gold-daily-business-summary',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/gold_daily_business_summary.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    gold_peak_hours = DockerOperator(
        task_id='gold_peak_hours_analysis',
        image='spark_silver_to_gold-spark-gold-peak-hours-analysis',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/peak_hours_analysis.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    gold_store_perf = DockerOperator(
        task_id='gold_store_performance',
        image='spark_silver_to_gold-spark-gold-store-performance',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/store_performance.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    gold_weather = DockerOperator(
        task_id='gold_weather_impact_summary',
        image='spark_silver_to_gold-spark-gold-weather-impact-summary',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/app/weather_impact_summary.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-net',
    )

    # Dependencies
    [orders_bronze, order_events_bronze, complaints_bronze, weather_bronze, silver_dim_time] >> \
    [silver_orders, silver_order_events, silver_complaints] >> \
    [silver_weather, dim_order_status] >> \
    silver_deliveries >> \
    [gold_complaints, gold_metrics, gold_summary, gold_business, gold_peak_hours, gold_store_perf, gold_weather]
