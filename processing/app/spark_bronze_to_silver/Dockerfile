FROM bitnami/spark:3.5.1

# Copy Iceberg JAR
COPY jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar /opt/bitnami/spark/jars/

# Copy all Spark apps for Bronze to Silver
COPY bronze_to_silver_orders.py /opt/bitnami/spark/app/
COPY bronze_to_silver_order_events.py /opt/bitnami/spark/app/
COPY bronze_to_silver_complaints.py /opt/bitnami/spark/app/
COPY bronze_to_silver_weather.py /opt/bitnami/spark/app/
COPY bronze_to_silver_deliveries.py /opt/bitnami/spark/app/
COPY generate_silver_dim_time.py /opt/bitnami/spark/app/
COPY silver_dim_store.py /opt/bitnami/spark/app/

WORKDIR /opt/bitnami/spark/app/

# Default command - will be overridden by docker-compose
CMD ["spark-submit", "bronze_to_silver_orders.py"]
