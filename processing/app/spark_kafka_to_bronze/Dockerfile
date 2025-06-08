FROM bitnami/spark:latest

# Remove old Iceberg JARs to avoid conflicts
RUN find /opt/bitnami/spark/jars/ -name "iceberg*" -delete

# Copy Iceberg JAR
COPY app/spark_kafka_to_bronze/jars/iceberg-spark-runtime-4.0_2.12-1.5.0.jar /opt/bitnami/spark/jars/

# Copy all Spark apps
COPY app/spark_kafka_to_bronze/spark_kafka_to_bronze_orders.py /opt/bitnami/spark/app/
COPY app/spark_kafka_to_bronze/spark_kafka_to_bronze_complaints.py /opt/bitnami/spark/app/
COPY app/spark_kafka_to_bronze/spark_kafka_to_bronze_weather.py /opt/bitnami/spark/app/

WORKDIR /opt/bitnami/spark/app/

CMD ["spark-submit", "spark_kafka_to_bronze_orders.py"]
