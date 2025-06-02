FROM bitnami/spark:latest

# Copy Iceberg JAR
COPY jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar /opt/bitnami/spark/jars/

# Copy all Spark apps
COPY spark_kafka_to_bronze_orders.py /opt/bitnami/spark/app/
COPY spark_kafka_to_bronze_complaints.py /opt/bitnami/spark/app/
COPY spark_kafka_to_bronze_weather.py /opt/bitnami/spark/app/

WORKDIR /opt/bitnami/spark/app/

CMD ["spark-submit", "spark_kafka_to_bronze_orders.py"]
