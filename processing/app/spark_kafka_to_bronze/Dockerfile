FROM bitnami/spark:3.5.1

# Remove old Iceberg JARs to avoid conflicts
RUN find /opt/bitnami/spark/jars/ -name "iceberg*" -delete

# Copy Iceberg JAR
COPY jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar /opt/bitnami/spark/jars/

# Copy Spark apps
COPY spark_kafka_to_bronze_orders.py /opt/bitnami/spark/app/
COPY spark_kafka_to_bronze_complaints.py /opt/bitnami/spark/app/
COPY spark_kafka_to_bronze_weather.py /opt/bitnami/spark/app/

WORKDIR /opt/bitnami/spark/app/

# שורה זו כבר לא דרושה כי ה־command מוגדר ב־docker-compose.yml
# CMD ["spark-submit", "spark_kafka_to_bronze_orders.py"]
