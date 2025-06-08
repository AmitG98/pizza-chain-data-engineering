FROM bitnami/spark:latest

USER root

RUN pip install kafka-python
RUN pip install pyspark

COPY app /app
WORKDIR /app

CMD ["spark-submit", "--master", "local[*]", "spark_kafka_to_bronze.py"]
