# Makefile for running Spark jobs in processing/app

.PHONY: all bronze silver

# ----------------------------
# Run All Jobs
# ----------------------------
all: bronze silver

# ----------------------------
# Run Bronze Layer Jobs
# ----------------------------
bronze:
	@echo ">>> Running Bronze Layer Jobs..."
	docker compose -f app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-orders-bronze
	docker compose -f app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-weather-bronze
	docker compose -f app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-complaints-bronze

# ----------------------------
# Run Silver Layer Jobs
# ----------------------------
silver:
	@echo ">>> Running Silver Layer Jobs..."
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-orders spark-submit /opt/bitnami/spark/app/bronze_to_silver_orders.py
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-complaints spark-submit /opt/bitnami/spark/app/bronze_to_silver_complaints.py
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-weather spark-submit /opt/bitnami/spark/app/bronze_to_silver_weather.py
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-deliveries spark-submit /opt/bitnami/spark/app/bronze_to_silver_deliveries.py
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-dim-time spark-submit /opt/bitnami/spark/app/generate_silver_dim_time.py
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-dim-store spark-submit /opt/bitnami/spark/app/silver_dim_store.py
