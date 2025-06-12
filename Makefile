.PHONY: all bronze silver \
        bronze-orders bronze-weather bronze-complaints \
        silver-orders silver-complaints silver-weather silver-deliveries silver-dim-time silver-dim-store \
		gold-delivery-metrics gold-complaints-by-type gold-delivery-summary gold-peak-hours gold-weather-impact gold-store-performance

# ----------------------------
# Run MinIO
# ----------------------------
minio: minio-reset minio-up

minio-reset:
	@echo ">>> Resetting MinIO (Stopping and removing volumes)..."
	cd minio && docker compose down --volumes

minio-up:
	@echo ">>> Starting MinIO service..."
	cd minio && docker compose up -d
	@echo ">>> MinIO is running at: http://localhost:9000"

# ----------------------------
# Run Kafka & Streaming
# ----------------------------

streaming-kafka:
	docker compose -f streaming/docker-compose.yml up -d zookeeper kafka
	docker network connect data-net kafka || true

streaming-build-producers:
	docker compose -f streaming/docker-compose.yml build order-producer weather-producer late-complaint-producer

streaming-build-consumers:
	docker compose -f streaming/docker-compose.yml build order-consumer weather-consumer late-complaint-consumer

streaming-run-producers:
	docker compose -f streaming/docker-compose.yml up -d order-producer weather-producer late-complaint-producer

streaming-run-consumers:
	docker compose -f streaming/docker-compose.yml up -d order-consumer weather-consumer late-complaint-consumer

streaming-run-all:
	docker compose -f streaming/docker-compose.yml down -v
	docker compose -f streaming/docker-compose.yml build
	docker compose -f streaming/docker-compose.yml up -d
	docker network connect data-net kafka || true

streaming-down:
	docker compose -f streaming/docker-compose.yml down

# ----------------------------
# Run All Jobs
# ----------------------------

all: bronze silver gold

# ----------------------------
# Run Bronze Layer Jobs
# ----------------------------

bronze: bronze-orders bronze-weather bronze-complaints

build-bronze:
	@echo ">>> Building Bronze Layer Docker Images..."
	docker compose -f processing/app/spark_kafka_to_bronze/docker-compose.yml build

bronze-orders:
	@echo ">>> Running Bronze Orders Job..."
	docker compose -f processing/app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-orders-bronze

bronze-weather:
	@echo ">>> Running Bronze Weather Job..."
	docker compose -f processing/app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-weather-bronze

bronze-complaints:
	@echo ">>> Running Bronze Complaints Job..."
	docker compose -f processing/app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-complaints-bronze

# ----------------------------
# Run Silver Layer Jobs
# ----------------------------

silver: silver-orders silver-complaints silver-weather silver-deliveries silver-dim-time silver-dim-store

build-silver:
	@echo ">>> Building Silver Layer Docker Images..."
	docker compose -f processing/app/spark_bronze_to_silver/docker-compose.yml build

silver-orders:
	@echo ">>> Running Silver Orders Job..."
	docker compose -f processing/app/spark_bronze_to_silver/docker-compose.yml run spark-silver-orders spark-submit /opt/bitnami/spark/app/bronze_to_silver_orders.py

silver-complaints:
	@echo ">>> Running Silver Complaints Job..."
	docker compose -f processing/app/spark_bronze_to_silver/docker-compose.yml run spark-silver-complaints spark-submit /opt/bitnami/spark/app/bronze_to_silver_complaints.py

silver-weather:
	@echo ">>> Running Silver Weather Job..."
	docker compose -f processing/app/spark_bronze_to_silver/docker-compose.yml run spark-silver-weather spark-submit /opt/bitnami/spark/app/bronze_to_silver_weather.py

silver-deliveries:
	@echo ">>> Running Silver Deliveries Job..."
	docker compose -f processing/app/spark_bronze_to_silver/docker-compose.yml run spark-silver-deliveries spark-submit /opt/bitnami/spark/app/bronze_to_silver_deliveries.py

silver-dim-time:
	@echo ">>> Running Silver Dim Time Job..."
	docker compose -f processing/app/spark_bronze_to_silver/docker-compose.yml run spark-silver-dim-time spark-submit /opt/bitnami/spark/app/generate_silver_dim_time.py

silver-dim-store:
	@echo ">>> Running Silver Dim Store Job..."
	docker compose -f processing/app/spark_bronze_to_silver/docker-compose.yml run spark-silver-dim-store spark-submit /opt/bitnami/spark/app/silver_dim_store.py

# ----------------------------
# Run Gold Layer Jobs
# ----------------------------

gold: \
    gold-delivery-metrics \
    gold-complaints-by-type \
    gold-delivery-summary \
    gold-peak-hours \
    gold-weather-impact \
    gold-store-performance

build-gold:
	@echo ">>> Building Gold Layer Docker Images..."
	docker compose -f processing/app/spark_silver_to_gold/docker-compose.yml build

gold-delivery-metrics:
	@echo ">>> Running Gold Delivery Metrics By Region Job..."
	docker compose -f processing/app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-delivery-metrics

gold-complaints-by-type:
	@echo ">>> Running Gold Complaints By Type Job..."
	docker compose -f processing/app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-complaints-by-type

gold-delivery-summary:
	@echo ">>> Running Gold Delivery Summary Daily Job..."
	docker compose -f processing/app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-delivery-summary-daily

gold-peak-hours:
	@echo ">>> Running Gold Peak Hours Analysis Job..."
	docker compose -f processing/app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-peak-hours-analysis

gold-weather-impact:
	@echo ">>> Running Gold Weather Impact Summary Job..."
	docker compose -f processing/app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-weather-impact-summary

gold-store-performance:
	@echo ">>> Running Gold Store Performance Job..."
	docker compose -f processing/app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-store-performance

# ----------------------------
# Run Data Quality Checks
# ----------------------------

quality-all: \
	quality-orders \
	quality-complaints \
	quality-weather \
	quality-deliveries \
	quality-dim-time \
	quality-dim-store
	@echo ">>> All Quality Checks Completed Successfully."

quality-build:
	@echo ">>> Building all Data Quality Docker Images..."
	docker compose -f processing/app/data_quality/docker-compose.yml build

quality-orders:
	@echo ">>> Running Data Quality Checks for Orders..."
	docker compose -f processing/app/data_quality/docker-compose.yml run --rm spark-quality-check

quality-complaints:
	@echo ">>> Running Data Quality Checks for Complaints..."
	docker compose -f processing/app/data_quality/docker-compose.yml run --rm spark-quality-complaints

quality-weather:
	@echo ">>> Running Data Quality Checks for Weather..."
	docker compose -f processing/app/data_quality/docker-compose.yml run --rm spark-quality-weather

quality-deliveries:
	@echo ">>> Running Data Quality Checks for Deliveries..."
	docker compose -f processing/app/data_quality/docker-compose.yml run --rm spark-quality-deliveries

quality-dim-time:
	@echo ">>> Running Data Quality Checks for Dim Time..."
	docker compose -f processing/app/data_quality/docker-compose.yml run --rm spark-quality-dim-time

quality-dim-store:
	@echo ">>> Running Data Quality Checks for Dim Store..."
	docker compose -f processing/app/data_quality/docker-compose.yml run --rm spark-quality-dim-store

# ----------------------------
# Run Data Air flow
# ----------------------------

airflow-up:
	docker compose -f orchestration/docker-compose.yml up -d --build

airflow-down:
	docker compose -f orchestration/docker-compose.yml down