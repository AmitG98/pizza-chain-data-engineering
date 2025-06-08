.PHONY: all bronze silver \
        bronze-orders bronze-weather bronze-complaints \
        silver-orders silver-complaints silver-weather silver-deliveries silver-dim-time silver-dim-store \
		gold-delivery-metrics gold-complaints-by-type gold-delivery-summary gold-peak-hours gold-weather-impact gold-store-performance


# ----------------------------
# Run All Jobs
# ----------------------------
all: bronze silver gold

# ----------------------------
# Run Bronze Layer Jobs
# ----------------------------
bronze: bronze-orders bronze-weather bronze-complaints

bronze-orders:
	@echo ">>> Running Bronze Orders Job..."
	docker compose -f app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-orders-bronze

bronze-weather:
	@echo ">>> Running Bronze Weather Job..."
	docker compose -f app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-weather-bronze

bronze-complaints:
	@echo ">>> Running Bronze Complaints Job..."
	docker compose -f app/spark_kafka_to_bronze/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-complaints-bronze

# ----------------------------
# Run Silver Layer Jobs
# ----------------------------
silver: silver-orders silver-complaints silver-weather silver-deliveries silver-dim-time silver-dim-store

silver-orders:
	@echo ">>> Running Silver Orders Job..."
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-orders spark-submit /opt/bitnami/spark/app/bronze_to_silver_orders.py

silver-complaints:
	@echo ">>> Running Silver Complaints Job..."
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-complaints spark-submit /opt/bitnami/spark/app/bronze_to_silver_complaints.py

silver-weather:
	@echo ">>> Running Silver Weather Job..."
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-weather spark-submit /opt/bitnami/spark/app/bronze_to_silver_weather.py

silver-deliveries:
	@echo ">>> Running Silver Deliveries Job..."
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-deliveries spark-submit /opt/bitnami/spark/app/bronze_to_silver_deliveries.py

silver-dim-time:
	@echo ">>> Running Silver Dim Time Job..."
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-dim-time spark-submit /opt/bitnami/spark/app/generate_silver_dim_time.py

silver-dim-store:
	@echo ">>> Running Silver Dim Store Job..."
	docker compose -f app/spark_bronze_to_silver/docker-compose.yml run spark-silver-dim-store spark-submit /opt/bitnami/spark/app/silver_dim_store.py

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

gold-delivery-metrics:
	@echo ">>> Running Gold Delivery Metrics By Region Job..."
	docker compose -f app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-delivery-metrics

gold-complaints-by-type:
	@echo ">>> Running Gold Complaints By Type Job..."
	docker compose -f app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-complaints-by-type

gold-delivery-summary:
	@echo ">>> Running Gold Delivery Summary Daily Job..."
	docker compose -f app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-delivery-summary-daily

gold-peak-hours:
	@echo ">>> Running Gold Peak Hours Analysis Job..."
	docker compose -f app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-peak-hours-analysis

gold-weather-impact:
	@echo ">>> Running Gold Weather Impact Summary Job..."
	docker compose -f app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-weather-impact-summary

gold-store-performance:
	@echo ">>> Running Gold Store Performance Job..."
	docker compose -f app/spark_silver_to_gold/docker-compose.yml up --build --abort-on-container-exit --exit-code-from spark-gold-store-performance

# ----------------------------
# Run Data quality check
# ----------------------------
quality-orders:
	@echo ">>> Running Data Quality Checks for Orders..."
	docker compose -f app/data_quality/docker-compose.yml run --rm spark-quality-check

quality-complaints:
	@echo ">>> Running Data Quality Checks for Complaints..."
	docker compose -f app/data_quality/docker-compose.yml run --rm spark-quality-complaints

quality-weather:
	@echo ">>> Running Data Quality Checks for Weather..."
	docker compose -f app/data_quality/docker-compose.yml run --rm spark-quality-weather

quality-deliveries:
	@echo ">>> Running Data Quality Checks for deliveries..."
	docker compose -f app/data_quality/docker-compose.yml run --rm spark-quality-deliveries

quality-dim-time:
	@echo ">>> Running Data Quality Checks for dim-time..."
	docker compose -f app/data_quality/docker-compose.yml run --rm spark-quality-dim-time

quality-dim-store:
	@echo ">>> Running Data Quality Checks for dim-store..."
	docker compose -f app/data_quality/docker-compose.yml run --rm spark-quality-dim-store

quality-all: quality-orders quality-complaints
	@echo ">>> All Quality Checks Completed"

