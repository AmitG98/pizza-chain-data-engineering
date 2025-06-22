# Component Descriptions

This document describes the structure and roles of each component in the end-to-end Data Engineering pipeline.

---

## Component Structure Overview

The project follows a modular and layered architecture, divided into three main directories:

### `streaming/`

Handles real-time data ingestion from simulated sources into Kafka topics and includes tools for monitoring the streaming flow:
- Includes Python **producers** (e.g., `order_producer.py`, `weather_api_producer.py`) that simulate live events and publish them to Kafka topics  
- Includes Python **consumers** (e.g., `order_consumer.py`, `weather_consumer.py`) that subscribe to Kafka topics for monitoring, debugging, or logging purposes  
- Feeds live data into Kafka for downstream consumption by Spark jobs in the Bronze layer

### Kafka Topics
- `orders-topic`: incoming orders stream
- `order-events-topic`: status updates on existing orders
- `complaints-topic`: customer complaints
- `weather-topic`: real-time weather data

### `processing/`

Contains all Spark-based data transformation logic, divided into subfolders:

#### `spark_kafka_to_bronze/`

Consumes raw streaming data from Kafka and saves it as Bronze Iceberg tables:
- `spark_kafka_to_bronze_orders.py`: ingests new orders
- `spark_kafka_to_bronze_order_events.py`: tracks order status events
- `spark_kafka_to_bronze_complaints.py`: stores customer complaints
- `spark_kafka_to_bronze_weather.py`: ingests real-time weather data

#### `spark_bronze_to_silver/`

Transforms raw Kafka data (Bronze) into structured Silver tables:
- `bronze_to_silver_orders.py`: cleans raw orders and calculates delivery delay
- `bronze_to_silver_order_events.py`: extracts order event history (for SCD2)
- `bronze_to_silver_complaints.py`: validates complaint types
- `bronze_to_silver_weather.py`: enriches weather data with region info
- `bronze_to_silver_deliveries.py`: joins complaints + weather + orders
- `generate_silver_dim_time.py`: builds date dimension table
- `silver_dim_order_status.py`: creates SCD2 dimension for order status

#### `spark_silver_to_gold/`

Performs aggregations and builds business-level tables:
- `complaints_by_type.py`: aggregates complaint counts
- `delivery_metrics_by_region.py`: delivery KPIs by region
- `delivery_summary_daily.py`: daily delivery summary
- `gold_daily_business_summary.py`: central business metrics table
- `peak_hours_analysis.py`: peak delivery hour detection
- `store_performance.py`: KPIs per store
- `weather_impact_summary.py`: analyzes weather influence on deliveries

Each script is containerized and executed by Airflow with Docker.

---

### `orchestration/`
Includes all Airflow DAGs to orchestrate the pipeline:
- `full_etl_pipeline.py`: master DAG that connects all processing steps
- `silver_quality_checks.py`: data validation DAG for Silver layer
- Other DAGs may exist for debugging, retries, or isolated runs

---

## Storage: Apache Iceberg + MinIO

- All tables are managed using Apache Iceberg under the `my_catalog` catalog
- Stored in an S3-compatible warehouse via MinIO (`s3a://warehouse`)
- Organized into:
  - **Bronze**: raw data from Kafka
  - **Silver**: cleaned and structured data
  - **Gold**: aggregated, business-ready data

---

## Data Quality Checks

All Silver tables undergo validation via dedicated PySpark scripts.  
Clean versions are saved (with `_valid` suffix) only if issues are found.  
See `data_quality_checks.md` for details.
