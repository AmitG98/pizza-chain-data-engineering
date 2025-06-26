# Final Project – End-to-End Data Engineering Pipeline

## Overview

This project implements a complete real-time and batch data engineering pipeline using:

- **Apache Kafka** for real-time data ingestion
- **Apache Spark** for both batch and streaming processing
- **Apache Iceberg** with **MinIO** (S3-compatible) for managing tables in Bronze, Silver, and Gold layers
- **Apache Airflow** for orchestration of all ETL and validation jobs
- **Docker Compose** for isolated, reproducible environments per component

The system simulates a realistic environment with multiple data sources, handles out-of-order (late-arriving) events, performs data quality checks, and applies proper data modeling techniques including SCD Type 2.

## Project Structure
project-root/
├── minio/                         # MinIO service setup
├── streaming/                     # Kafka, producers, consumers
├── processing/
│   └── app/
│       ├── spark_kafka_to_bronze/     # Bronze Spark jobs
│       ├── spark_bronze_to_silver/    # Silver Spark jobs
│       ├── spark_silver_to_gold/      # Gold Spark jobs
│       └── data_quality/              # Spark jobs for quality checks
├── orchestration/                # Airflow config and DAGs
├── docs/                         # Diagrams and Mermaid.js models
├── Makefile                      # Project control entrypoint
└── README.md                     # Project documentation

---

## How to Run Locally

## Requirements

- Docker + Docker Compose
- On **Windows** or **Mac**, please ensure Docker Desktop is running.
- On **Linux**, Docker can be used without Docker Desktop.

# Test Docker is working by running:
docker info

- No ports in use on: `9000` (MinIO), `8080` (Airflow), `9092` (Kafka)

### Testing the Pipeline

# 1. Start storage (MinIO)
make minio-up

# 2. Start Kafka + all producers & consumers
make streaming-run-all
# or
make streaming-kafka
make streaming-build-producers
make streaming-build-consumers
make streaming-run-producers
make streaming-run-consumers

# 3. Start Airflow
make airflow-up

# 4. Review docs/DAG_guide.md and Browse:

Airflow at http://localhost:8080
(Access Key: airflow, Secret Key: airflow)

MinIO at http://localhost:9000
(Access Key: minioadmin, Secret Key: minioadmin)


## Run complete ETL: Bronze → Silver → Gold
make bronze
make silver
make gold

## Run Data Quality Checks
make quality-all

