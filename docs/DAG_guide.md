# DAG Guide – Using Airflow in the Final Project

This guide explains how to interact with Apache Airflow to view and trigger the ETL pipeline defined in this project.

---

## Step 1: Access Airflow

# After running:

make airflow-up

# Open your browser and navigate to:

http://localhost:8080

# Use the following credentials to log in:

Username: airflow
Password: airflow

## Step 2: Enable DAGs
# Once logged in:

1. Go to the "DAGs" tab.
You will see several DAGs available:

bronze_layer_ingestion
silver_layer_processing
gold_delievery_metrics
silver_quality_checks

2. Enable each DAG by clicking the toggle switch (gray → blue).
This allows Airflow to run them either manually or on a schedule.

## Step 3: Trigger a DAG Manually

1. Click on a DAG name, e.g., bronze_layer_ingestion
2. Inside the DAG page, click the "Play" button at the top-right corner
3. Confirm and trigger the run

## Step 4: Monitor Progress
# Airflow provides several useful views:

1. Graph View
Shows the task dependencies (ETL flow).
Helps visualize the order of execution.

2. Tree View
Shows execution status of each task over time (success, failure, retry).

3. Logs
Click on any task to see its log output (e.g., Spark execution logs, error messages).

