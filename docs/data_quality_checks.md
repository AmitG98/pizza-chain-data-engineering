# Data Quality Checks

This document outlines the data quality checks applied to the Silver layer tables in the ETL pipeline.

Each check is implemented as a dedicated Spark job, executed via Airflow DockerOperator, and saves a cleaned version of the table only if issues are found.

---

## silver_orders_clean

**Checks performed:**
- `order_id` is not null
- `customer_id` is not null
- `status` is either `on_time` or `late`
- `delivery_delay` is within range [0, 300]
- No duplicate `order_id`

**Actions:**
- Problematic rows are written to: `silver_orders_clean_quality_issues`
- Pipeline fails if issues are found

---

## silver_complaints_clean

**Checks performed:**
- Non-null: `complaint_id`, `order_id`, `complaint_type`
- `complaint_type` in: `["Late delivery", "Missing item", "Wrong order", "Cold food"]`
- No duplicate `complaint_id`

**Actions:**
- If issues are found, valid rows are saved to: `silver_complaints_clean_valid`
- If no valid rows exist, pipeline exits with failure

---

## silver_weather_enriched

**Checks performed:**
- Non-null: `order_id`, `order_time`, `region`, `temperature`, `precipitation`, `wind_speed`, `weather_condition`
- `temperature` in range: [-50°C, 60°C]
- `precipitation` ≥ 0
- `wind_speed` in range: [0, 300]

**Actions:**
- If issues are found, valid rows are saved to: `silver_weather_enriched_valid`

---

## silver_deliveries_enriched

**Checks performed:**
- Non-null: `order_id`, `customer_id`, `store_id`, `order_time`, `delay_category`, `ingestion_time`

**Actions:**
- Pipeline fails immediately if any of the above are missing

---

## silver_order_status_scd2

**Checks performed:**
- Non-null: `order_id`, `status`, `effective_time`, `ingestion_time`, `is_current`

**Actions:**
- If issues found, cleaned table saved to: `silver_order_status_scd2_valid`

---

## silver_dim_time

**Checks performed:**
- Non-null: `date`, `day_of_week`, `is_weekend`, `is_holiday`, `is_short_friday`

**Actions:**
- Pipeline fails if any critical fields are missing

---

## Execution

All quality checks are triggered from a dedicated DAG: `silver_quality_checks`.  
Each check runs inside its own Docker container using Spark, and logs are visible in Airflow.

---

## Notes

- Cleaned tables are only written if issues are detected.
- Tables use Iceberg format and are stored on MinIO (S3-compatible).
- No data is deleted from original silver tables — only filtered copies are created.

