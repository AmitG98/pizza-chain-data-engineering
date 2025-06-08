# Data Quality Checks

## silver_orders_clean

**Fields Checked:**
- `customer_id` – must not be null
- `order_id` – must not be null and must be unique
- `delivery_delay` – must be between 0 and 300 minutes
- `status` – must be one of: delivered, delayed, canceled

**Check Script:**  
Location: `processing/data_quality/orders_quality_check.py`  
Run manually or as part of the pipeline (Makefile target: `quality-orders`)

**Failure behavior:**  
If any of the checks fail, the job will exit with non-zero status.
