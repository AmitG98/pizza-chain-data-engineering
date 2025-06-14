from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import sys

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataQualityCheck")

logger.info("=== Starting Data Quality Check for silver_deliveries_enriched ===")

# Start session
spark = SparkSession.builder \
    .appName("Silver Deliveries Data Quality Check") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load table
try:
    df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")
    logger.info("Table loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load table: {e}")
    sys.exit(1)

# Null checks
null_order_id = df.filter(col("order_id").isNull()).count()
null_customer_id = df.filter(col("customer_id").isNull()).count()
null_store_id = df.filter(col("store_id").isNull()).count()
null_order_time = df.filter(col("order_time").isNull()).count()
null_delay_category = df.filter(col("delay_category").isNull()).count()
null_ingestion_time = df.filter(col("ingestion_time").isNull()).count()

# Optional: weather or complaint data might be null (not mandatory)
logger.info(f"Null order_id: {null_order_id}")
logger.info(f"Null customer_id: {null_customer_id}")
logger.info(f"Null store_id: {null_store_id}")
logger.info(f"Null order_time: {null_order_time}")
logger.info(f"Null delay_category: {null_delay_category}")
logger.info(f"Null ingestion_time: {null_ingestion_time}")

total_issues = sum([
    null_order_id,
    null_customer_id,
    null_store_id,
    null_order_time,
    null_delay_category,
    null_ingestion_time
])

if total_issues > 0:
    logger.warning(f"Data quality check failed. Total issues: {total_issues}")
    sys.exit(1)
else:
    logger.info("All checks passed. No critical data quality issues found.")
