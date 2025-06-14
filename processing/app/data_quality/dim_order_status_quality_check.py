from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import sys

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataQualityCheck")

# Spark session
spark = SparkSession.builder \
    .appName("Silver Order Status SCD2 Data Quality Check") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

logger.info("=== Starting Data Quality Check for silver_order_status_scd2 ===")

# Load table
try:
    df = spark.read.format("iceberg").load("my_catalog.silver_order_status_scd2")
    logger.info("Table loaded successfully.")
except Exception as e:
    logger.error("Failed to load table: " + str(e))
    sys.exit(1)

# Null checks
null_order_id = df.filter(col("order_id").isNull()).count()
null_status = df.filter(col("status").isNull()).count()
null_effective_time = df.filter(col("effective_time").isNull()).count()
null_ingestion_time = df.filter(col("ingestion_time").isNull()).count()
null_is_current = df.filter(col("is_current").isNull()).count()

total_issues = sum([
    null_order_id,
    null_status,
    null_effective_time,
    null_ingestion_time,
    null_is_current
])

# Log results
logger.info(f"Null order_id: {null_order_id}")
logger.info(f"Null status: {null_status}")
logger.info(f"Null effective_time: {null_effective_time}")
logger.info(f"Null ingestion_time: {null_ingestion_time}")
logger.info(f"Null is_current: {null_is_current}")
logger.info(f"Total issues found: {total_issues}")

# Clean if needed
if total_issues > 0:
    logger.warning("Issues found. Creating cleaned table silver_order_status_scd2_valid")
    try:
        df_clean = df.dropna(subset=["order_id", "status", "effective_time", "ingestion_time", "is_current"])
        df_clean.writeTo("my_catalog.silver_order_status_scd2_valid").createOrReplace()
        logger.info("Cleaned table created successfully.")
    except Exception as e:
        logger.error(f"Failed to save cleaned table: {e}")
        sys.exit(1)
else:
    logger.info("All checks passed. No action needed.")
