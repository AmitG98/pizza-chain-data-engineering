from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Set up logging
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

df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")

logger.info("Running data quality checks...")

# Basic null checks (use only existing columns!)
null_order_id = df.filter(col("order_id").isNull()).count()
null_customer_id = df.filter(col("customer_id").isNull()).count()
null_store_id = df.filter(col("store_id").isNull()).count()
null_order_time = df.filter(col("order_time").isNull()).count()

logger.info(f"Null order_id: {null_order_id}")
logger.info(f"Null customer_id: {null_customer_id}")
logger.info(f"Null store_id: {null_store_id}")
logger.info(f"Null order_time: {null_order_time}")

total_issues = sum([
    null_order_id,
    null_customer_id,
    null_store_id,
    null_order_time
])

logger.info(f"Total issues found: {total_issues}")

if total_issues > 0:
    logger.warning("Data quality check failed.")
    exit(1)
else:
    logger.info("Data quality check passed.")
