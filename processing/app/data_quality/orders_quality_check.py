from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Start Spark Session
spark = SparkSession.builder \
    .appName("Silver Orders Data Quality Check") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Use Spark logger
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("DataQualityCheck")
spark.sparkContext.setLogLevel("INFO")

logger.info("=== Starting Data Quality Check for silver_orders_clean ===")

try:
    df = spark.read.format("iceberg").load("my_catalog.silver_orders_clean")
    logger.info("Table loaded successfully.")
except Exception as e:
    logger.error("Failed to load table: " + str(e))
    sys.exit(1)

logger.info("Schema: " + str(df.schema))
df.show(5)

# Null checks
null_customer_id = df.filter(col("customer_id").isNull()).count()
logger.info(f"Null customer_id: {null_customer_id}")

# Range check
invalid_delay = df.filter((col("delivery_delay") < 0) | (col("delivery_delay") > 300)).count()
logger.info(f"Invalid delivery_delay (not in [0-300]): {invalid_delay}")

# Enum check for status
allowed_statuses = ["delivered", "delayed", "canceled"]
invalid_status = df.filter(~col("status").isin(allowed_statuses)).count()
logger.info(f"Invalid status values: {invalid_status}")

# Duplicate check
duplicate_orders = df.groupBy("order_id").count().filter("count > 1").count()
logger.info(f"Duplicate order_id: {duplicate_orders}")

# Null primary key
null_order_id = df.filter(col("order_id").isNull()).count()
logger.info(f"Null order_id: {null_order_id}")

# Total issues
total_issues = sum([null_customer_id, invalid_delay, invalid_status, duplicate_orders, null_order_id])
logger.info(f"Total issues found: {total_issues}")

if total_issues > 0:
    logger.error("Data quality check failed.")
    sys.exit(1)
else:
    logger.info("All checks passed.")
