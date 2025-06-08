from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DataQualityCheck")

logger.info("=== Starting Data Quality Check for silver_complaints_clean ===")

# Spark session
spark = SparkSession.builder \
    .appName("Silver Complaints Data Quality Check") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load data
df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")

# === Data Quality Checks ===
logger.info("Running data quality checks...")

# Null checks
null_complaint_id = df.filter(col("complaint_id").isNull()).count()
null_order_id = df.filter(col("order_id").isNull()).count()
null_type = df.filter(col("complaint_type").isNull()).count()

# Enum check
valid_types = ["Late delivery", "Missing item", "Wrong order", "Cold food"]
invalid_type = df.filter(~col("complaint_type").isin(valid_types)).count()

# Duplicate complaints
duplicate_ids = df.groupBy("complaint_id").count().filter("count > 1").count()

# Logging results
logger.info(f"Null complaint_id: {null_complaint_id}")
logger.info(f"Null order_id: {null_order_id}")
logger.info(f"Null complaint_type: {null_type}")
logger.info(f"Invalid complaint_type: {invalid_type}")
logger.info(f"Duplicate complaint_id: {duplicate_ids}")

# Summary
total_issues = sum([null_complaint_id, null_order_id, null_type, invalid_type, duplicate_ids])
logger.info(f"Total issues found: {total_issues}")

# Save cleaned version to a new Iceberg table
df_cleaned = df.filter(
    col("complaint_id").isNotNull() &
    col("order_id").isNotNull() &
    col("complaint_type").isNotNull() &
    col("complaint_type").isin(valid_types)
)

# Exit status based on issues
if total_issues > 0:
    logger.info("Preparing cleaned version of the data...")
    if df_cleaned.count() == 0:
        logger.warning("Cleaned dataframe is empty - no table will be written.")
        sys.exit(1)
    try:
        logger.info("Saving cleaned data to new Iceberg table: silver_complaints_clean_valid")
        df_cleaned.writeTo("my_catalog.silver_complaints_clean_valid").createOrReplace()
        logger.info("Cleaned table created successfully.")
    except Exception as e:
        logger.error(f"Failed to save cleaned table: {e}")
        sys.exit(1)
    logger.warning("Data quality issues were found, but cleaned table was created.")
else:
    logger.info("All checks passed. No action needed.")

