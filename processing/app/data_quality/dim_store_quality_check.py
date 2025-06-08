from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import sys

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataQualityCheck")

# Spark session
spark = SparkSession.builder \
    .appName("Silver Dim Store Data Quality Check") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

logger.info("=== Starting Data Quality Check for silver_dim_store ===")

# Load data
df = spark.read.format("iceberg").load("my_catalog.silver_dim_store")

# Count issues
null_store_id = df.filter(col("store_id").isNull()).count()
null_region = df.filter(col("region").isNull()).count()
null_admin = df.filter(col("admin").isNull()).count()
null_end_date = df.filter(col("end_date").isNull()).count()
null_is_current = df.filter(col("is_current").isNull()).count()

total_issues = null_store_id + null_region + null_admin + null_end_date + null_is_current

logger.info(f"Null store_id: {null_store_id}")
logger.info(f"Null region: {null_region}")
logger.info(f"Null admin: {null_admin}")
logger.info(f"Null end_date: {null_end_date}")
logger.info(f"Null is_current: {null_is_current}")
logger.info(f"Total issues found: {total_issues}")

# Clean data
df_clean = df.filter(
    col("store_id").isNotNull() &
    col("region").isNotNull() &
    col("admin").isNotNull() &
    col("end_date").isNotNull() &
    col("is_current").isNotNull()
)

# Save only if issues found
if total_issues > 0:
    try:
        logger.info("Saving cleaned data to new Iceberg table: silver_dim_store_valid")
        df_clean.writeTo("my_catalog.silver_dim_store_valid").createOrReplace()
        logger.info("Cleaned table created successfully.")
    except Exception as e:
        logger.error(f"Failed to save cleaned table: {e}")
        sys.exit(1)

    logger.warning("Data quality issues were found, but cleaned table was created.")
else:
    logger.info("All checks passed. No action needed.")
