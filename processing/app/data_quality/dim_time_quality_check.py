from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DataQualityCheck")

logger.info("=== Starting Data Quality Check for silver_dim_time ===")

# Spark session
spark = SparkSession.builder \
    .appName("Silver Dim Time Data Quality Check") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.format("iceberg").load("my_catalog.silver_dim_time")

# Data quality checks (existing columns only)
null_date = df.filter(col("date").isNull()).count()
null_day_of_week = df.filter(col("day_of_week").isNull()).count()
null_is_weekend = df.filter(col("is_weekend").isNull()).count()
null_is_holiday = df.filter(col("is_holiday").isNull()).count()
null_is_short_friday = df.filter(col("is_short_friday").isNull()).count()

logger.info(f"Null date: {null_date}")
logger.info(f"Null day_of_week: {null_day_of_week}")
logger.info(f"Null is_weekend: {null_is_weekend}")
logger.info(f"Null is_holiday: {null_is_holiday}")
logger.info(f"Null is_short_friday: {null_is_short_friday}")

total_issues = sum([
    null_date,
    null_day_of_week,
    null_is_weekend,
    null_is_holiday,
    null_is_short_friday
])

logger.info(f"Total issues found: {total_issues}")

if total_issues > 0:
    logger.warning("Data quality check failed.")
    exit(1)
else:
    logger.info("Data quality check passed.")
