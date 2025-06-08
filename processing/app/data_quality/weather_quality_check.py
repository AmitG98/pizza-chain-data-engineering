from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("WeatherDataQualityCheck")

logger.info("=== Starting Data Quality Check for silver_weather_enriched ===")

# Spark session
spark = SparkSession.builder \
    .appName("Silver Weather Data Quality Check") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load data
df = spark.read.format("iceberg").load("my_catalog.silver_weather_enriched")

# === Data Quality Checks ===
logger.info("Running data quality checks...")

# Null checks
null_order_id = df.filter(col("order_id").isNull()).count()
null_order_time = df.filter(col("order_time").isNull()).count()
null_region = df.filter(col("region").isNull()).count()
null_temp = df.filter(col("temperature").isNull()).count()
null_precip = df.filter(col("precipitation").isNull()).count()
null_wind = df.filter(col("wind_speed").isNull()).count()
null_condition = df.filter(col("weather_condition").isNull()).count()

# Range checks
invalid_temp = df.filter((col("temperature") < -50) | (col("temperature") > 60)).count()
invalid_precip = df.filter(col("precipitation") < 0).count()
invalid_wind = df.filter((col("wind_speed") < 0) | (col("wind_speed") > 300)).count()

# Log results
logger.info(f"Null order_id: {null_order_id}")
logger.info(f"Null order_time: {null_order_time}")
logger.info(f"Null region: {null_region}")
logger.info(f"Null temperature: {null_temp}")
logger.info(f"Null precipitation: {null_precip}")
logger.info(f"Null wind_speed: {null_wind}")
logger.info(f"Null weather_condition: {null_condition}")
logger.info(f"Invalid temperature range: {invalid_temp}")
logger.info(f"Invalid precipitation range: {invalid_precip}")
logger.info(f"Invalid wind_speed range: {invalid_wind}")

# Total
total_issues = sum([
    null_order_id, null_order_time, null_region,
    null_temp, null_precip, null_wind, null_condition,
    invalid_temp, invalid_precip, invalid_wind
])
logger.info(f"Total issues found: {total_issues}")

# Clean data
df_clean = df.filter(
    col("order_id").isNotNull() &
    col("order_time").isNotNull() &
    col("region").isNotNull() &
    col("temperature").isNotNull() & (col("temperature") >= -50) & (col("temperature") <= 60) &
    col("precipitation").isNotNull() & (col("precipitation") >= 0) &
    col("wind_speed").isNotNull() & (col("wind_speed") >= 0) & (col("wind_speed") <= 300) &
    col("weather_condition").isNotNull()
)

# Save if issues found
if total_issues > 0:
    try:
        logger.info("Saving cleaned data to new Iceberg table: silver_weather_enriched_valid")
        df_clean.writeTo("my_catalog.silver_weather_enriched_valid").createOrReplace()
        logger.info("Cleaned table created successfully.")
    except Exception as e:
        logger.error(f"Failed to save cleaned table: {e}")
        sys.exit(1)

    logger.warning("Data quality issues were found, but cleaned table was created.")
else:
    logger.info("All checks passed. No action needed.")
