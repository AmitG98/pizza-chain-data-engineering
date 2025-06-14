from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
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

# Logger
logger = spark._jvm.org.apache.log4j.LogManager.getLogger("DataQualityCheck")
spark.sparkContext.setLogLevel("INFO")

logger.info("=== Starting Data Quality Check for silver_orders_clean ===")

# Load table
try:
    df = spark.read.format("iceberg").load("my_catalog.silver_orders_clean")
    logger.info("Table loaded successfully.")
except Exception as e:
    logger.error("Failed to load table: " + str(e))
    sys.exit(1)

logger.info("Schema: " + str(df.schema))
df.show(5)

# Individual Checks
issues = []

null_customer_id_df = df.filter(col("customer_id").isNull()).withColumn("issue", lit("null_customer_id"))
if null_customer_id_df.count() > 0:
    logger.warn(f"Null customer_id: {null_customer_id_df.count()}")
    issues.append(null_customer_id_df)

invalid_delay_df = df.filter((col("delivery_delay") < 0) | (col("delivery_delay") > 300)).withColumn("issue", lit("invalid_delivery_delay"))
if invalid_delay_df.count() > 0:
    logger.warn(f"Invalid delivery_delay (not in [0-300]): {invalid_delay_df.count()}")
    issues.append(invalid_delay_df)

duplicate_orders_df = df.groupBy("order_id").count().filter("count > 1")
if duplicate_orders_df.count() > 0:
    logger.warn(f"Duplicate order_id: {duplicate_orders_df.count()}")
    duplicate_ids = [row['order_id'] for row in duplicate_orders_df.collect()]
    duplicate_df = df.filter(col("order_id").isin(duplicate_ids)).withColumn("issue", lit("duplicate_order_id"))
    issues.append(duplicate_df)

null_order_id_df = df.filter(col("order_id").isNull()).withColumn("issue", lit("null_order_id"))
if null_order_id_df.count() > 0:
    logger.warn(f"Null order_id: {null_order_id_df.count()}")
    issues.append(null_order_id_df)

# Combine all issues
if issues:
    final_issues_df = issues[0]
    for other in issues[1:]:
        final_issues_df = final_issues_df.unionByName(other)

    final_issues_df.writeTo("my_catalog.silver_orders_clean_quality_issues").createOrReplace()
    logger.error("Data quality check failed. Issues saved to silver_orders_clean_quality_issues.")
    sys.exit(1)
else:
    logger.info("All checks passed.")
