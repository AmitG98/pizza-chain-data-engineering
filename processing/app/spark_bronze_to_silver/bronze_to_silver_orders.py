from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col

# Session
spark = SparkSession.builder \
    .appName("BronzeToSilverOrders") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load Bronze table
df = spark.read.format("iceberg").load("my_catalog.orders_bronze")

# Add order_time column (copy of timestamp)
df = df.withColumn("order_time", col("timestamp"))

# Add delivery_delay in minutes
df = df.withColumn("delivery_delay",
    (unix_timestamp("actual_delivery_time") - unix_timestamp("estimated_delivery_time")) / 60
)

# Clean data
df_clean = df \
    .dropna(subset=["order_id", "customer_id", "store_id", "delivery_address"]) \
    .filter(col("order_id") > 0) \
    .filter(col("customer_id") > 0) \
    .filter(col("store_id") > 0) \
    .filter(col("delivery_delay").isNotNull() & (col("delivery_delay") >= 0) & (col("delivery_delay") < 300)) \
    .filter(col("actual_delivery_time").isNotNull() & col("estimated_delivery_time").isNotNull()) \
    .filter(col("actual_delivery_time") >= col("estimated_delivery_time")) \
    .dropDuplicates()

# Define the full schema for Silver table
silver_table_schema = """
CREATE TABLE my_catalog.silver_orders_clean (
  order_id INT,
  customer_id INT,
  store_id INT,
  event_time TIMESTAMP,
  order_time TIMESTAMP,
  delivery_address STRING,
  estimated_delivery_time TIMESTAMP,
  actual_delivery_time TIMESTAMP,
  status STRING,
  delivery_delay DOUBLE
)
USING iceberg
"""

# Create the table if it doesn't exist
if not spark.catalog.tableExists("my_catalog.silver_orders_clean"):
    spark.sql(silver_table_schema)

# Write to Silver table
df_clean.writeTo("my_catalog.silver_orders_clean").append()
