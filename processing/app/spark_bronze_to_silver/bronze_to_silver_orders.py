from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, current_timestamp


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

df = spark.read.format("iceberg").load("my_catalog.orders_bronze")

df = df.withColumn("order_time", col("timestamp")) \
        .withColumn("delivery_delay", (unix_timestamp("actual_delivery_time") - unix_timestamp("estimated_delivery_time")) / 60) \
        .withColumn("status", when(col("delivery_delay") <= 0, "on_time").otherwise("late")) \
        .withColumn("ingestion_time", current_timestamp())

clean_df = df.dropna(subset=["order_id", "customer_id", "store_id", "delivery_address", "estimated_delivery_time", "actual_delivery_time"]) \
             .filter((col("order_id") > 0) & (col("customer_id") > 0) & (col("store_id") > 0)) \
             .filter((col("delivery_delay").isNotNull()) & (col("delivery_delay") < 300)) \
             .filter(col("actual_delivery_time") >= col("estimated_delivery_time")) \
             .dropDuplicates()

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver_orders_clean (
  order_id INT,
  customer_id INT,
  store_id INT,
  event_time TIMESTAMP,
  order_time TIMESTAMP,
  delivery_address STRING,
  estimated_delivery_time TIMESTAMP,
  actual_delivery_time TIMESTAMP,
  delivery_delay DOUBLE,
  status STRING,
  ingestion_time TIMESTAMP
)
USING iceberg
""")

clean_df.writeTo("my_catalog.silver_orders_clean").append()
