from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp


spark = SparkSession.builder \
    .appName("BronzeToSilverOrderEvents") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.format("iceberg").load("my_catalog.order_events_bronze")
df = df.withColumn("ingestion_time", current_timestamp())

clean_df = df.dropna(subset=["order_id", "event_time", "event_type"]) \
             .filter(col("event_type").isin([
                 "order_received", "order_in_preparation",
                 "order_sent_out_for_delivery", "order_delivered"
             ])) \
             .filter(col("event_time") >= expr("current_timestamp() - interval 48 hours")) \
             .dropDuplicates()

spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.silver_order_events_clean (
  order_id INT,
  customer_id INT,
  store_id INT,
  event_time TIMESTAMP,
  timestamp TIMESTAMP,
  event_type STRING,
  ingestion_time TIMESTAMP
)
USING iceberg
""")

clean_df.writeTo("my_catalog.silver_order_events_clean").append()
