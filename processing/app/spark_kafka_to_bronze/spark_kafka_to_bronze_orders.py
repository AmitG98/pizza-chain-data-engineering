from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, expr

# Schema of the data coming from Kafka (no status)
order_schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("customer_id", IntegerType()) \
    .add("store_id", IntegerType()) \
    .add("event_time", TimestampType()) \
    .add("timestamp", TimestampType()) \
    .add("delivery_address", StringType()) \
    .add("estimated_delivery_time", TimestampType()) \
    .add("actual_delivery_time", TimestampType())

spark = SparkSession.builder \
    .appName("KafkaToBronzeOrders") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Table definition (no status)
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.orders_bronze (
  order_id INT,
  customer_id INT,
  store_id INT,
  event_time TIMESTAMP,
  timestamp TIMESTAMP,
  delivery_address STRING,
  estimated_delivery_time TIMESTAMP,
  actual_delivery_time TIMESTAMP
)
USING iceberg
""")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json("json_string", order_schema).alias("data")) \
    .select("data.*")

filtered = parsed.filter(
    col("event_time") >= expr("current_timestamp() - interval 48 hours")
)

filtered.writeTo("my_catalog.orders_bronze").append()
