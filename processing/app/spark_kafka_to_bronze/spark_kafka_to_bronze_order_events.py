from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, expr

# Event schema from Kafka
event_schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("customer_id", IntegerType()) \
    .add("store_id", IntegerType()) \
    .add("event_time", TimestampType()) \
    .add("timestamp", TimestampType()) \
    .add("event_type", StringType())

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToBronzeOrderEvents") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Create Iceberg table if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.order_events_bronze (
  order_id INT,
  customer_id INT,
  store_id INT,
  event_time TIMESTAMP,
  timestamp TIMESTAMP,
  event_type STRING
)
USING iceberg
""")

# Read from Kafka topic: orders-events-topic
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders-events-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse and extract event data
parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json("json_string", event_schema).alias("data")) \
    .select("data.*")

# Optional: filter last 48 hours
filtered = parsed.filter(
    col("event_time") >= expr("current_timestamp() - interval 48 hours")
)

# Append to Iceberg table
filtered.writeTo("my_catalog.order_events_bronze").append()
