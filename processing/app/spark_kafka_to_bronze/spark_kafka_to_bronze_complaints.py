from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType

# Schema of the data coming from Kafka
schema = StructType() \
    .add("complaint_id", IntegerType()) \
    .add("order_id", IntegerType()) \
    .add("timestamp_received", TimestampType()) \
    .add("complaint_type", StringType()) \
    .add("description", StringType())

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToBronzeComplaints") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Create the Iceberg table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.complaints_bronze (
    complaint_id INT,
    order_id INT,
    timestamp_received TIMESTAMP,
    complaint_type STRING,
    description STRING
)
USING iceberg
""")

# Batch read from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "complaints-late-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse and extract fields
parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json("json_string", schema).alias("data")) \
    .select("data.*")

# Optional: filter recent complaints only (e.g., last 48 hours)
filtered = parsed.filter(
    col("timestamp_received") >= expr("current_timestamp() - interval 48 hours")
)

# Append to Iceberg table
filtered.writeTo("my_catalog.complaints_bronze").append()
