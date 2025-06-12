from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, TimestampType

# Schema
schema = StructType() \
    .add("location", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("temperature", DoubleType()) \
    .add("precipitation", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("weather_condition", StringType())

# SparkSession
spark = SparkSession.builder \
    .appName("KafkaToBronzeWeather") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Create Iceberg table if needed
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.weather_bronze (
    location STRING,
    timestamp TIMESTAMP,
    temperature DOUBLE,
    precipitation DOUBLE,
    wind_speed DOUBLE,
    weather_condition STRING
)
USING iceberg
""")

# Batch read from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-api-topic") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse Kafka JSON
parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json("json_string", schema).alias("data")) \
    .select("data.*")

# Optional: Filter last 48h
filtered = parsed.filter(
    col("timestamp") >= expr("current_timestamp() - interval 48 hours")
)

# Write to Iceberg
filtered.writeTo("my_catalog.weather_bronze").append()
