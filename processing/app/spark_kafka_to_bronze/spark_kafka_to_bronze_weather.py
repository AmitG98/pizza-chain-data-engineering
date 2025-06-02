from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType, TimestampType

schema = StructType() \
    .add("location", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("temperature", DoubleType()) \
    .add("precipitation", DoubleType()) \
    .add("wind_speed", DoubleType()) \
    .add("weather_condition", StringType())

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

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather-api-topic") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json("json_string", schema).alias("data")) \
    .select("data.*")

query = parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/weather_bronze") \
    .toTable("my_catalog.weather_bronze")

query.awaitTermination()


