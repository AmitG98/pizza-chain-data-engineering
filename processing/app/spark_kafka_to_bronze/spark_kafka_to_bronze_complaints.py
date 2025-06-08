from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType

# Schema of the data coming from Kafka
schema = StructType() \
    .add("complaint_id", IntegerType()) \
    .add("order_id", IntegerType()) \
    .add("timestamp_received", TimestampType()) \
    .add("complaint_type", StringType()) \
    .add("description", StringType())

# Create a SparkSession with settings for Iceberg and MinIO
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

# Create Iceberg table if it doesn't exist
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

# Reading from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "complaints-late-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert from JSON to DataFrame
parsed = df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json("json_string", schema).alias("data")) \
    .select("data.*")

# Writing to a table using Iceberg
query = parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/complaints_bronze") \
    .toTable("my_catalog.complaints_bronze")

query.awaitTermination()

