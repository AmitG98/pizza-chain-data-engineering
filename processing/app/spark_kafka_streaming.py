from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Create Session
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# JSON schema sent from Kafka
order_schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_name", StringType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", TimestampType())

# Reading data from Kafka
df_kafka_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value to JSON and DataFrame
df_parsed = df_kafka_raw \
    .selectExpr("CAST(value AS STRING)") \
    .withColumn("jsonData", from_json(col("value"), order_schema)) \
    .select("jsonData.*")

# Writing to Parquet format
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "/opt/spark-data/bronze/orders") \
    .option("checkpointLocation", "/opt/spark-data/checkpoints/orders") \
    .outputMode("append") \
    .start()

query.awaitTermination()
