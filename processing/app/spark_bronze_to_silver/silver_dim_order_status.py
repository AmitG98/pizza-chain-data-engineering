from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lead, current_timestamp

# Start Spark session
spark = SparkSession.builder \
    .appName("SilverOrderStatusSCD2") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load clean order events
df = spark.read.format("iceberg").load("my_catalog.silver_order_events_clean")

# Create window spec by order_id ordered by event_time
window_spec = Window.partitionBy("order_id").orderBy("event_time")

# Add SCD2 fields
df = df.withColumn("effective_time", col("event_time")) \
       .withColumn("end_time", lead("event_time").over(window_spec)) \
       .withColumn("is_current", col("end_time").isNull()) \
       .withColumn("ingestion_time", current_timestamp())

# Select only needed fields
result = df.select(
    "order_id",
    col("event_type").alias("status"),
    "effective_time",
    "end_time",
    "is_current",
    "ingestion_time"
)

# Write to Iceberg table
if not spark.catalog.tableExists("my_catalog.silver_order_status_scd2"):
    result.writeTo("my_catalog.silver_order_status_scd2").createOrReplace()
else:
    result.writeTo("my_catalog.silver_order_status_scd2").append()
