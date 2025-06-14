from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp

# Session
spark = SparkSession.builder \
    .appName("GoldComplaintsByType") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Input
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")

# Total complaints
total_complaints = complaints_df.count()

# Grouping by complaint type
complaints_by_type = complaints_df.groupBy("complaint_type").agg(
    count("*").alias("count")
)

# Calculate percentage of total complaints
complaints_by_type = complaints_by_type.withColumn(
    "percent_of_orders", col("count") / total_complaints
)

complaints_by_type = complaints_by_type.withColumn("ingestion_time", current_timestamp())

if not spark.catalog.tableExists("my_catalog.gold_complaints_by_type"):
    complaints_by_type.writeTo("my_catalog.gold_complaints_by_type").createOrReplace()
else:
    complaints_by_type.writeTo("my_catalog.gold_complaints_by_type").append()