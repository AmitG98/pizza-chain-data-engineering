from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# Start SparkSession
spark = SparkSession.builder \
    .appName("BronzeToSilverComplaints") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load Bronze table
df = spark.read.format("iceberg").load("my_catalog.complaints_bronze")

# Clean data
df_clean = df \
    .dropna(subset=["complaint_id", "order_id", "timestamp_received"]) \
    .filter(trim(col("complaint_type")) != "") \
    .filter(trim(col("description")) != "") \
    .filter(col("complaint_id") >= 1) \
    .filter(col("order_id") >= 1) \
    .dropDuplicates()

# Write to Silver table
if not spark.catalog.tableExists("my_catalog.silver_complaints_clean"):
    df_clean.writeTo("my_catalog.silver_complaints_clean").createOrReplace()
else:
    df_clean.writeTo("my_catalog.silver_complaints_clean").append()

