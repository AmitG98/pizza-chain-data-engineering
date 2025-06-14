from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, current_timestamp

# Start Spark session
spark = SparkSession.builder \
    .appName("GoldDeliveryMetricsByRegion") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load Silver tables
deliveries_df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")

# Filter deliveries that have region
deliveries_with_region = deliveries_df.filter(col("region").isNotNull())

# Aggregate delivery stats per region
delivery_stats = deliveries_with_region.groupBy("region").agg(
    avg("delivery_delay").alias("avg_delivery_time"),
    (count(when(col("delay_category") == "late", True)) / count("*")).alias("late_deliveries_pct"),
    count("*").alias("total_orders")
)

# Join complaints with region info via order_id
complaints_with_region = complaints_df.join(
    deliveries_with_region.select("order_id", "region"), on="order_id", how="inner"
)

# Aggregate complaints per region
complaints_stats = complaints_with_region.groupBy("region").agg(
    count("*").alias("complaints")
)

# Join delivery stats with complaints stats
final_df = delivery_stats.join(complaints_stats, on="region", how="left").fillna(0)

# Compute complaint rate per region
final_df = final_df.withColumn("complaints_pct", col("complaints") / col("total_orders"))

# Add ingestion timestamp
final_df = final_df.withColumn("ingestion_time", current_timestamp())

# Select final columns
final_df = final_df.select(
    "region",
    "avg_delivery_time",
    "late_deliveries_pct",
    "complaints_pct",
    "ingestion_time"
)

# Write to Gold table
if not spark.catalog.tableExists("my_catalog.gold_delivery_metrics_by_region"):
    final_df.writeTo("my_catalog.gold_delivery_metrics_by_region").createOrReplace()
else:
    final_df.writeTo("my_catalog.gold_delivery_metrics_by_region").append()
