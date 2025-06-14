from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, current_timestamp

# Create Spark session
spark = SparkSession.builder \
    .appName("GoldWeatherImpactSummary") \
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
weather_df = spark.read.format("iceberg").load("my_catalog.silver_weather_enriched")
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")

# Rename columns before join to avoid collision
weather_df = weather_df.selectExpr("order_id as w_order_id", "weather_condition as w_condition")

# Join deliveries with weather data
combined_df = deliveries_df.join(weather_df, deliveries_df.order_id == weather_df.w_order_id, "inner")

# Aggregate delivery stats per weather condition
delivery_stats = combined_df.groupBy("w_condition").agg(
    avg("delivery_delay").alias("avg_delivery_delay"),
    (count(when(col("delay_category") == "late", True)) / count("*")).alias("late_deliveries_pct"),
    count("*").alias("total_orders")
)

# Join complaints to weather
complaints_with_weather = complaints_df.join(weather_df, complaints_df.order_id == weather_df.w_order_id, "inner")

# Aggregate complaints per weather condition
complaints_stats = complaints_with_weather.groupBy("w_condition").agg(
    count("*").alias("complaints")
)

# Combine stats
final_df = delivery_stats.join(complaints_stats, on="w_condition", how="left").fillna(0)

# Compute complaints percentage
final_df = final_df.withColumn("complaints_pct", col("complaints") / col("total_orders"))

# Add ingestion timestamp
final_df = final_df.withColumn("ingestion_time", current_timestamp())

# Final column selection
final_df = final_df.select(
    col("w_condition").alias("weather_condition"),
    "avg_delivery_delay",
    "late_deliveries_pct",
    "complaints_pct",
    "ingestion_time"
)

# Write to GOLD table
if not spark.catalog.tableExists("my_catalog.gold_weather_impact_summary"):
    final_df.writeTo("my_catalog.gold_weather_impact_summary").createOrReplace()
else:
    final_df.writeTo("my_catalog.gold_weather_impact_summary").append()
