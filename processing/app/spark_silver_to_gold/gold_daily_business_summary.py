from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, to_date, current_timestamp, expr

# Start Spark session
spark = SparkSession.builder \
    .appName("GoldDailyBusinessSummary") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load silver tables
orders = spark.read.format("iceberg").load("my_catalog.silver_orders_clean")
complaints = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")
events = spark.read.format("iceberg").load("my_catalog.silver_order_events_clean")
weather = spark.read.format("iceberg").load("my_catalog.silver_weather_enriched")

# Add date column from order_time
orders = orders.withColumn("date", to_date("order_time"))

# Complaints per order
complaints_per_order = complaints.groupBy("order_id").agg(count("*").alias("has_complaint"))

# Join complaints
orders = orders.join(complaints_per_order, on="order_id", how="left") \
               .withColumn("has_complaint", when(col("has_complaint").isNotNull(), 1).otherwise(0))

# Event durations (prep + delivery)
pivoted_events = events.groupBy("order_id").pivot("event_type").agg(expr("max(event_time)"))
pivoted_events = pivoted_events \
    .withColumn("prep_minutes", (col("order_in_preparation").cast("long") - col("order_received").cast("long")) / 60) \
    .withColumn("delivery_minutes", (col("order_delivered").cast("long") - col("order_sent_out_for_delivery").cast("long")) / 60)

orders = orders.join(pivoted_events.select("order_id", "prep_minutes", "delivery_minutes"), on="order_id", how="left")

# Join weather and classify severe weather
orders = orders.join(weather.select("order_id", "precipitation", "wind_speed"), on="order_id", how="left") \
    .withColumn("severe_weather", when((col("precipitation") > 20) | (col("wind_speed") > 40), 1).otherwise(0))

# Group by date & store_id
summary = orders.groupBy("date", "store_id").agg(
    count("*").alias("orders_count"),
    count(when(col("status") == "late", True)).alias("late_count"),
    count(when(col("status") == "on_time", True)).alias("on_time_count"),
    count(when(col("has_complaint") == 1, True)).alias("complaints_count"),
    avg("delivery_delay").alias("avg_delivery_delay"),
    avg("prep_minutes").alias("avg_prep_time"),
    avg("delivery_minutes").alias("avg_delivery_time"),
    count(when(col("severe_weather") == 1, True)).alias("severe_weather_count")
).withColumn("ingestion_time", current_timestamp())

# Write to Iceberg table
if not spark.catalog.tableExists("my_catalog.gold_daily_business_summary"):
    summary.writeTo("my_catalog.gold_daily_business_summary").createOrReplace()
else:
    summary.writeTo("my_catalog.gold_daily_business_summary").append()
