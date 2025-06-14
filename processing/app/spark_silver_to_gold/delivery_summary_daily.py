from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count, when, first, current_timestamp

# Spark session
spark = SparkSession.builder \
    .appName("GoldDeliverySummaryDaily") \
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

# Add date column
deliveries_df = deliveries_df.withColumn("date", to_date("order_time"))

# Aggregate delivery summary by date
daily_summary = deliveries_df.groupBy("date").agg(
    count("*").alias("total_orders"),
    count(when(col("delay_category") == "late", True)).alias("late_orders")
)

# Add date to weather and aggregate one condition per day
weather_df = weather_df.withColumn("date", to_date("order_time"))

weather_summary_df = weather_df.groupBy("date").agg(
    first("weather_condition", ignorenulls=True).alias("weather_summary")
)

# Join delivery and weather summaries
final_df = daily_summary.join(weather_summary_df, on="date", how="left")

# Add ingestion timestamp
final_df = final_df.withColumn("ingestion_time", current_timestamp())

# Write to Gold table
if not spark.catalog.tableExists("my_catalog.gold_delivery_summary_daily"):
    final_df.writeTo("my_catalog.gold_delivery_summary_daily").createOrReplace()
else:
    final_df.writeTo("my_catalog.gold_delivery_summary_daily").append()
