from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, expr, trim, lower, abs as abs_spark

# Start SparkSession
spark = SparkSession.builder \
    .appName("BronzeToSilverWeather") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load tables
weather_df = spark.read.format("iceberg").load("my_catalog.weather_bronze")
orders_df = spark.read.format("iceberg").load("my_catalog.silver_orders_clean")

# Remove NULLs from weather data
weather_df = weather_df.dropna(subset=["timestamp", "location", "temperature", "precipitation", "wind_speed", "weather_condition"])

# Add region column to orders (extract from delivery_address)
orders_df = orders_df.withColumn("region", trim(expr("split(delivery_address, ',')[1]")))

# Normalize region and city names
orders_df = orders_df.withColumn("region_normalized", lower(col("region")))
weather_df = weather_df.withColumn("city_normalized", lower(trim(col("location"))))

# Join on time ±1 hour and matching region
joined = orders_df.alias("orders").join(
    weather_df.alias("weather"),
    (col("orders.region_normalized") == col("weather.city_normalized")) &
    (abs_spark(unix_timestamp(col("orders.order_time")) - unix_timestamp(col("weather.timestamp"))) <= 3600),
    how="left"
)

# Select relevant columns
result = joined.select(
    "orders.order_id",
    "orders.order_time",
    "orders.region",
    "weather.temperature",
    "weather.precipitation",
    "weather.wind_speed",
    "weather.weather_condition"
).dropDuplicates()

# Write to Silver table
result.writeTo("my_catalog.silver_weather_enriched").createOrReplace()
