from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan

# Start Spark session
spark = SparkSession.builder \
    .appName("BronzeToSilverDeliveries") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Load silver tables
orders_df = spark.read.format("iceberg").load("my_catalog.silver_orders_clean")
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")
weather_df = spark.read.format("iceberg").load("my_catalog.silver_weather_enriched")

# Join complaints (left join – not every order has complaint)
orders_with_complaints = orders_df.join(
    complaints_df.select("order_id", "complaint_type"),
    on="order_id",
    how="left"
)

# Join weather (left join)
orders_with_weather = orders_with_complaints.join(
    weather_df.drop("region", "order_time"),  # drop extra fields
    on="order_id",
    how="left"
)

# Add derived columns
final_df = orders_with_weather.withColumn(
    "had_complaint", when(col("complaint_type").isNotNull(), True).otherwise(False)
)

# Optionally categorize delivery delay
final_df = final_df.withColumn(
    "delay_category",
    when(col("delivery_delay") < 5, "on_time")
    .when(col("delivery_delay") < 15, "minor_delay")
    .when(col("delivery_delay") < 60, "major_delay")
    .otherwise("extreme_delay")
)

# Filter bad or missing order_id
final_df = final_df.filter(
    col("order_id").isNotNull() &
    (col("order_id") > 0)
)

# Remove duplicates (safety net)
final_df = final_df.dropDuplicates(["order_id"])

# Select and reorder columns
columns = [
    "order_id", "customer_id", "store_id", "delivery_address", "order_time",
    "estimated_delivery_time", "actual_delivery_time", "delivery_delay",
    "complaint_type", "had_complaint", "temperature", "precipitation",
    "wind_speed", "weather_condition", "delay_category"
]
final_df = final_df.select(*columns)

# Write to Iceberg table
if not spark.catalog.tableExists("my_catalog.silver_deliveries_enriched"):
    final_df.writeTo("my_catalog.silver_deliveries_enriched").createOrReplace()
else:
    final_df.writeTo("my_catalog.silver_deliveries_enriched").append()

