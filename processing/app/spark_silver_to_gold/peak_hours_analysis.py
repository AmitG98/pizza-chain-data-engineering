from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, count, avg, when

# Session
spark = SparkSession.builder \
    .appName("GoldPeakHoursAnalysis") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# קלט
df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")

# הוספת עמודת שעה
df = df.withColumn("hour_of_day", hour("order_time"))

# חישובים לפי שעה
result = df.groupBy("hour_of_day").agg(
    count("*").alias("order_count"),
    avg("delivery_delay").alias("avg_delivery_delay"),
    (count(when(col("delay_category") == "late", True)) / count("*")).alias("late_orders_pct")
)

# כתיבה לטבלת GOLD
if not spark.catalog.tableExists("my_catalog.gold_peak_hours_analysis"):
    result.writeTo("my_catalog.gold_peak_hours_analysis").createOrReplace()
else:
    result.writeTo("my_catalog.gold_peak_hours_analysis").append()