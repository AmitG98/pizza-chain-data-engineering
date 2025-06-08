from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when

# יצירת session
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

# טבלאות
deliveries_df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")
dim_store_df = spark.read.format("iceberg").load("my_catalog.silver_dim_store")  # הכרחי!

# הוספת region להזמנות לפי store_id
deliveries_with_region = deliveries_df.join(
    dim_store_df.select("store_id", "region"), on="store_id", how="left"
)

# חישוב נתוני משלוחים לפי region
delivery_stats = deliveries_with_region.groupBy("region").agg(
    avg("delivery_delay").alias("avg_delivery_time"),
    (count(when(col("delay_category") == "late", True)) / count("*")).alias("late_deliveries_pct"),
    count("*").alias("total_orders")
)

# חיבור תלונות להזמנות ול־region
complaints_with_region = complaints_df.join(
    deliveries_with_region.select("order_id", "region"), on="order_id", how="inner"
)

complaints_stats = complaints_with_region.groupBy("region").agg(
    count("*").alias("complaints")
)

# שילוב הנתונים
final_df = delivery_stats.join(complaints_stats, on="region", how="left").fillna(0)

# אחוז תלונות מתוך כלל ההזמנות
final_df = final_df.withColumn("complaints_pct", col("complaints") / col("total_orders"))

# עמודות סופיות
final_df = final_df.select(
    "region",
    "avg_delivery_time",
    "late_deliveries_pct",
    "complaints_pct"
)

# כתיבה ל־GOLD
if not spark.catalog.tableExists("my_catalog.gold_delivery_metrics_by_region"):
    final_df.writeTo("my_catalog.gold_delivery_metrics_by_region").createOrReplace()
else:
    final_df.writeTo("my_catalog.gold_delivery_metrics_by_region").append()

