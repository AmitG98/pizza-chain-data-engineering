from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# Session
spark = SparkSession.builder \
    .appName("GoldStorePerformance") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# קריאה מהטבלאות
deliveries_df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")
dim_store_df = spark.read.format("iceberg").load("my_catalog.silver_dim_store")

# הצטרפות ל־region לפי store_id
deliveries_with_region = deliveries_df.join(
    dim_store_df.select("store_id", "region"), on="store_id", how="left"
)

# סטטיסטיקות לפי store_id + region
delivery_stats = deliveries_with_region.groupBy("store_id", "region").agg(
    avg("delivery_delay").alias("avg_delay"),
    count("*").alias("orders_total")
)

# חיבור תלונות להזמנות → נזהה את החנות והאזור לפי order_id
complaints_with_info = complaints_df.join(
    deliveries_with_region.select("order_id", "store_id", "region"),
    on="order_id", how="inner"
)

complaints_stats = complaints_with_info.groupBy("store_id", "region").agg(
    count("*").alias("complaints")
)

# שילוב כל הנתונים
final_df = delivery_stats.join(complaints_stats, on=["store_id", "region"], how="left").fillna(0)

# חישוב complaints_rate
final_df = final_df.withColumn("complaints_rate", col("complaints") / col("orders_total"))

# בחירת העמודות הסופיות
final_df = final_df.select(
    "store_id",
    "region",
    "avg_delay",
    "orders_total",
    "complaints_rate"
)

# כתיבה לטבלת GOLD
if not spark.catalog.tableExists("my_catalog.gold_store_performance"):
    final_df.writeTo("my_catalog.gold_store_performance").createOrReplace()
else:
    final_df.writeTo("my_catalog.gold_store_performance").append()