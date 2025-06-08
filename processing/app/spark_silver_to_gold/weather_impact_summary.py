from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when

# Session
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

# קלט
deliveries_df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")
weather_df = spark.read.format("iceberg").load("my_catalog.silver_weather_enriched")
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")

# פתרון לבעיה של עמודה כפולה: נשנה שמות לפני join
weather_df = weather_df.selectExpr("order_id as w_order_id", "weather_condition as w_condition")

# איחוד deliveries עם weather לפי order_id
combined_df = deliveries_df.join(weather_df, deliveries_df.order_id == weather_df.w_order_id, "inner")

# חישובי עיכוב ואיחורים לפי תנאי מזג אוויר
delivery_stats = combined_df.groupBy("w_condition").agg(
    avg("delivery_delay").alias("avg_delivery_delay"),
    (count(when(col("delay_category") == "late", True)) / count("*")).alias("late_deliveries_pct"),
    count("*").alias("total_orders")
)

# חיבור תלונות למזג אוויר לפי order_id
complaints_with_weather = complaints_df.join(weather_df, complaints_df.order_id == weather_df.w_order_id, "inner")

complaints_stats = complaints_with_weather.groupBy("w_condition").agg(
    count("*").alias("complaints")
)

# שילוב הנתונים
final_df = delivery_stats.join(complaints_stats, on="w_condition", how="left").fillna(0)

# חישוב אחוז תלונות
final_df = final_df.withColumn("complaints_pct", col("complaints") / col("total_orders"))

# בחירת עמודות סופיות ושינוי שם עמודה בחזרה
final_df = final_df.select(
    col("w_condition").alias("weather_condition"),
    "avg_delivery_delay",
    "late_deliveries_pct",
    "complaints_pct"
)

# כתיבה לטבלת GOLD
if not spark.catalog.tableExists("my_catalog.gold_weather_impact_summary"):
    final_df.writeTo("my_catalog.gold_weather_impact_summary").createOrReplace()
else:
    final_df.writeTo("my_catalog.gold_weather_impact_summary").append()