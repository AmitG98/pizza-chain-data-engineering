from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count, when, first

# Session
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

# קלט
deliveries_df = spark.read.format("iceberg").load("my_catalog.silver_deliveries_enriched")
weather_df = spark.read.format("iceberg").load("my_catalog.silver_weather_enriched")

# הוספת עמודת תאריך
deliveries_df = deliveries_df.withColumn("date", to_date("order_time"))

# סיכום לפי יום
daily_summary = deliveries_df.groupBy("date").agg(
    count("*").alias("total_orders"),
    count(when(col("delay_category") == "late", True)).alias("late_orders")
)

# נניח שב-weather_enriched יש עמודת timestamp → נוציא את התאריך ונחבר לפי date
weather_df = weather_df.withColumn("date", to_date("order_time"))

# ניקח את weather_summary הראשון באותו יום
weather_summary_df = weather_df.groupBy("date").agg(
    first("weather_condition", ignorenulls=True).alias("weather_summary")
)

# חיבור שני הנתונים
final_df = daily_summary.join(weather_summary_df, on="date", how="left")

# כתיבה לטבלת GOLD
if not spark.catalog.tableExists("my_catalog.gold_delivery_summary_daily"):
    final_df.writeTo("my_catalog.gold_delivery_summary_daily").createOrReplace()
else:
    final_df.writeTo("my_catalog.gold_delivery_summary_daily").append()
