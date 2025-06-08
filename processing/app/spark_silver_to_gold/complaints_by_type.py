from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Session
spark = SparkSession.builder \
    .appName("GoldComplaintsByType") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# קלט
complaints_df = spark.read.format("iceberg").load("my_catalog.silver_complaints_clean")

# סך כל התלונות
total_complaints = complaints_df.count()

# קיבוץ לפי סוג תלונה
complaints_by_type = complaints_df.groupBy("complaint_type").agg(
    count("*").alias("count")
)

# חישוב אחוז מתוך כלל התלונות
complaints_by_type = complaints_by_type.withColumn(
    "percent_of_orders", col("count") / total_complaints
)

# כתיבה לטבלת GOLD
if not spark.catalog.tableExists("my_catalog.gold_complaints_by_type"):
    complaints_by_type.writeTo("my_catalog.gold_complaints_by_type").createOrReplace()
else:
    complaints_by_type.writeTo("my_catalog.gold_complaints_by_type").append()