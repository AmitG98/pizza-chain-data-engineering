from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_format, current_timestamp
from pyspark.sql.types import DateType
from datetime import date

# Creation of SparkSession
spark = SparkSession.builder \
    .appName("GenerateTimeDimension") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Date range - from 2020 to ten years ahead
start = "2020-01-01"
end = "2029-12-31"

# Generating a list of dates
df = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")

df = df.withColumn("day_of_week", date_format("date", "EEEE")) \
       .withColumn("is_weekend", expr("day_of_week IN ('Saturday', 'Sunday')"))

# Fixed holidays by date only 
holidays = ["01-01", "12-25"]
df = df.withColumn("is_holiday", date_format("date", "MM-dd").isin(holidays))

df = df.withColumn("is_short_friday", (col("day_of_week") == "Friday"))

df = df.withColumn("ingestion_time", current_timestamp())

if not spark.catalog.tableExists("my_catalog.silver_dim_time"):
    df.writeTo("my_catalog.silver_dim_time").createOrReplace()
else:
    df.writeTo("my_catalog.silver_dim_time").append()