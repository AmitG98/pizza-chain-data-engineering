from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date
from pyspark.sql.types import StructType, IntegerType, StringType, DateType

# יצירת SparkSession
spark = SparkSession.builder \
    .appName("SilverDimStoreSCD2") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# נתונים לדוגמה (2 גרסאות לאותו store_id)
data = [
    (101, "North", "Alice", "2020-01-01", None, True),
    (101, "South", "Bob",   "2022-01-01", None, True),
    (102, "Center", "Eve",  "2021-05-01", None, True),
]

schema = StructType() \
    .add("store_id", IntegerType()) \
    .add("region", StringType()) \
    .add("admin", StringType()) \
    .add("effective_date", StringType()) \
    .add("end_date", StringType()) \
    .add("is_current", "boolean")

df = spark.createDataFrame(data, schema=schema) \
          .withColumn("effective_date", to_date("effective_date")) \
          .withColumn("end_date", to_date("end_date"))

# כתיבה כטבלת Iceberg עם תמיכה ב-SCD2
if not spark.catalog.tableExists("my_catalog.silver_dim_store"):
    df.writeTo("my_catalog.silver_dim_store").createOrReplace()
else:
    df.writeTo("my_catalog.silver_dim_store").append()