from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Reading data from the SILVER layer
df_silver = spark.readStream \
    .format("parquet") \
    .load("/opt/bitnami/spark-data/silver/orders/")

# Processing – for example, calculating average orders per customer
df_gold = df_silver.groupBy("customer_name").agg(avg("amount").alias("avg_amount"))

# Writing to the GOLD layer
query = df_gold.writeStream \
    .format("parquet") \
    .option("path", "/opt/bitnami/spark-data/gold/orders/") \
    .option("checkpointLocation", "/opt/bitnami/spark-data/checkpoints/silver_to_gold/") \
    .outputMode("complete") \
    .start()

query.awaitTermination()
