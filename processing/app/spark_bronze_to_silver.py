from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Create a Spark session
spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .getOrCreate()

# Reading from the bronze layer
df_bronze = spark.readStream \
    .format("parquet") \
    .load("/opt/bitnami/spark-data/bronze/orders/")

# Example transformation:
df_silver = df_bronze \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .filter(col("amount") > 0)

# Writing for silver
query = df_silver.writeStream \
    .format("parquet") \
    .option("path", "/opt/bitnami/spark-data/silver/orders/") \
    .option("checkpointLocation", "/opt/bitnami/spark-data/checkpoints/bronze_to_silver/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
