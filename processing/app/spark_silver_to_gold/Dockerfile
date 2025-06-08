FROM bitnami/spark:3.5.1

# שלב 1: הסרת קבצי Iceberg ישנים כדי למנוע קונפליקטים
RUN find /opt/bitnami/spark/jars/ -name "iceberg*" -delete

# שלב 2: העתקת JAR של Iceberg (התאימי שם גרסה ונתיב במידת הצורך)
COPY app/spark_kafka_to_bronze/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar /opt/bitnami/spark/jars/

# שלב 3: העתקת כל קבצי Spark של שלב GOLD
COPY app/spark_silver_to_gold/delivery_metrics_by_region.py /opt/bitnami/spark/app/
COPY app/spark_silver_to_gold/complaints_by_type.py /opt/bitnami/spark/app/
COPY app/spark_silver_to_gold/delivery_summary_daily.py /opt/bitnami/spark/app/
COPY app/spark_silver_to_gold/peak_hours_analysis.py /opt/bitnami/spark/app/
COPY app/spark_silver_to_gold/weather_impact_summary.py /opt/bitnami/spark/app/
COPY app/spark_silver_to_gold/store_performance.py /opt/bitnami/spark/app/

# תיקיית עבודה
WORKDIR /opt/bitnami/spark/app/

# פקודת ברירת מחדל (שניתנת לשינוי ב־docker-compose)
CMD ["spark-submit", "delivery_metrics_by_region.py"]
