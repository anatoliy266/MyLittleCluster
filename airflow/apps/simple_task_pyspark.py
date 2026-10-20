from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("airflow-hdfs-test") \
    .getOrCreate()

# Пишем данные в HDFS
df = spark.range(100)
df.write.mode("overwrite").parquet("hdfs://namenode:9000/test/numbers.parquet")
print("✅ Данные записаны в HDFS")

# Читаем обратно
df_read = spark.read.parquet("hdfs://namenode:9000/test/numbers.parquet")
count = df_read.count()
print(f"✅ Прочитано строк из HDFS: {count}")

spark.stop()