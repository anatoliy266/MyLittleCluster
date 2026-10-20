# wildfires-russia

import json
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_date, year, month, dayofmonth
from pyspark.sql.types import StructType

logger = logging.getLogger("airflow.task")

def main():
    spark = SparkSession.builder\
        .appName("Nightly_Firms_Ingestion") \
        .master("yarn") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview1") \
        .getOrCreate()
    
    
    
    schema_path = "/apps/spark_HDFS_data_processing/firms_schema.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    
    nasa_schema = StructType.fromJson(schema_dict)
    logger.info("--- схема загружена ---")

    firms_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker-1:9092,broker-2:9092,broker-3:9092") \
        .option("subscribe", "wildfires-russia") \
        .option("startingOffsets", "earliest") \
        .load()
    
    df_parsed = firms_df.select(
        from_json(col("value").cast("string"), nasa_schema).alias("data")
    ).select("data.*")

    query = df_final.writeStream \
        .format("parquet") \
        .partitionBy("year", "month", "day") \
        .option("path", "/lake/bronze/nasa_firms") \
        .option("checkpointLocation", "/lake/checkpoints/nasa_firms") \
        .trigger(availableNow=True) \
        .start()
    
    query.awaitTermination()
    logger.info("--- Данные успешно очищены и сохранены в Parquet на HDFS ---")