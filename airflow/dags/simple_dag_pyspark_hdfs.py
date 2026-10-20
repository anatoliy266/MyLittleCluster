from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 5, 22),
}

with DAG(
    dag_id="pyspark_hdfs_test",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Простой тест PySpark + HDFS",
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="pyspark_hdfs_test",
        application="/apps/simple_task_pyspark.py",
        conn_id="spark_default",
        conf={
            "spark.master": "yarn",
            "spark.submit.deployMode": "client",
        },
        executor_memory="512m",
        driver_memory="512m",
    )

    spark_task