from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_pi_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "spark", "yarn"],
) as dag:

    calculate_pi = SparkSubmitOperator(
        task_id="calculate_pi",
        application="/opt/spark/examples/jars/spark-examples_2.13-4.2.0-preview5.jar",
        java_class="org.apache.spark.examples.SparkPi",
        conf={
            "spark.master": "yarn",
            "spark.submit.deployMode": "client",
        },
        executor_memory="512m",
        driver_memory="512m",
        name="airflow-spark-pi",
        verbose=True,
    )

    count_hdfs = SparkSubmitOperator(
        task_id="count_hdfs_files",
        application="/opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar",
        java_class="org.apache.spark.examples.SparkDFSReadWriteTest",
        application_args=[
            "hdfs://namenode:9000/",
            "hdfs://namenode:9000/output/"
        ],
        conf={
            "spark.master": "yarn",
            "spark.submit.deployMode": "client",
        },
        executor_memory="512m",
        driver_memory="512m",
        name="airflow-hdfs-test",
    )

    calculate_pi >> count_hdfs
