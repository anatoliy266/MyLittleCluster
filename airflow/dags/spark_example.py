from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

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

    create_file = BashOperator(
        task_id="create_test_file",
        bash_command="echo 'test data for spark' > /tmp/test.txt",
        dag=dag,
    )

    count_hdfs = SparkSubmitOperator(
        task_id="count_hdfs_files",
        application="/opt/spark/examples/jars/spark-examples_2.13-4.2.0-preview5.jar",
        java_class="org.apache.spark.examples.DFSReadWriteTest",
        application_args=[
            "/tmp/test.txt",
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

    calculate_pi >> create_file >> count_hdfs
