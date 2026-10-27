from utils.default_args import default_args
from utils import utils

from datetime import datetime, timedelta

from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, AsyncCallback
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="nasa_firms_extract",
    default_args=default_args,
    description="Извлечение данных NASA FIRMS в raw слой DataLake",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,
    tags=["extract", "nasa_firms", "hdfs", "bronze", "production"],
    on_failure_callback=utils.task_failure_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=2),
        callback=AsyncCallback(utils.log_deadline_miss),
    ),
    params={
        "bbox_list": [97.836607, 50.414121, 115.177044, 58.906169],  # Байкал
        "satellites": ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT"],
        "raw_path": "/raw/nasa_firms/",
        "stg_path": "/stg/nasa_firms/",
    },
) as dag:
    start = EmptyOperator(task_id="start_pipeline")

    transform_task=SparkSubmitOperator(
        task_id="transform_firms_data",
        application="/apps/spark_open_meteo_payload_validation.py",
                conn_id="spark_default",
                application_args=[
                    "--raw_path",
                    "{{ params.staging_path }}",
                    "--stg_path",
                    "{{ params.path }}",
                    "--execution_date",
                    "{{ ds }}",
                ],
                conf={
                    "spark.master": "yarn",
                    "spark.submit.deployMode": "client",
                },
                executor_memory="512m",
                driver_memory="512m",
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> transform_task >> end