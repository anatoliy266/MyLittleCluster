# Schedule: @daily (или @hourly для проверки обновлений)
# Task 1: Download CSV from NASA FIRMS API for bounding box
# Task 2: Save to HDFS Bronze layer: /bronze/nasa_firms/year=YYYY/month=MM/day=DD/raw.csv
# Task 3: Send notification to Kafka topic: nasa_firms_raw ?

import logging
import posixpath
from datetime import datetime, timedelta
from typing import List

from utils.default_args import default_args
from utils import utils

import requests

from airflow.sdk import (
    DAG,
    DeadlineAlert,
    DeadlineReference,
    AsyncCallback,
    TriggerRule,
    Variable,
)
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


logger = logging.getLogger("airflow.task")

def extract_firms_data(**context):
    logger.info("Начало загрузки данных FIRMS")

    try:
        firms_token = Variable.get("nasa_firms_token")
    except Exception as e:
        logger.error(f"Не удалось получить значение переменной из Variables: {e}")
        raise AirflowFailException(
            "Не удалось получить значение переменной из Variables nasa_firms_token"
        ) from e

    bbox_list: List[float] = context["params"]["bbox_list"]
    if not isinstance(bbox_list, (list, tuple)) or len(bbox_list) != 4:
        raise AirflowFailException(f"некорректный bounding box координат: {bbox_list}")

    satellites: List[str] = context["params"]["satellites"]
    if not isinstance(satellites, (list, tuple)) or len(satellites) == 0:
        raise AirflowFailException(f"некорректный список спутников: {satellites}")

    region_bbox = ",".join(map(str, bbox_list))

    logical_date = context["logical_date"]
    execution_date = logical_date.date()

    target_date = execution_date.isoformat()
    target_date_compact = execution_date.strftime("%Y%m%d")

    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")

    hdfs_client = utils.get_hdfs_client("hdfs_conn")

    staging_base_path = context["params"]["staging_path"]

    staging_date_path = posixpath.join(
        staging_base_path,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )

    if hdfs_client.status(staging_date_path, strict=False) is not None:
        hdfs_client.delete(staging_date_path, recursive=True)
    hdfs_client.makedirs(staging_date_path)

    for sat in satellites:
        logger.info(f"Начало загрузки для спутника: {sat}, дата: {target_date}")

        url = (
            "https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
            f"{firms_token}/{sat}/{region_bbox}/1/{target_date}"
        )

        try:
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            logger.error(
                f"HTTP ошибка для {sat}: {http_err}. Ответ: {response.text[:200]}"
            )
            raise AirflowFailException(
                f"HTTP ошибка при запросе к NASA FIRMS для {sat}"
            ) from http_err
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Сетевая ошибка для {sat}: {req_err}")
            raise

        logger.info(f"Сохранение ответа в datalake")

        hdfs_path = posixpath.join(
            staging_date_path,
            f"firms_{sat}_{target_date_compact}.csv",
        )

        try:
            with hdfs_client.write(
                hdfs_path, overwrite=True
            ) as writer:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        writer.write(chunk)
        except Exception as e:
            logger.error(f"Ошибка при записи в HDFS для {sat}: {e}")
            raise AirflowFailException(
                f"Сбой на этапе загрузки в хранилище для {sat}: {e}"
            ) from e


def publish_firms_data(**context):
    satellites: List[str] = context["params"]["satellites"]
    if not isinstance(satellites, (list, tuple)) or len(satellites) == 0:
        raise AirflowFailException(f"некорректный список спутников: {satellites}")

    logical_date = context["logical_date"]
    execution_date = logical_date.date()

    target_date = execution_date.isoformat()
    target_date_compact = execution_date.strftime("%Y%m%d")

    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")

    hdfs_client = utils.get_hdfs_client("hdfs_conn")

    staging_base_path = context["params"]["staging_path"]
    staging_date_path = posixpath.join(
        staging_base_path,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )
    if hdfs_client.status(staging_date_path, strict=False) is None:
        raise AirflowFailException(
            f"Нет staging-каталога для публикации: {staging_date_path}"
        )

    raw_base_path = context["params"]["path"]
    raw_date_path = posixpath.join(
        raw_base_path,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )

    for sat in satellites:
        staged_file = posixpath.join(
            staging_date_path,
            f"firms_{sat}_{target_date_compact}.csv",
        )
        if hdfs_client.status(staged_file, strict=False) is None:
            raise AirflowFailException(f"Не найден файл спутника {sat}: {staged_file}")

    raw_parent = posixpath.dirname(raw_date_path)
    hdfs_client.makedirs(raw_parent)

    # Временный каталог для предыдущей версии RAW.
    run_id = context["run_id"].replace("/", "_").replace(":", "_")
    backup_path = posixpath.join(
        raw_parent,
        f"._backup_day={day}_{run_id}",
    )

    raw_exists = hdfs_client.status(raw_date_path, strict=False) is not None
    backup_exists = hdfs_client.status(backup_path, strict=False) is not None

    if backup_exists:
        raise AirflowFailException(
            f"Обнаружен резервный каталог предыдущей публикации: {backup_path}. "
            f"Автоматическая публикация остановлена. "
            f"Проверь состояние RAW, backup и staging перед повторным запуском."
        )

    if raw_exists:
        if not hdfs_client.rename(raw_date_path, backup_path):
            raise AirflowFailException(
                f"Не удалось перенести текущие RAW-данные в backup: "
                f"{raw_date_path} -> {backup_path}"
            )

    try:
        if not hdfs_client.rename(staging_date_path, raw_date_path):
            raise AirflowFailException(f"Не удалось перенести данные из staging в raw")
    except Exception:
        if (
            hdfs_client.status(backup_path, strict=False) is not None
            and hdfs_client.status(raw_date_path, strict=False) is None
        ):
            hdfs_client.rename(backup_path, raw_date_path)
        raise

    if hdfs_client.status(backup_path, strict=False) is not None:
        try:
            hdfs_client.delete(backup_path, recursive=True)
        except Exception as exc:
            logger.warning(
                f"Новый RAW опубликован, но резервный каталог не удалось удалить: "
                f"{backup_path}: {exc}"
            )

    logger.info(f"Опубликованы данные FIRMS: {raw_date_path}")


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
        "satellites": ["VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT"],
        "path": "/raw/nasa_firms/",
        "staging_path": "/stagging/nasa_firms/",
    },
) as dag:
    start = EmptyOperator(task_id="start_pipeline")

    extract_task = PythonOperator(
        task_id="extract_firms_data",
        python_callable=extract_firms_data,
    )

    validation_task = SparkSubmitOperator(
        task_id="validate_payload",
        application="/apps/spark_firms_payload_validation.py",
        conn_id="spark_default",
        application_args=[
            "--staging_path",
            "{{ params.staging_path }}",
            "--publish_path",
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

    extract_task_retry = PythonOperator(
        task_id="extract_firms_data_retry",
        python_callable=extract_firms_data,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    validation_task_retry = SparkSubmitOperator(
        task_id="validate_payload_retry",
        application="/apps/spark_firms_payload_validation.py",
        conn_id="spark_default",
        application_args=[
            "--staging_path",
            "{{ params.staging_path }}",
            "--publish_path",
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

    publish_task = PythonOperator(
        task_id="publish_firms_data",
        python_callable=publish_firms_data,
        trigger_rule=TriggerRule.ALL_DONE_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> extract_task >> validation_task
    
    validation_task >> publish_task
    validation_task >> extract_task_retry >> validation_task_retry >> publish_task

    publish_task >> end
