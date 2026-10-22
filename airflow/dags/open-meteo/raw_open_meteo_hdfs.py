import logging
import csv
import io
import json
import posixpath
import numpy as np
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

from default_args import default_args
from handlers.handlers import task_failure_alert, log_deadline_miss

import requests
from hdfs import InsecureClient

from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, Variable, AsyncCallback
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

# requested_params = ["temperature_2m,weather_code,cloud_cover,rain,showers,precipitation,precipitation_probability,apparent_temperature,wind_speed_10m,wind_direction_10m,wind_gusts_10m,soil_temperature_0cm,soil_moisture_0_to_1cm,soil_moisture_1_to_3cm,soil_moisture_3_to_9cm,relative_humidity_2m,wind_speed_80m,wind_direction_80m,dew_point_2m,vapour_pressure_deficit,evapotranspiration,et0_fao_evapotranspiration,cape,boundary_layer_height"]
requested_params = [
    "temperature_2m",
    "weather_code",
    "cloud_cover",
    "rain",
    "showers",
    "precipitation",
    "precipitation_probability",
    "apparent_temperature",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "soil_temperature_0cm",
    "soil_moisture_0_to_1cm",
    "soil_moisture_1_to_3cm",
    "soil_moisture_3_to_9cm",
    "relative_humidity_2m",
    "wind_speed_80m",
    "wind_direction_80m",
    "dew_point_2m",
    "vapour_pressure_deficit",
    "evapotranspiration",
    "et0_fao_evapotranspiration",
    "cape",
    "boundary_layer_height",
]

logger = logging.getLogger("airflow.task")


def get_hdfs_client() -> InsecureClient:
    """Безопасное получение клиента HDFS из Airflow Connections."""
    try:
        hook = WebHDFSHook(webhdfs_conn_id="hdfs_conn")
        return hook.get_conn()
    except Exception as e:
        logger.error(f"Failed to initialize HDFS client: {e}")
        raise AirflowFailException(
            "Не удалось подключиться к HDFS. Проверьте Connection 'hdfs_conn'."
        ) from e


def split_bbox(bbox, step, max_points) -> List:
    """
    Алгоритмически делит один большой BBox на сетку из мелких BBox'ов.

    :param bbox: Список координат [min_lon, min_lat, max_lon, max_lat]
    :param step: размер локации модели в градусах
    :param max_points: ограничение апи
    :return: Список мини-BBox'ов в формате Open-Meteo
    """

    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        raise ValueError(
            "BBox должен быть списком или кортежем из 4 элементов: [min_lon, min_lat, max_lon, max_lat]"
        )

    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox)
    except (ValueError, TypeError):
        raise TypeError("Все координаты в BBox должны быть числами (int или float)")

    # 2. ПРОВЕРКА: Реальные географические границы Земли (WGS84)
    if not (-90.0 <= min_lat <= 90.0) or not (-90.0 <= max_lat <= 90.0):
        raise ValueError(
            f"Критическая ошибка широты (Latitude)! Она должна быть в пределах от -90 до 90 градусов. У вас: {min_lat}, {max_lat}"
        )

    if not (-180.0 <= min_lon <= 180.0) or not (-180.0 <= max_lon <= 180.0):
        raise ValueError(
            f"Критическая ошибка долготы (Longitude)! Она должна быть в пределах от -180 до 180 градусов. У вас: {min_lon}, {max_lon}"
        )

    # 3. ПРОВЕРКА: Логика минимума и максимума
    if min_lon > max_lon:
        raise ValueError(
            f"Начальная долгота (min_lon: {min_lon}) не может быть больше конечной (max_lon: {max_lon})"
        )
    if min_lat > max_lat:
        raise ValueError(
            f"Начальная широта (min_lat: {min_lat}) не может быть больше конечной (max_lat: {max_lat})"
        )

    lons = np.arange(min_lon, max_lon + step, step)
    lats = np.arange(min_lat, max_lat + step, step)
    all_points = np.array(np.meshgrid(lons, lats)).T.reshape(-1, 2)

    if len(all_points) > 1_000_000:
        raise OverflowError(
            f"Регион слишком огромен ({all_points} точек). Сузьте BBox."
        )

    chunks = []
    for i in range(0, len(all_points), max_points):
        points = all_points[i : i + max_points]
        chunks.append(
            {
                "min_longitude": float(np.min(points[:, 0])),
                "max_longitude": float(np.max(points[:, 0])),
                "min_latitude": float(np.min(points[:, 1])),
                "max_latitude": float(np.max(points[:, 1])),
            }
        )

    return chunks


def wait_for_rate_limit(response_time: float, waiting_seconds: int):
    leftover_time = waiting_seconds - (time.monotonic() - response_time)
    if leftover_time > 0:
        logger.info(f"Ожидание охлаждения API, {leftover_time} секунд")
        time.sleep(leftover_time)


def extract_openmeteo_data(**context):
    bbox: List[float] = context["params"]["bbox_list"]
    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        logger.error(
            f"BBox должен быть списком или кортежем из 4 элементов: [min_lon, min_lat, max_lon, max_lat]: {bbox}"
        )
        raise AirflowFailException(f"неверный параметр: bbox_list: {bbox}")

    # ecmwf_ifs
    models: List[str] = context["params"]["models"]
    stagging_path = context["params"]["stagging_path"]

    bbox_list = split_bbox(bbox, 0.1, 1000)

    execution_date = context["logical_date"]
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    date_from = (execution_date - timedelta(days=1)).strftime("%Y-%0m-%0d")
    date_to = execution_date.strftime("%Y-%0m-%0d")

    hdfs_client = get_hdfs_client()

    for area in bbox_list:
        logger.info(f"Загрузка данных для [{",".join(map(str, area))}]")
        openmeteo_url = f'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly={",".join(requested_params)}&models={",".join(map(str, models))}&wind_speed_unit=ms&bounding_box={",".join(map(str, area))}&start_date={date_from}&end_date={date_to}'
        try:
            response = requests.get(openmeteo_url, timeout=30, stream=True)
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            if "json" not in content_type:
                logger.error(f"Ожидался JSON, получен Content-Type: {content_type}")
                raise AirflowFailException(
                    f"Запрос для {",".join(map(str, area))} не вернул валидный json: получен Content-Type:{content_type}"
                )
            response_time = time.monotonic()
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP ошибка: {http_err}. Ответ: {response.text[:200]}")
            raise AirflowFailException(
                f"HTTP ошибка при запросе к Open Meteo "
            ) from http_err
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Сетевая ошибка: {req_err}")
            raise

        logger.info(f"Сохранение ответа в datalake")
        stagging_hdfs_path = f"{stagging_path}/year={year}/month={month}/day={day}/open_meteo_{",".join(bbox)}_{",".join(models)}_{execution_date.strftime("%Y%0m%0d")}.json"

        try:
            with hdfs_client.write(
                stagging_hdfs_path, overwrite=True, encoding="utf-8"
            ) as writer:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        writer.write(chunk.decode("utf-8"))
        except Exception as e:
            logger.error(f"Ошибка при записи в HDFS или Kafka: {e}")
            raise AirflowFailException(
                f"Сбой на этапе загрузки в хранилище: {e}"
            ) from e

        wait_for_rate_limit(response_time, 60)


def publish_openmeteo_data(**context):
    logger.info(
        f"Публикация данных {",".join(map(str, context['params']['bbox_list']))} за {context['logical_date']} в raw слой"
    )
    staging_path = context["params"]["stagging_path"]
    raw_path = context["params"]["path"]

    hdfs_client = get_hdfs_client()

    files_to_publish = []

    for root, dirs, files in hdfs_client.walk(staging_path):
        for file in files:
            staging_file_path = posixpath.join(root, file)
            relative_path = posixpath.relpath(staging_file_path, staging_path)
            raw_file_path = posixpath.join(raw_path, relative_path)

            files_to_publish.append((staging_file_path, raw_file_path))

    if not files_to_publish:
        raise AirflowFailException(f"В staging-каталоге нет файлов: {staging_path}")

    for staging_file_path, raw_file_path in files_to_publish:
        logger.info(f"Перенос файла: {staging_file_path} -> {raw_file_path}")

        parent_dir = posixpath.dirname(raw_file_path)
        hdfs_client.makedirs(parent_dir)

        if hdfs_client.status(raw_file_path, strict=False) is not None:
            raise AirflowFailException(f"Целевой файл уже существует: {raw_file_path}")

        hdfs_client.rename(staging_file_path, raw_file_path)


with DAG(
    dag_id="nasa_firms_extract",
    default_args=default_args,
    description="Извлечение данных NASA FIRMS в raw слой DataLake",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,
    tags=["extract", "nasa_firms", "hdfs", "bronze", "production"],
    on_failure_callback=task_failure_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=2),
        callback=AsyncCallback(log_deadline_miss),
    ),
    params={
        "bbox_list": [97.836607, 50.414121, 115.177044, 58.906169],  # Байкал
        "models": ["ecmwf_ifs"],
        "path": "/raw/open_meteo/",
        "stagging_path": "/stagging/open_meteo/",
    },
) as dag:
    start = EmptyOperator(task_id="start_pipeline")

    extract_task = PythonOperator(
        task_id="extract_openmeteo_data",
        python_callable=extract_openmeteo_data,
    )

    validation_task = SparkSubmitOperator(
        task_id="validate_payload",
        application="/apps/spark_open_meteo_payload_validation.py",
        conn_id="spark_default",
        conf={
            "spark.master": "yarn",
            "spark.submit.deployMode": "client",
        },
        executor_memory="512m",
        driver_memory="512m",
    )

    publish_task = PythonOperator(
        task_id="publish_openmeteo_data",
        python_callable=publish_openmeteo_data,
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> extract_task >> validation_task >> publish_task >> end
