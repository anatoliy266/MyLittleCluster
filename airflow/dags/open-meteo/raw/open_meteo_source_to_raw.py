import logging
import posixpath
import numpy as np
import time
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
)
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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


def split_bbox(bbox, step, max_points) -> List:
    """
    Делит BBox на прямоугольные области.
    Количество узлов сетки в каждой области < max_points.
    """

    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        raise ValueError(
            "BBox должен быть списком или кортежем из 4 элементов: "
            "[min_lon, min_lat, max_lon, max_lat]"
        )

    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox)
    except (ValueError, TypeError):
        raise TypeError("Все координаты BBox должны быть числами")

    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("Широта должна быть в диапазоне [-90, 90]")

    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError("Долгота должна быть в диапазоне [-180, 180]")

    if min_lon > max_lon or min_lat > max_lat:
        raise ValueError("Минимальные координаты не могут превышать максимальные")

    if step <= 0 or max_points < 2:
        raise ValueError("step должен быть > 0, max_points должен быть >= 2")

    lons = np.arange(min_lon, max_lon + step * 1e-9, step)
    lats = np.arange(min_lat, max_lat + step * 1e-9, step)

    lon_grid, lat_grid = np.meshgrid(lons, lats)

    if lon_grid.size > 1_000_000:
        raise OverflowError("Регион слишком большой. Сузьте BBox.")

    n_rows, n_cols = lon_grid.shape

    limit = max_points - 1

    rows = min(n_rows, int(np.sqrt(limit)))
    cols = min(n_cols, limit // rows)
    rows = min(n_rows, limit // cols)

    row_starts = np.arange(0, n_rows, rows)
    col_starts = np.arange(0, n_cols, cols)

    row_idx, col_idx = np.meshgrid(row_starts, col_starts, indexing="ij")

    row_idx = row_idx.ravel()
    col_idx = col_idx.ravel()

    row_end = np.minimum(row_idx + rows, n_rows) - 1
    col_end = np.minimum(col_idx + cols, n_cols) - 1

    chunks = [
        {
            "min_longitude": float(lon_grid[r0, c0]),
            "max_longitude": float(lon_grid[r1, c1]),
            "min_latitude": float(lat_grid[r0, c0]),
            "max_latitude": float(lat_grid[r1, c1]),
        }
        for r0, c0, r1, c1 in zip(row_idx, col_idx, row_end, col_end)
    ]

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
    stagging_path = context["params"]["staging_path"]
    raw_path = context["params"]["path"]

    bbox_list = split_bbox(bbox, 0.1, 1000)

    execution_date = context["logical_date"]
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    date_from = (execution_date - timedelta(days=1)).strftime("%Y-%0m-%0d")
    date_to = execution_date.strftime("%Y-%0m-%0d")

    hdfs_client = utils.get_hdfs_client("hdfs_conn")

    for area in bbox_list:
        stagging_hdfs_path = f"{stagging_path}/year={year}/month={month}/day={day}/open_meteo_{','.join(map(str, [area['min_latitude'], area['min_longitude'], area['max_latitude'], area['max_longitude']]))}_{','.join(map(str, models))}_{execution_date.strftime('%Y%m%d')}.json"

        raw_hdfs_path = f"{raw_path}/year={year}/month={month}/day={day}/open_meteo_{",".join(map(str, [area['min_latitude'], area['min_longitude'], area['max_latitude'], area['max_longitude']]))}_{",".join(map(str, models))}_{execution_date.strftime("%Y%m%d")}.json"

        if hdfs_client.status(raw_hdfs_path, strict=False) is not None:
            logger.info(
                f"Целевой файл уже существует: {raw_hdfs_path}, загрузка пропущена"
            )
            continue

        if hdfs_client.status(stagging_hdfs_path, strict=False) is not None:
            logger.info(
                f"Целевой файл уже существует: {stagging_hdfs_path}, загрузка пропущена"
            )
            continue

        logger.info(f"Загрузка данных для [{",".join(map(str, area))}]")
        openmeteo_url = f'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly={",".join(requested_params)}&models={",".join(map(str, models))}&wind_speed_unit=ms&bounding_box={",".join(map(str, [area['min_latitude'], area['min_longitude'], area['max_latitude'], area['max_longitude']]))}&start_date={date_from}&end_date={date_to}'
        try:
            bridge_url = "http://host.docker.internal:8765/v1/forecast"
            api_params = openmeteo_url.split("?", 1)[1]

            response = requests.get(
                f"{bridge_url}?{api_params}",
                timeout=90,
                stream=False,
            )
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()
            if "json" not in content_type:
                logger.error(f"Ожидался JSON, получен Content-Type: {content_type}")
                raise AirflowFailException(
                    f"Запрос для {','.join(map(str, area))} "
                    f"не вернул валидный JSON: Content-Type={content_type}"
                )

            response_time = time.monotonic()

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP ошибка: {http_err}. Ответ: {response.text[:200]}")
            raise AirflowFailException(
                "HTTP ошибка при запросе к посреднику Open-Meteo"
            ) from http_err

        except requests.exceptions.RequestException as req_err:
            logger.error(f"Сетевая ошибка посредника: {req_err}")
            raise

        logger.info(f"Сохранение ответа в datalake")

        try:
            with hdfs_client.write(stagging_hdfs_path, overwrite=True) as writer:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        writer.write(chunk)
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
    staging_path = context["params"]["staging_path"]
    raw_path = context["params"]["path"]

    hdfs_client = utils.get_hdfs_client("hdfs_conn")

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
    dag_id="nasa_openmeteo_extract",
    default_args=default_args,
    description="Извлечение данных open meteo API в raw слой DataLake",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,
    tags=["extract", "open_meteo", "hdfs", "bronze", "production"],
    on_failure_callback=utils.task_failure_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=2),
        callback=AsyncCallback(utils.log_deadline_miss),
    ),
    params={
        "bbox_list": [97.836607, 50.414121, 115.177044, 58.906169],  # Байкал
        "models": ["ecmwf_ifs"],
        "path": "/raw/open_meteo/",
        "staging_path": "/stagging/open_meteo/",
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
        task_id="extract_openmeteo_data_retry",
        python_callable=extract_openmeteo_data,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    validation_task_retry = SparkSubmitOperator(
        task_id="validate_payload_retry",
        application="/apps/spark_open_meteo_payload_validation.py",
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
        task_id="publish_openmeteo_data",
        python_callable=publish_openmeteo_data,
        trigger_rule=TriggerRule.ALL_DONE_MIN_ONE_SUCCESS,
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> extract_task >> validation_task

    validation_task >> publish_task
    validation_task >> extract_task_retry >> validation_task_retry >> publish_task

    publish_task >> end
