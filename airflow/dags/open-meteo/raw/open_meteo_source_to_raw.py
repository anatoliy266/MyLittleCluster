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
    Param,
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
    models: List[str] = context["params"]["models"]
    staging_base_path = context["params"]["staging_path"]
    raw_base_path = context["params"]["path"]
    force_reload = context["params"]["force_reload"]

    if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        raise AirflowFailException(f"Некорректный параметр bbox_list: {bbox}")

    bbox_list = split_bbox(bbox, 0.1, 1000)

    execution_date = context["logical_date"]
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")
    target_date = execution_date.strftime("%Y%m%d")

    date_from = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    date_to = execution_date.strftime("%Y-%m-%d")

    staging_date_path = posixpath.join(
        staging_base_path,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )
    raw_date_path = posixpath.join(
        raw_base_path,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )

    hdfs_client = utils.get_hdfs_client("hdfs_conn")

    # При force_reload очищаем только STAGING текущей даты.
    # RAW остаётся нетронутым до публикации.
    if force_reload:
        if hdfs_client.status(staging_date_path, strict=False) is not None:
            hdfs_client.delete(staging_date_path, recursive=True)
            logger.info(
                f"STAGING очищен перед принудительной загрузкой: "
                f"{staging_date_path}"
            )

    hdfs_client.makedirs(staging_date_path)

    for area in bbox_list:
        coordinates = [
            area["min_latitude"],
            area["min_longitude"],
            area["max_latitude"],
            area["max_longitude"],
        ]
        area_str = ",".join(map(str, coordinates))
        models_str = ",".join(models)

        filename = f"open_meteo_{area_str}_{models_str}_{target_date}.json"

        staging_file = posixpath.join(staging_date_path, filename)
        raw_file = posixpath.join(raw_date_path, filename)

        # Обычный запуск: существующие данные повторно не загружаем.
        if not force_reload:
            if hdfs_client.status(raw_file, strict=False) is not None:
                # Если остался файл от незавершённого прошлого запуска,
                # не даём ему случайно попасть в повторную публикацию.
                if hdfs_client.status(staging_file, strict=False) is not None:
                    hdfs_client.delete(staging_file)
                    logger.info(
                        f"Удалён устаревший STAGING-файл, "
                        f"поскольку данные уже опубликованы: {staging_file}"
                    )

                logger.info(
                    f"Файл уже существует в RAW, загрузка пропущена: " f"{raw_file}"
                )
                continue

            if hdfs_client.status(staging_file, strict=False) is not None:
                logger.info(
                    f"Файл уже существует в STAGING, загрузка пропущена: "
                    f"{staging_file}"
                )
                continue

        logger.info(f"Загрузка данных для области [{area_str}]")

        api_params = {
            "latitude": 52.52,
            "longitude": 13.41,
            "hourly": ",".join(requested_params),
            "models": models_str,
            "wind_speed_unit": "ms",
            "bounding_box": area_str,
            "start_date": date_from,
            "end_date": date_to,
        }

        bridge_url = "http://host.docker.internal:8765/v1/forecast"

        try:
            with requests.get(
                bridge_url,
                params=api_params,
                timeout=90,
                stream=True,
            ) as response:
                response.raise_for_status()

                content_type = response.headers.get("Content-Type", "").lower()

                if "json" not in content_type:
                    raise AirflowFailException(
                        f"Ожидался JSON для области {area_str}, "
                        f"получен Content-Type={content_type}"
                    )

                response_time = time.monotonic()

                logger.info(f"Сохранение ответа в HDFS: {staging_file}")

                try:
                    with hdfs_client.write(
                        staging_file,
                        overwrite=True,
                    ) as writer:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                writer.write(chunk)

                except Exception as e:
                    logger.error(f"Ошибка записи в HDFS: {staging_file}: {e}")

                    # Не оставляем частично записанный файл,
                    # который следующий запуск мог бы принять за готовый.
                    if hdfs_client.status(staging_file, strict=False) is not None:
                        hdfs_client.delete(staging_file)

                    raise AirflowFailException(f"Ошибка загрузки в STAGING: {e}") from e

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP-ошибка Open-Meteo: {e}. " f"Ответ: {response.text[:200]}"
            )
            raise AirflowFailException(
                f"HTTP-ошибка при запросе области {area_str}"
            ) from e

        except requests.exceptions.RequestException as e:
            logger.error(f"Сетевая ошибка Open-Meteo: {e}")
            raise

        wait_for_rate_limit(response_time, 120)

    logger.info(f"Извлечение завершено. STAGING: {staging_date_path}")

def publish_openmeteo_data(**context):
    execution_date = context["logical_date"]
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")

    staging_base_path = context["params"]["staging_path"]
    raw_base_path = context["params"]["path"]

    staging_date_path = posixpath.join(
        staging_base_path,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )
    raw_date_path = posixpath.join(
        raw_base_path,
        f"year={year}",
        f"month={month}",
        f"day={day}",
    )

    hdfs_client = utils.get_hdfs_client("hdfs_conn")

    if hdfs_client.status(staging_date_path, strict=False) is None:
        logger.info(
            f"STAGING-каталог отсутствует, публикация пропущена: "
            f"{staging_date_path}"
        )
        return

    files_to_publish = []

    for root, dirs, files in hdfs_client.walk(staging_date_path):
        for filename in files:
            staging_file = posixpath.join(root, filename)
            relative_path = posixpath.relpath(staging_file, staging_date_path)
            raw_file = posixpath.join(raw_date_path, relative_path)

            files_to_publish.append((staging_file, raw_file))

    if not files_to_publish:
        logger.info(f"В STAGING нет файлов для публикации: {staging_date_path}")
        return

    for staging_file, raw_file in files_to_publish:
        logger.info(f"Публикация: {staging_file} -> {raw_file}")

        hdfs_client.makedirs(posixpath.dirname(raw_file))

        backup_file = f"{raw_file}.backup"

        if hdfs_client.status(backup_file, strict=False) is not None:
            raise AirflowFailException(
                f"Найден backup предыдущей публикации: {backup_file}. "
                "Требуется проверить состояние HDFS перед повторным запуском."
            )

        raw_exists = hdfs_client.status(raw_file, strict=False) is not None

        if raw_exists:
            if not hdfs_client.rename(raw_file, backup_file):
                raise AirflowFailException(
                    f"Не удалось сохранить предыдущий RAW-файл: {raw_file}"
                )

        try:
            if not hdfs_client.rename(staging_file, raw_file):
                raise AirflowFailException(
                    f"Не удалось опубликовать файл: {staging_file}"
                )

        except Exception:
            backup_exists = hdfs_client.status(backup_file, strict=False) is not None
            raw_now_exists = hdfs_client.status(raw_file, strict=False) is not None

            if backup_exists and not raw_now_exists:
                if not hdfs_client.rename(backup_file, raw_file):
                    logger.error(
                        f"Не удалось восстановить предыдущий RAW-файл: "
                        f"{backup_file}"
                    )

            raise

        if raw_exists:
            if hdfs_client.status(backup_file, strict=False) is not None:
                try:
                    hdfs_client.delete(backup_file)
                except Exception as e:
                    logger.warning(
                        f"Новый файл опубликован, но backup не удалён: "
                        f"{backup_file}: {e}"
                    )

    logger.info(f"Публикация завершена: {raw_date_path}")


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
        "force_reload": Param(
            False,
            type="boolean",
            description=(
                "Принудительно очистить STAGING за дату запуска "
                "и повторно загрузить все области Open-Meteo"
            ),
        ),
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
