# Schedule: @daily (или @hourly для проверки обновлений)
# Task 1: Download CSV from NASA FIRMS API for bounding box
# Task 2: Save to HDFS Bronze layer: /bronze/nasa_firms/year=YYYY/month=MM/day=DD/raw.csv
# Task 3: Send notification to Kafka topic: nasa_firms_raw ?

import logging
import csv
import io
from datetime import datetime, timedelta
from typing import List, Dict, Any

import requests
from hdfs import InsecureClient

from default_args import default_args

from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, Variable, AsyncCallback
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

EXPECTED_FIRMS_HEADERS = {
    "latitude", "longitude", "bright_ti4", "scan", "track", "acq_date", "acq_time", "satellite", "instrument", "confidence", "version", "bright_ti5", "frp", "daynight"
    # "latitude", "longitude", "brightness", "scan", "track", "acq_date", "acq_time", "satellite", "instrument", "confidence", "version", "bright_t31", "frp", "daynight"
}

logger = logging.getLogger("airflow.task") 

def get_hdfs_client() -> InsecureClient:
    """Безопасное получение клиента HDFS из Airflow Connections."""
    try:
        hook = WebHDFSHook(webhdfs_conn_id='hdfs_conn')
        return hook.get_conn()
    except Exception as e:
        logger.error(f"Failed to initialize HDFS client: {e}")
        raise AirflowFailException("Не удалось подключиться к HDFS. Проверьте Connection 'hdfs_conn'.") from e

def validate_csv_payload(text_content: str) -> bool:
    """
    Проверяет, что ответ API является валидным CSV FIRMS, а не HTML-ошибкой.
    """
    try:
        reader = csv.reader(io.StringIO(text_content))
        headers = set(next(reader))
        
        if not EXPECTED_FIRMS_HEADERS.issubset(headers):
            logger.warning(f"Неожиданные заголовки в ответе. Получено: {headers}")
            return False
        return True
    except StopIteration:
        logger.error("Пустой ответ от API")
        return False
    except Exception as e:
        logger.error(f"Ошибка парсинга CSV для валидации: {e}")
        return False


def extract_firms_data(**context):
    print(">>> extract_firms_data START")

    try:
        firms_token = Variable.get("nasa_firms_token")
    except Exception as e:
        logger.error(f"Не удалось получить значение переменной из Variables: {e}")
        raise AirflowFailException("Не удалось получить значение переменной из Variables nasa_firms_token") from e


    bbox_list: List[float] = context['params']['bbox_list']
    satellites: List[str] = context['params']['satellites']
    execution_date = context['logical_date']
    
    region_bbox = ",".join(map(str, bbox_list))

    execution_date = context['logical_date']
    target_date = execution_date.strftime("%Y-%0m-%0d")
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")

    hdfs_client = get_hdfs_client()

    for sat in satellites:
        logger.info(f"Начало загрузки для спутника: {sat}, дата: {target_date}")
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{firms_token}/{sat}/{region_bbox}/1/{target_date}"
        try:
            response = requests.get(url, timeout=30)
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP ошибка для {sat}: {http_err}. Ответ: {response.text[:200]}")
            raise AirflowFailException(f"HTTP ошибка при запросе к NASA FIRMS для {sat}") from http_err
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Сетевая ошибка для {sat}: {req_err}")
            raise


        logger.info(f"Валидация ответа от firms api")
        sample_text = response.content[:1024].decode('utf-8', errors='ignore')
        if not validate_csv_payload(sample_text):
            logger.error(f"Невалидный формат данных для {sat}. Начало ответа: {sample_text}")
            raise AirflowFailException(f"Получены невалидные данные (не CSV) для {sat}")


        logger.info(f"Сохранение ответа в datalake")
        hdfs_path = f"/raw/nasa_firms/year={year}/month={month}/day={day}/firms_{sat}_{target_date.replace('-', '')}.csv"
        try:
            with hdfs_client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        writer.write(chunk.decode('utf-8'))
        except Exception as e:
            logger.error(f"Ошибка при записи в HDFS или Kafka для {sat}: {e}")
            raise AirflowFailException(f"Сбой на этапе загрузки в хранилище для {sat}: {e}") from e

async def log_deadline_miss(**kwargs):
    """Асинхронный callback для DeadlineAlert: логирует пропуск дедлайна."""
    context = kwargs.get("context", {})
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", "unknown")
    logger.critical("SLA MISS в DAG %s! Дедлайн пропущен.", dag_id)

def task_failure_alert(context):
    """Коллбек для сфейлившейся задачи."""
    logger.critical(f"Task {context.get('task_instance').task_id} failed! {context.get('exception')}")
            

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
        "bbox_list": [97.836607, 50.414121, 115.177044, 58.906169], # Байкал
        "satellites": ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT"]
        
    }
) as dag:
    start = EmptyOperator(task_id="start_pipeline")

    extract_task = PythonOperator(
        task_id="extract_firms_data",
        python_callable=extract_firms_data,
    )

    end = EmptyOperator(task_id="end_pipeline")

    start >> extract_task >> end


    