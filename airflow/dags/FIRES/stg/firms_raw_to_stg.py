# -*- coding: utf-8 -*-
"""
DAG: FIRMS RAW -> STG + QUARANTINE

Оркестрация запуска spark_firms_raw_to_stg.py.
Вся бизнес-логика, валидация, дедупликация, публикация и фиксация
реализованы в Spark-приложении. DAG только готовит контекст, делает preflight,
запускает SparkSubmit, верифицирует опубликованный бандл и финиширует.
"""

import json
import logging
import posixpath
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from utils.default_args import default_args
from utils import utils

logger = logging.getLogger("airflow.task")

def read_hdfs_json(client, path):
    with client.read(path, encoding="utf-8") as reader:
        return json.load(reader)

def verify_run_bundle(**context):
    """
    Верифицирует, что Spark успешно опубликовал бандл для этого run_id.
    Проверяет metadata/manifest.json в published_run. Не трогает CURRENT.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    ds = context["ds"]

    stg_root = context["params"]["stg_path"]
    hdfs_run_id = run_id.replace(":", "-")

    run_path = posixpath.join(stg_root, "runs", hdfs_run_id)

    client = utils.get_hdfs_client(conn_id="hdfs_conn")

    manifest_path = posixpath.join(run_path, "metadata", "manifest.json")
    metrics_path = posixpath.join(run_path, "metadata", "metrics.json")
    stg_bundle_path = posixpath.join(run_path, "stg")
    quarantine_bundle_path = posixpath.join(run_path, "quarantine")

    logger.info("Verifying published bundle at %s", run_path)

    for p in [manifest_path, metrics_path, stg_bundle_path, quarantine_bundle_path]:
        try:
            status = client.status(p, strict=False)
        except Exception as exc:
            raise AirflowFailException(
                f"Failed to check HDFS path: {p}. Details: {exc}"
            )
        if status is None:
            raise AirflowFailException(
                f"Missing required artifact: {p}"
            )

    # Читаем манифест
    try:
        with client.read(manifest_path, encoding="utf-8") as reader:
                manifest = json.load(reader)
        
    except Exception as exc:
        raise AirflowFailException(
            f"Failed to read/parse manifest.json: {manifest_path}. Details: {exc}"
        )

    # Базовые проверки
    if manifest.get("run_id") != run_id:
        raise AirflowFailException(
            f"Manifest run_id mismatch. Expected {run_id}, got {manifest.get('run_id')}"
        )

    if manifest.get("execution_date") != ds:
        raise AirflowFailException(
            f"Manifest execution_date mismatch. Expected {ds}, got {manifest.get('execution_date')}"
        )

    if manifest.get("status") != "READY_FOR_PUBLICATION":
        raise AirflowFailException(
            f"Manifest status not READY_FOR_PUBLICATION: {manifest.get('status')}"
        )

    if manifest.get("is_committed") is not False:
        raise AirflowFailException(
            f"Manifest is_committed must be false before commit step: {manifest.get('is_committed')}"
        )

    # Метрики
    try:
        with client.read(metrics_path, encoding="utf-8") as reader:
            metrics = json.load(reader)
        
    except Exception as exc:
        raise AirflowFailException(
            f"Failed to read/parse metrics.json: {metrics_path}. Details: {exc}"
        )

    summary_out = metrics.get("output_count")
    msum_out = manifest.get("metrics_summary", {}).get("output_count")
    if msum_out is not None and summary_out is not None and msum_out != summary_out:
        raise AirflowFailException(
            f"metrics_summary.output_count != metrics.json.output_count: {msum_out} vs {summary_out}"
        )

    logger.info(
        "Bundle verified OK. run_id=%s, input=%s, invalid=%s, duplicate=%s, output=%s",
        run_id,
        metrics.get("input_count"),
        metrics.get("invalid_count"),
        metrics.get("duplicate_count"),
        metrics.get("output_count"),
    )

    ti.xcom_push(key="verified_manifest", value=manifest)
    ti.xcom_push(key="verified_metrics", value=metrics)

    return {
        "published_run_path": run_path,
        "run_id": run_id,
        "hdfs_run_id": hdfs_run_id,
    }


def finish_run(**context):
    """
    Фиксация успешного завершения. Фактическая замена CURRENT происходит в Spark-приложении
    (commit_current_run). Здесь только логируем финальный статус.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    ds = context["ds"]

    stg_root = context["params"]["stg_path"]
    hdfs_run_id = run_id.replace(":", "-")

    run_path = posixpath.join(stg_root, "runs", hdfs_run_id)
    current_path = posixpath.join(stg_root, "CURRENT")

    manifest = ti.xcom_pull(task_ids="verify_run_bundle", key="verified_manifest") or {}
    metrics = ti.xcom_pull(task_ids="verify_run_bundle", key="verified_metrics") or {}

    logger.info("=== FIRMS RAW->STG FINISHED ===")
    logger.info("run_id: %s", run_id)
    logger.info("execution_date: %s", ds)
    logger.info("published_run_path: %s", run_path)
    logger.info("current_path: %s", current_path)
    logger.info("input_count: %s", metrics.get("input_count"))
    logger.info("invalid_count: %s", metrics.get("invalid_count"))
    logger.info("duplicate_count: %s", metrics.get("duplicate_count"))
    logger.info("output_count: %s", metrics.get("output_count"))
    logger.info("manifest_status: %s", manifest.get("status"))
    logger.info("===============================")

    return True


with DAG(
    dag_id="firms_raw_to_stg_v2",
    default_args=default_args,
    description="NASA FIRMS RAW -> STG + КАРАНТИН (оркестрация Spark)",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,
    tags=["fires", "firms", "raw", "stg", "quarantine"],
    render_template_as_native_obj=True,
    params={
        "raw_path": "/raw/nasa_firms/",
        "stg_path": "/stg/nasa_firms/",
        "spark_app_path": "/apps/spark_firms_raw_to_stg.py",
    },
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    
    wait_for_raw = ExternalTaskSensor(
        task_id="wait_for_raw_load",
        external_dag_id="nasa_firms_extract_v2",
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        timeout=6 * 60 * 60,
        poke_interval=60,
        mode="reschedule",
    )

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_spark_job",
        application="{{ params.spark_app_path }}",
        conn_id="spark_default",
        application_args=[
            "--raw_path",
            "{{ params.raw_path }}",
            "--stg_path",
            "{{ params.stg_path }}",
            "--run_id",
            "{{ run_id }}",
            "--execution_date",
            "{{ ds }}",
        ],
        conf={
            "spark.master": "yarn",
            "spark.submit.deployMode": "client",
        },
        executor_memory="512m",
        driver_memory="512m",
        verbose=True,
        status_poll_interval=30,
    )

    verify_run_bundle_task = PythonOperator(
        task_id="verify_run_bundle",
        python_callable=verify_run_bundle,
    )

    finish_run_task = PythonOperator(
        task_id="finish_run",
        python_callable=finish_run,
    )

    start >> wait_for_raw >> submit_spark_job >> verify_run_bundle_task >> finish_run_task
