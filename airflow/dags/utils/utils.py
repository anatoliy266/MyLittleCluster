import logging

from dags.utils.default_args import default_args

from hdfs import InsecureClient
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook


logger = logging.getLogger("airflow.task")

def get_hdfs_client(conn_id) -> InsecureClient:
    """Безопасное получение клиента HDFS из Airflow Connections."""
    try:
        hook = WebHDFSHook(webhdfs_conn_id=conn_id)
        return hook.get_conn()
    except Exception as e:
        logger.error(f"Failed to initialize HDFS client: {e}")
        raise AirflowFailException("Не удалось подключиться к HDFS. Проверьте Connection 'hdfs_conn'.") from e

def task_failure_alert(**context):
    """Коллбек для сфейлившейся задачи."""
    logger.critical(f"Task {context.get('task_instance').task_id} failed! {context.get('exception')}")

async def log_deadline_miss(**kwargs):
    """Асинхронный callback для DeadlineAlert: логирует пропуск дедлайна."""
    context = kwargs.get("context", {})
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", "unknown")
    logger.critical("SLA MISS в DAG %s! Дедлайн пропущен.", dag_id)