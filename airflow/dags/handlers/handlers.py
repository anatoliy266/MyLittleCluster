async def log_deadline_miss(**kwargs):
    """Асинхронный callback для DeadlineAlert: логирует пропуск дедлайна."""
    context = kwargs.get("context", {})
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", "unknown")
    logger.critical("SLA MISS в DAG %s! Дедлайн пропущен.", dag_id)

def task_failure_alert(context):
    """Коллбек для сфейлившейся задачи."""
    logger.critical(f"Task {context.get('task_instance').task_id} failed! {context.get('exception')}")