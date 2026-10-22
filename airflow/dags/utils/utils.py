def get_hdfs_client(conn_id) -> InsecureClient:
    """Безопасное получение клиента HDFS из Airflow Connections."""
    try:
        hook = WebHDFSHook(webhdfs_conn_id=conn_id)
        return hook.get_conn()
    except Exception as e:
        logger.error(f"Failed to initialize HDFS client: {e}")
        raise AirflowFailException("Не удалось подключиться к HDFS. Проверьте Connection 'hdfs_conn'.") from e