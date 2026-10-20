import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaConnectionError

logger = logging.getLogger("airflow.task")

def ensure_topic(bootstrap_servers: list, topic_name: str, partitions: int = 3, replication_factor: int = 1):
    """Проверяет существование топика в Kafka.

    Если топика нет — создает его с боевым конфигом удержания данных.
    """
    logger.info(f"Подключение к Kafka Admin по адресу: {bootstrap_servers}")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='airflow_infra_manager',
            request_timeout_ms=10000
        )
    except KafkaConnectionError as e:
        logger.error(f"Критическая ошибка подключения к брокеру {bootstrap_servers}: {e}")
        raise

    # Сообщения удаляются через 24 часа, сегменты бьются по 10 МБ для экономии памяти
    topic_config = {
        "retention.ms": "86400000",   
        "segment.bytes": "10485760"   
    }

    new_topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        topic_configs=topic_config
    )

    try:
        logger.info(f"Проверка/создание топика '{topic_name}'...")
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        logger.info(f"🚀 Топик '{topic_name}' успешно инициализирован.")
    except TopicAlreadyExistsError:
        logger.info(f"ℹ️ Топик '{topic_name}' уже существует в кластере. Создание пропущено.")
    except Exception as e:
        logger.error(f"Не удалось создать топик из-за ошибки: {e}")
        raise
    finally:
        admin_client.close()