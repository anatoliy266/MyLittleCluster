from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk  import Variable
from airflow.providers.standard.operators.python import PythonOperator

from airflow.exceptions import AirflowFailException

KAFKA_BOOTSTRAP_SERVERS = [
    "broker-1:9092",
    "broker-2:9092",
    "broker-3:9092"
]
KAFKA_TOPIC = "wildfires-russia"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_init_kafka():
    """Обертка для вызова инициализации Kafka."""
    from FIRMS.kafka_utils import ensure_topic
    ensure_topic(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic_name=KAFKA_TOPIC)

def run_fetch_data(**context):
    """Обертка для вызова сбора данных NASA."""
    if not firms_key:
        raise AirflowFailException("Критическая ошибка: Переменная 'nasa_firms_token' не настроена в Airflow Variables!")
    from FIRMS.producer import fetch_and_push_data
    
    firms_key = Variable.get("nasa_firms_token")
    if not firms_key:
        raise ValueError("Критическая ошибка: Переменная 'nasa_firms_token' не настроена в Airflow Variables!")
        
    fetch_and_push_data(
        logical_date=context['logical_date'],
        firms_key=firms_key,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic_name=KAFKA_TOPIC
    )

with DAG(
    dag_id="nasa_wildfires_modular_v1",
    default_args=default_args,
    description="Пайплайн проверки топика и сбора данных NASA FIRMS на PythonOperator",
    # ИСПРАВЛЕНО: вместо schedule_interval используем schedule
    schedule="@daily", 
    start_date=datetime(2026, 5, 28), 
    catchup=True, 
    max_active_runs=1,
    tags=["nasa", "kafka", "production"],
) as dag:

    task_init_kafka = PythonOperator(
        task_id="check_or_create_topic",
        python_callable=run_init_kafka,
    )

    task_fetch_data = PythonOperator(
        task_id="fetch_and_push_data",
        python_callable=run_fetch_data,
    )

    task_init_kafka >> task_fetch_data