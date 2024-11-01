from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from libs_dag.kafka_processor import KafkaProcessor
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


def process_kafka_messages():
    processor = KafkaProcessor(
        topic='minio-events',
        bootstrap_servers='kafka:9092',
        group_id='airflow-consumer-group'
    )
    processor.process_messages()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'orchestration_handle_minio_notify_events',
    default_args=default_args,
    description='Process MinIO events from Kafka',
    schedule_interval=None,
    catchup=False
) as dag:

    kafka_task = PythonOperator(
        task_id='process_kafka_messages',
        python_callable=process_kafka_messages,
        dag=dag
    )

    kafka_task
