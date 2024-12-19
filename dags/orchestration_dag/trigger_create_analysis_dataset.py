from airflow import DAG
from airflow.operators.python import PythonOperator
import signal
from datetime import datetime, timedelta
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


def process_dataset_events():
    """Consume Kafka messages and trigger DAGs based on events"""
    from dags.libs_dag.dataset_events_processor import DatasetEventsProcessor

    processor = DatasetEventsProcessor(
        topic='dataset_creation_events',
        bootstrap_servers='kafka:9092',
        group_id='table_events_consumer_group',
        mapping_file='/opt/airflow/job_entries/g2i/mapping.json'
    )

    def handle_signal(signum, frame):
        processor.stop()
        logger.info("Received shutdown signal, stopping consumer...")

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    processor.process_messages()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'creation_dataset_events_consumer',
    default_args=default_args,
    description='Consumer for table creation events',
    schedule_interval='@once',
    catchup=False
) as dag:

    process_events = PythonOperator(
        task_id='process_dataset_events',
        python_callable=process_dataset_events,
        execution_timeout=None,
    )
