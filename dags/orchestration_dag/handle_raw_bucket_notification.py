from airflow import DAG
from datetime import datetime, timedelta
from libs_dag.kafka import DeferrableKafkaSensor
from helper.logger import LoggerSimple
import os
from airflow.operators.python import PythonOperator

logger = LoggerSimple.get_logger(__name__)
bucket_name = os.getenv("DATALAKE_BUCKET")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


def trigger_load_data(**context):
    """Function to trigger the load data process using the control file path."""
    control_file_path = context['ti'].xcom_pull(
        key='control_file_path', task_ids='consume_and_process_kafka_messages')
    logger.info(control_file_path)
    if control_file_path:
        # Call Spark to run the script with the control file path as an argument
        spark = SparkSessionManager.get_session()
        loader = R2GLoadData(spark, f"s3a://{bucket_name}/{control_file_path}")
        loader.load_data()


with DAG(
    'kafka_listener_dag',
    default_args=default_args,
    description='A DAG that consumes and processes Kafka messages with deferrable operator',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    # Step 1: Kafka Sensor to detect new messages
    kafka_sensor_task = DeferrableKafkaSensor(
        task_id='consume_and_process_kafka_messages',
        topic='minio-events',
        bootstrap_servers='kafka:9092',
        group_id='airflow-consumer-group',
        mode='reschedule',
        poll_interval=5,
        timeout=None
    )

    # Step 2: Trigger the load_data process upon receiving a Kafka message
    load_data_task = PythonOperator(
        task_id='trigger_load_data_task',
        python_callable=trigger_load_data,
        provide_context=True
    )

    # Set the task dependencies
    kafka_sensor_task >> load_data_task
