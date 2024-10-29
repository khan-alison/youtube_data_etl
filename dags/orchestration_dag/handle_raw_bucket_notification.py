from airflow import DAG
from datetime import datetime, timedelta
from libs_dag.kafka import DeferrableKafkaSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'kafka_listener_dag',
    default_args=default_args,
    description='A DAG that consumes and processes Kafka messages with deferrable operator',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1 
) as dag:

    kafka_sensor_task = DeferrableKafkaSensor(
        task_id='consume_and_process_kafka_messages',
        topic='minio-events',
        bootstrap_servers='kafka:9092',
        group_id='airflow-consumer-group',
        mode='reschedule',
        poll_interval=1,
        timeout=None
    )

    kafka_sensor_task
