import json
from io import BytesIO
from minio import Minio
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from helper.logger import LoggerSimple
from common.spark_session import SparkSessionManager
from jobs.ingestion.trending_videos import fetch_and_save_trending_videos as save_trending_videos
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")

logger = LoggerSimple.get_logger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}


def create_data_folder(**kwargs):
    batch_run_timestamp = int(datetime.now().timestamp() * 1000)
    current_date = datetime.now().strftime('%Y%m%d')
    kwargs['ti'].xcom_push(
        key='batch_run_id', value=batch_run_timestamp)
    kwargs['ti'].xcom_push(
        key='current_date', value=current_date
    )
    return batch_run_timestamp


def generate_metadata_file(ti, source_system, database, table):
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    batch_run_timestamp = ti.xcom_pull(task_ids='create_data_folder_task')
    date_str = datetime.now().strftime('%Y%m%d')

    folder_path = f"{source_system}/{database}/{table}/data/date_{date_str}/batch_run_timestamp-{batch_run_timestamp}/"

    buckets = minio_client.list_buckets()
    for bucket in buckets:
        print(bucket.name, bucket.creation_date)

    objects_list = list(minio_client.list_objects(
        bucket_name, prefix=folder_path, recursive=True))

    partitions = [
        f"{obj.object_name}" for obj in objects_list if obj.object_name.endswith(".csv")]

    metadata = {
        "event_type": "PARTNER_ABC_SHARE_FILE",
        "date": date_str,
        "batch_run_timestamp": batch_run_timestamp,
        "partitions": partitions
    }

    metadata_json = json.dumps(metadata, indent=4)
    metadata_bytes = metadata_json.encode('utf-8')

    metadata_path = f"{source_system}/{database}/{table}/metadata/date_{date_str}/batch_run_timestamp-{batch_run_timestamp}/control_file.json"

    minio_client.put_object(
        bucket_name,
        metadata_path,
        data=BytesIO(metadata_bytes),
        length=len(metadata_bytes),
        content_type="application/json"
    )


with DAG(
        'ingestion_dag',
        default_args=default_args,
        description='Ingestion DAG for fetching YouTube data',
        schedule='@daily',
        catchup=False,
) as dag:
    create_data_folder_task = PythonOperator(
        task_id='create_data_folder_task',
        python_callable=create_data_folder,
        provide_context=True,
        dag=dag
    )

    fetch_and_save_trending_videos_job = BashOperator(
        task_id='fetch_and_save_trending_videos_job',
        bash_command=(
            'batch_run_id="{{ ti.xcom_pull(task_ids=\'create_data_folder_task\') }}" && '
            'current_date="{{ ti.xcom_pull(task_ids=\'create_data_folder_task\', key=\'current_date\') }}" && '
            'spark-submit --master local[*] '
            '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0 '
            '/opt/airflow/jobs/ingestion/trending_videos.py --batch_run_id $batch_run_id --current_date $current_date'
        ),
        dag=dag
    )

    generate_trending_metadata_task = PythonOperator(
        task_id='generate_trending_metadata_task',
        python_callable=generate_metadata_file,
        provide_context=True,
        op_kwargs={'source_system': 'youtube',
                   'database': 'trending', 'table': 'trending_videos'},
        trigger_rule='all_done',
        dag=dag
    )

    fetch_and_save_channel_information_jobs = BashOperator(
        task_id='fetch_and_save_channel_information_jobs',
        bash_command=(
            'batch_run_id="{{ ti.xcom_pull(task_ids=\'create_data_folder_task\')}}" && '
            'current_date="{{ ti.xcom_pull(task_ids=\'create_data_folder_task\', key=\'current_date\') }}" && '
            'spark-submit --master local[*] '
            '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367,io.delta:delta-core_2.12:2.4.0 '
            '/opt/airflow/jobs/ingestion/channels_information.py --batch_run_id $batch_run_id --current_date $current_date'
        ),
        dag=dag)

    generate_channel_metadata_task = PythonOperator(
        task_id='generate_channel_metadata',
        python_callable=generate_metadata_file,
        provide_context=True,
        op_kwargs={'source_system': 'youtube',
                   'database': 'trending', 'table': 'channels_information'},
        trigger_rule='all_done',
        dag=dag
    )


create_data_folder_task >> fetch_and_save_trending_videos_job >> generate_trending_metadata_task >> fetch_and_save_channel_information_jobs >> generate_channel_metadata_task
# create_data_folder_task >> generate_metadata_task
