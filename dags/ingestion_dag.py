import json
from io import BytesIO
from minio import Minio
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    batch_run_id = f'batch_run_timestamp={batch_run_timestamp}'
    kwargs['ti'].xcom_push(
        key='batch_run_id', value=batch_run_id)
    return batch_run_id


def fetch_and_save_trending_videos_task(ti):
    try:
        batch_run_id = ti.xcom_pull(task_ids='create_data_folder_task')
        save_trending_videos(batch_run_id=batch_run_id)
    except Exception as e:
        logger.info(f"Error in fetching or saving trending videos: {str(e)}")


def generate_metadata_file(ti):
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    batch_run_id = ti.xcom_pull(task_ids='create_data_folder_task')
    date_str = datetime.now().strftime('%Y%m%d')

    folder_path = f"youtube/trending/trending_videos/data/date={date_str}/batch_run_timestamp={batch_run_id}/"

    objects = minio_client.list_objects(
        bucket_name, prefix=folder_path, recursive=True)
    partitions = [
        f"./{obj.object_name}" for obj in objects if obj.object_name.endswith(".csv")]

    metadata = {
        "event_type": "PARTNER_ABC_SHARE_FILE",
        "date": date_str,
        "batch_run_timestamp": batch_run_id,
        "partitions": partitions
    }

    metadata_json = json.dumps(metadata, indent=4)
    metadata_bytes = metadata_json.encode('utf-8')

    metadata_path = f"youtube/trending/trending_videos/meta_data/date={date_str}/batch_run_timestamp={batch_run_id}/metadata.json"

    minio_client.put_object(
        bucket_name,
        metadata_path,
        data=BytesIO(metadata_bytes),
        length=len(metadata_bytes),
        content_type="application/json"
    )

    print(f"Metadata file saved to {metadata_path}")


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

    spark_submit_task = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='./jobs/ingestion/trending_videos.py',
        conn_id='spark_conn',
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT"),
            "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY"),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g",
        },
        execution_timeout=timedelta(hours=2),
        dag=dag
    )

generate_metadata_task = PythonOperator(
    task_id='generate_metadata',
    python_callable=generate_metadata_file,
    provide_context=True,
    dag=dag
)

create_data_folder_task >> spark_submit_task >> generate_metadata_task
