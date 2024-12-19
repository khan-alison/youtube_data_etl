from airflow import DAG
from datetime import datetime, timedelta
from libs_dag.spark_utils import SparkUtils
from libs_dag.minio_utils import MinioUtils

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

spark_utils = SparkUtils()
minio_utils = MinioUtils()

with DAG(
        'ingestion_dag',
        default_args=default_args,
        description='Ingestion DAG for fetching YouTube data',
        schedule='@daily',
        catchup=False,
) as dag:
    create_data_folder_task = spark_utils.create_data_folder_task(dag=dag)

    fetch_and_save_trending_videos_job = spark_utils.create_ingestion_spark_bash_operator(
        task_id='fetch_and_save_trending_videos_job',
        script_name='trending_videos',
        dag=dag
    )
    
    generate_trending_metadata_task = minio_utils.create_metadata_task(
        task_id='generate_trending_metadata_task',
        source_system='youtube',
        database='raw',
        table='trending_videos',
        dag=dag
    )

    fetch_and_save_channel_information_jobs = spark_utils.create_ingestion_spark_bash_operator(
        task_id='fetch_and_save_channel_information_jobs',
        script_name='channels_information',
        dag=dag
    )

    generate_channel_metadata_task = minio_utils.create_metadata_task(
        task_id='generate_channel_metadata',
        source_system='youtube',
        database='raw',
        table='channels_information',
        dag=dag
    )

    fetch_and_save_categories_task = spark_utils.create_ingestion_spark_bash_operator(
        task_id='fetch_and_save_categories_task',
        script_name='categories',
        dag=dag
    )

    generate_categories_metadata_task = minio_utils.create_metadata_task(
        task_id='generate_categories_metadata',
        source_system='youtube',
        database='raw',
        table='categories',
        dag=dag
    )

    fetch_and_save_comment_threads_task = spark_utils.create_ingestion_spark_bash_operator(
        task_id='fetch_and_save_comment_threads_task',
        script_name='comment_threads',
        dag=dag
    )

    generate_comment_threads_metadata_task = minio_utils.create_metadata_task(
        task_id='generate_comment_threads_metadata_task',
        source_system='youtube',
        database='raw',
        table='comment_threads',
        dag=dag
    )

    fetch_and_save_replies_task = spark_utils.create_ingestion_spark_bash_operator(
        task_id='fetch_and_save_replies_comments_task',
        script_name='replies',
        dag=dag
    )

    generate_replies_metadata_task = minio_utils.create_metadata_task(
        task_id='generate_replies_comment_metadata_task',
        source_system='youtube',
        database='raw',
        table='replies',
        dag=dag
    )

    fetch_and_save_related_category_videos_task = spark_utils.create_ingestion_spark_bash_operator(
        task_id='fetch_and_save_related_category_videos_task',
        script_name='search_relate_category_videos',
        dag=dag
    )

    generate_related_category_videos_metadata_task = minio_utils.create_metadata_task(
        task_id='generate_related_category_videos_metadata_task',
        source_system='youtube',
        database='raw',
        table='related_category_videos',
        dag=dag
    )

    create_data_folder_task >> [
        fetch_and_save_trending_videos_job, fetch_and_save_categories_task]

    fetch_and_save_trending_videos_job >> generate_trending_metadata_task
    fetch_and_save_categories_task >> generate_categories_metadata_task
    generate_trending_metadata_task >> [
        fetch_and_save_channel_information_jobs, fetch_and_save_comment_threads_task, fetch_and_save_related_category_videos_task
    ]

    fetch_and_save_channel_information_jobs >> generate_channel_metadata_task
    fetch_and_save_related_category_videos_task >> generate_related_category_videos_metadata_task
    fetch_and_save_comment_threads_task >> generate_comment_threads_metadata_task >> fetch_and_save_replies_task >> generate_replies_metadata_task
