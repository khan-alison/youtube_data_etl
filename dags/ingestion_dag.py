from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from jobs.ingestion.trending_videos import fetch_and_save_trending_videos as save_trending_videos
from jobs.ingestion.channels_information import fetch_and_save_channels_information as save_channels_information
from jobs.ingestion.comment_threads import fetch_and_save_comment_threads as save_comment_threads
from jobs.ingestion.comments import fetch_and_save_comments as save_comments
from jobs.ingestion.search_relate_video_categories import fetch_and_save_related_categories_videos as save_related_categories_videos
from jobs.ingestion.categories import fetch_and_save_categories as save_categories
from dotenv import load_dotenv
import os

load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

def create_data_folder(**kwargs): 
    current_timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    folder_name = f'{current_timestamp}'
    return folder_name

def fetch_and_save_trending_videos_task(ti):
    folder_name = ti.xcom_pull(task_ids='create_data_folder_task')
    save_trending_videos(folder_name=folder_name)

def fetch_and_save_channels_information_task(ti):
    folder_name = ti.xcom_pull(task_ids='create_data_folder_task')
    save_channels_information(folder_name=folder_name)

def fetch_and_save_comment_threads_task(ti):
    folder_name = ti.xcom_pull(task_ids='create_data_folder_task')
    save_comment_threads(folder_name=folder_name)

def fetch_and_save_comments_task(ti):
    folder_name = ti.xcom_pull(task_ids='create_data_folder_task')
    save_comments(folder_name=folder_name)

def fetch_and_save_related_categories_videos_task(ti):
    folder_name = ti.xcom_pull(task_ids='create_data_folder_task')
    save_related_categories_videos(folder_name=folder_name)

def fetch_and_save_categories_task(ti):
    folder_name = ti.xcom_pull(task_ids='create_data_folder_task')
    save_categories(folder_name=folder_name)  


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

    fetch_trending_task = PythonOperator(
        task_id='fetch_trending_videos',
        python_callable=fetch_and_save_trending_videos_task,
        provide_context=True,
        dag=dag
    )

    fetch_channels_info_task = PythonOperator(
        task_id='fetch_channels_info',
        python_callable=fetch_and_save_channels_information_task,
        provide_context=True,
        dag=dag
    )

    fetch_categories_info_task = PythonOperator(
        task_id='fetch_categories_info',
        python_callable=fetch_and_save_categories_task,
        provide_context=True,
        dag=dag
    )

    fetch_related_categories_videos_task = PythonOperator(
        task_id='fetch_related_categories_videos',
        python_callable=fetch_and_save_related_categories_videos_task,
        provide_context=True,
        dag=dag
    )

    fetch_comment_threads_task = PythonOperator(
        task_id='fetch_comment_threads',
        python_callable=fetch_and_save_comment_threads_task,
        provide_context=True,
        dag=dag
    )

    fetch_comments_task = PythonOperator(
        task_id='fetch_comments',
        python_callable=fetch_and_save_comments_task,
        provide_context=True,
        dag=dag
    )

    create_data_folder_task >> fetch_trending_task
    fetch_trending_task >> [fetch_channels_info_task, fetch_comment_threads_task,
                            fetch_related_categories_videos_task, fetch_categories_info_task]
    fetch_comment_threads_task >> fetch_comments_task