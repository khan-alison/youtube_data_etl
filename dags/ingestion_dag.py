from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from jobs.ingestion.trending_videos import TrendingVideosFetcher
from jobs.ingestion.channels_information import ChannelsInformationFetcher
from common.base_manager import BaseCSVManager
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

with DAG(
        'ingestion_dag',
        default_args=default_args,
        description='Ingestion DAG for fetching YouTube data',
        schedule='@daily',
        catchup=False,
) as dag:
    def fetch_trending_videos_task():
        data_manager = BaseCSVManager(
            file_name="trending_videos.csv",
            bucket_name=bucket_name
        )
        executor = TrendingVideosFetcher(data_manager=data_manager)
        executor.execute()


    def fetch_channels_info_task():
        trending_videos_data_manager = BaseCSVManager(
            file_name="trending_videos.csv",
            bucket_name=bucket_name)
        trending_video_data = trending_videos_data_manager.load_data()
        channel_ids = trending_video_data['channel_id'].tolist()
        unique_channel_ids = list(set(channel_ids))
        data_manager = BaseCSVManager(
            file_name="channels_information.csv",
            bucket_name=bucket_name
        )

        executor = ChannelsInformationFetcher(data_manager=data_manager, ids=unique_channel_ids)
        executor.execute()


    fetch_trending = PythonOperator(
        task_id='fetch_trending_videos',
        python_callable=fetch_trending_videos_task,
        dag=dag
    )

    fetch_channels_info = PythonOperator(
        task_id='fetch_channels_info',
        python_callable=fetch_channels_info_task,
        dag=dag
    )

    fetch_trending >> fetch_channels_info
