import argparse
from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from common.spark_session import SparkSessionManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, TimestampType
from helper.logger import LoggerSimple


load_dotenv()

logger = LoggerSimple.get_logger(__name__)

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class TrendingVideosFetcher(YoutubeFetcher):
    def __init__(self, spark: SparkSession, data_manager: BaseCSVManager) -> None:
        params: dict[str, str | int] = {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": "US",
            "maxResults": 20
        }

        formatter = YouTubeHelper().format_trending_videos
        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='videos', params=params, formatter=formatter)


def fetch_and_save_trending_videos(current_date: str, batch_run_timestamp: str) -> None:
    try:
        spark = SparkSessionManager.get_session()

        data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database="trending",
            table="trending_videos",
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name
        )
        executor = TrendingVideosFetcher(
            spark=spark, data_manager=data_manager)

        executor.execute()

    except Exception as e:
        logger.error(f"Error in fetching or saving trending videos: {str(e)}")
    finally:
        SparkSessionManager.close_session()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fetch and save trending YouTube videos.')
    parser.add_argument('--batch_run_timestamp', type=str,
                        required=True, help='The batch run ID for the job.')
    parser.add_argument('--current_date', type=str,
                        required=True, help='The date run the job.')
    args = parser.parse_args()
    fetch_and_save_trending_videos(args.current_date, args.batch_run_timestamp)
