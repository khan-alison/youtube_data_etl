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
    def __init__(self, spark, data_manager):
        params = {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": "US",
            "maxResults": 20
        }

        formatter = YouTubeHelper().format_trending_videos
        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='videos', params=params, formatter=formatter)


def fetch_and_save_trending_videos(batch_run_id):

    try:
        spark = SparkSession.builder.getOrCreate()

        current_date = datetime.now().strftime('%Y%m%d')
        data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database="trending",
            table="trending_videos",
            run_date=current_date,
            batch_run_id=batch_run_id,
            bucket_name=bucket_name
        )
        executor = YoutubeFetcher(spark=spark, data_manager=data_manager,
                                  endpoint_name='videos', params={
                                      "part": "snippet,statistics,contentDetails",
                                      "chart": "mostPopular",
                                      "regionCode": "US",
                                      "maxResults": 20
                                  },
                                  formatter=YouTubeHelper().format_trending_videos)
        executor.execute()

    except Exception as e:
        logger.error(f"Error in fetching or saving trending videos: {str(e)}")


if __name__ == "__main__":
    batch_run_id = "sample_batch_run_id"  # Set the actual batch_run_id if required
    fetch_and_save_trending_videos(batch_run_id)
