from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os
import pandas as pd
from helper.logger import LoggerSimple
from common.spark_session import SparkSessionManager
from pyspark.sql import SparkSession
import argparse
from typing import List, Optional
from pyspark.sql import functions as F


logger = LoggerSimple.get_logger(__name__)
load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class CommentThreadsFetcher(YoutubeFetcher):
    def __init__(self, spark: SparkSession,  data_manager: BaseCSVManager, video_id: str) -> None:
        params: dict[str, str | int] = {
            "part": "id,snippet,replies",
            "videoId": video_id,
            "maxResults": 5
        }

        formatter = YouTubeHelper().format_comment_threads_data
        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='commentThreads', params=params, formatter=formatter)


def fetch_and_save_comment_threads(current_date: str, batch_run_timestamp: str) -> None:
    """Main function to fetch and save comment threads for trending videos."""
    try:
        spark: SparkSession = SparkSessionManager.get_session()

        trending_videos_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database="trending",
            table="trending_videos",
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name
        )

        logger.info("Loading trending videos data.")
        trending_video_data: Optional[DataFrame] = trending_videos_manager.load_data(
        )

        if trending_video_data is not None:
            video_ids_rdd = trending_video_data.select(
                'video_id').rdd.flatMap(lambda x: x)
            video_ids = video_ids_rdd.collect()

            data_manager = BaseCSVManager(
                spark=spark,
                source_system='youtube',
                database="trending",
                table="comment_threads",
                run_date=current_date,
                batch_run_id=batch_run_timestamp,
                bucket_name=bucket_name
            )

            combined_df = None
            for video_id in video_ids:
                logger.info(f"Fetching data for video_id: {video_id}")
                executor = CommentThreadsFetcher(
                    spark=spark,
                    data_manager=data_manager,
                    video_id=video_id
                )
                response = executor.fetch_data()

                if response:
                    formatted_data = executor.format_data(response)
                    if formatted_data.count() > 0:
                        formatted_data = formatted_data.withColumn(
                            'video_id', F.lit(video_id))
                        combined_df = formatted_data if combined_df is None else combined_df.union(
                            formatted_data)

            if combined_df is not None and combined_df.count() > 0:
                logger.info(
                    f"Saving combined data with {combined_df.count()} rows...")
                data_manager.save_data(combined_df)
            else:
                logger.info("No data to save.")
        else:
            logger.info("No trending video data available.")

    except Exception as e:
        logger.error(f"Error in fetching or saving comment threads: {str(e)}")
    finally:
        SparkSessionManager.close_session()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fetch and save Youtube comment threads.')
    parser.add_argument('--batch_run_timestamp', type=str,
                        required=True, help='The batch run ID for the job.')
    parser.add_argument('--current_date', type=str,
                        required=True, help='The date run the job.')
    args = parser.parse_args()
    fetch_and_save_comment_threads(args.current_date, args.batch_run_timestamp)
