import pandas as pd
from common.youtube_fetcher import YoutubeFetcher
from common.file_manager.csv_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from common.spark_session import SparkSessionManager
from dotenv import load_dotenv
import os
from helper.logger import LoggerSimple
import argparse
from typing import List, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logger = LoggerSimple.get_logger(__name__)
load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class CommentsFetcher(YoutubeFetcher):
    def __init__(self, spark: SparkSession, data_manager: BaseCSVManager, parentId: str):
        params = {
            "part": "snippet",
            "parentId": parentId,
            "maxResults": 10
        }

        formatter = YouTubeHelper().format_comments_data
        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='comments', params=params, formatter=formatter)


def fetch_and_save_comments(current_date: str, batch_run_timestamp: str):
    """Main function to fetch and save comments"""
    try:
        spark = SparkSessionManager.get_session()
        comment_threads_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database='raw',
            table='comment_threads',
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name,
        )
        logger.info("Loading comment threads data from minIO. 🦩")
        comment_threads_data = comment_threads_manager.load_data()

        if comment_threads_data is not None:
            comment_thread_ids_rdd = comment_threads_data \
                .select('comment_thread_id') \
                .rdd.flatMap(lambda x: x)
            comment_thread_ids = comment_thread_ids_rdd.collect()

            data_manager = BaseCSVManager(
                spark=spark,
                source_system='youtube',
                database='raw',
                table='replies',
                run_date=current_date,
                batch_run_id=batch_run_timestamp,
                bucket_name=bucket_name
            )

            combined_df = None
            for thread_id in comment_thread_ids:
                logger.info(
                    f"Fetching replies for comment thread 💬: {thread_id}")
                executor = CommentsFetcher(
                    spark=spark,
                    data_manager=data_manager,
                    parentId=thread_id
                )
                response = executor.fetch_data()
                logger.info(f"response replies for comment {response}")

                if response:
                    formatted_data = executor.format_data(response)
                    logger.info(f"formatted data for comment {formatted_data}")
                    if formatted_data.count() > 0:
                        formatted_data = formatted_data \
                            .withColumn('comment_thread_id', F.lit(thread_id))
                        combined_df = formatted_data if combined_df is None else combined_df.union(
                            formatted_data)
            if combined_df:
                logger.info("Saving combined replies data...😎")
                data_manager.save_data(combined_df)
            else:
                logger.info("No replies data to save 😟")
        else:
            logger.info("No comment threads data available.")
    except Exception as e:
        logger.error(f"Error in fetching or saving comments 😢: {str(e)}")
    finally:
        SparkSessionManager.close_session()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Fetch replies Youtube video.')
    parser.add_argument('--batch_run_timestamp', type=str,
                        required=True, help='The batch run timestamp')
    parser.add_argument('--current_date', type=str,
                        required=True, help='The date run job.')
    args = parser.parse_args()
    fetch_and_save_comments(args.current_date, args.batch_run_timestamp)
