import math
from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from common.spark_session import SparkSessionManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os
import pandas as pd
from helper.logger import LoggerSimple
import argparse
from pyspark.sql import SparkSession
from typing import List, Optional
from pyspark.sql import functions as F


logger = LoggerSimple.get_logger(__name__)
load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class SearchRelateVideoCategories(YoutubeFetcher):
    def __init__(self, spark: SparkSession, data_manager: BaseCSVManager, trending_video_category_id: str) -> None:
        params = {
            "part": "snippet",
            "type": "video",
            "videoCategoryId": trending_video_category_id,
            "maxResults": 20
        }
        formatter = YouTubeHelper().format_search_videos
        super().__init__(
            spark=spark,
            data_manager=data_manager,
            endpoint_name='search',
            params=params,
            formatter=formatter
        )


def fetch_and_save_related_categories_videos(current_date: str, batch_run_timestamp: str) -> None:
    try:
        spark = SparkSessionManager.get_session()
        trending_data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database='trending',
            table='trending_videos',
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name
        )

        data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database='trending',
            table='related_category_videos',
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name
        )

        trending_video_data: Optional[DataFrame] = trending_data_manager \
            .load_data()
        if trending_video_data is not None:
            categories_ids_rdd = trending_video_data.select('category_id').distinct() \
                .rdd.flatMap(lambda x: x)

            category_ids = categories_ids_rdd.collect()

            combined_df = None
            for category_id in category_ids:
                logger.info(f"Fetching data for category_id: {category_id}")
                executor = SearchRelateVideoCategories(
                    spark=spark,
                    data_manager=data_manager,
                    trending_video_category_id=category_id
                )
                response = executor.fetch_data()

                if response:
                    formatted_data = executor.format_data(response)
                    if formatted_data.count() > 0:
                        formatted_data = formatted_data.withColumn(
                            'category_id',
                            F.lit(category_id)
                        )
                    combined_df = formatted_data if combined_df is None else combined_df.union(
                        formatted_data)
            if combined_df is not None:
                logger.info("Saving combined data...")
                data_manager.save_data(combined_df)
            else:
                logger.info("No data to save.")
        else:
            logger.info("No trending videos data available.")
    except Exception as e:
        logger.error(f"Error in fetching related categories videos: {str(e)}")
    finally:
        SparkSessionManager.close_session()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch related categories videos.")
    parser.add_argument("--batch_run_timestamp", type=str,
                        required=True, help='The batch run timestamp')
    parser.add_argument("--current_date", type=str,
                        required=True, help='The current date')
    args = parser.parse_args()
    fetch_and_save_related_categories_videos(
        args.current_date, args.batch_run_timestamp)
