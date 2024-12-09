from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
from common.spark_session import SparkSessionManager
from pyspark.sql import SparkSession
import os
import argparse
from helper.logger import LoggerSimple

load_dotenv()

logger = LoggerSimple.get_logger(__name__)

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class CategoriesFetcher(YoutubeFetcher):
    def __init__(self, spark: SparkSession, data_manager: BaseCSVManager) -> None:
        params: dict[str, str] = {
            "part": "snippet",
            "regionCode": "VN"
        }
        formatter = YouTubeHelper().format_categories_data
        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='videoCategories', params=params, formatter=formatter)


def fetch_and_save_categories(current_date: str, batch_run_timestamp: str) -> None:
    try:
        spark = SparkSessionManager.get_session()

        data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database="raw",
            table="categories",
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name,
        )

        executor = CategoriesFetcher(spark=spark, data_manager=data_manager)
        executor.execute()
    except Exception as e:
        logger.error(f"Error in fetching or saving categories: {str(e)}")
    finally:
        SparkSessionManager.close_session()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch and save Youtube categories data.")
    parser.add_argument("--batch_run_timestamp", type=str,
                        required=True, help='The batch run timestamp')
    parser.add_argument("--current_date", type=str,
                        required=True, help='The current date')
    args = parser.parse_args()
    fetch_and_save_categories(args.current_date, args.batch_run_timestamp)
