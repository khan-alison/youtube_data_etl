from common.youtube_fetcher import YoutubeFetcher
from common.file_manager.csv_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
from common.spark_session import SparkSessionManager
import os
import argparse
from helper.logger import LoggerSimple


load_dotenv()

logger = LoggerSimple.get_logger(__name__)

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class ChannelsInformationFetcher(YoutubeFetcher):
    def __init__(self, spark, data_manager, ids):
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(ids)
        }
        formatter = YouTubeHelper().format_channel_info_data

        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='channels', params=params, formatter=formatter)


def fetch_and_save_channels_information(current_date, batch_run_timestamp):
    try:
        spark = SparkSessionManager.get_session()
        trending_videos_data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database="raw",
            table="trending_videos",
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name
        )
        trending_video_data = trending_videos_data_manager.load_data()

        if trending_video_data is not None:
            channel_ids_rdd = trending_video_data.select(
                "channel_id").rdd.flatMap(lambda x: x)
            channel_ids = channel_ids_rdd.collect()
            unique_channel_ids = list(set(channel_ids))
            logger.info(f"Unique channel IDs: {unique_channel_ids}")

            data_manager = BaseCSVManager(
                spark=spark,
                source_system='youtube',
                database="raw",
                table="channels_information",
                run_date=current_date,
                batch_run_id=batch_run_timestamp,
                bucket_name=bucket_name
            )

            executor = ChannelsInformationFetcher(spark=spark,
                                                  data_manager=data_manager, ids=unique_channel_ids)
            executor.execute()
        else:
            logger.info("No data found in trending_videos.csv")
    except Exception as e:
        logger.error(
            f"Error in fetching or saving channels information: {str(e)}")
    finally:
        SparkSessionManager.close_session()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fetch and save YouTube channel information')
    parser.add_argument('--batch_run_timestamp', type=str,
                        required=True, help='The batch run ID for the job.')
    parser.add_argument('--current_date', type=str,
                        required=True, help='The date run the job.')
    args = parser.parse_args()
    fetch_and_save_channels_information(
        args.current_date, args.batch_run_timestamp)
