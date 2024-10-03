from common.raw_fetcher import RawDataFetcher
from common.base_manager import BaseCSVManager
import os
import googleapiclient.discovery
from dotenv import load_dotenv
import pandas as pd
import json
from minio import Minio
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class ChannelsInformation(RawDataFetcher):
    def __init__(self, youtube, data_manager):
        super().__init__(youtube, data_manager)

    def fetch_data(self):
        """
        Fetch detailed information about channels using YouTube API.
        :param channel_ids: List of channel IDs to fetch data for.
        """
        logger.info("Fetching channel data...")
        try:
            request = self.youtube.channels().list(
                part="snippet,statistics,contentDetails",
                id=",".join(channel_ids)
            )
            response = request.execute()
            return response
        except Exception as e:
            logger.error(f"Error fetching channel data: {str(e)}")
            return None

    def format_data(self, data):
        """
        Convert data from JSON to DataFrame with necessary columns.
        """
        channels_data = []
        for item in data['items']:
            channel_data = {
                "channel_id": item['id'],
                "title": item['snippet']['title'],
                "description": item['snippet'].get('description', 'N/A'),
                "published_at": item['snippet']['publishedAt'],
                "view_count": item['statistics']['viewCount'],
                "subscriber_count": item['statistics'].get('subscriberCount', 'N/A'),
                "video_count": item['statistics'].get('videoCount', 'N/A'),
                "country": item['snippet'].get('country', 'N/A')
            }
            channels_data.append(channel_data)
        df = pd.DataFrame(channels_data)
        return df

    def save_data(self, data):
        formatted_data = self.format_data(data)
        self.data_manager.save_data(formatted_data)

    def execute(self, data):
        data = self.fetch_data()
        if data:
            self.save_data(data)
            logger.info("Channel data fetched and saved successfully.")
        else:
            logger.error("Failed to fetch channel data.")


if __name__ == "__main__":
    load_dotenv()
    developer_key = os.getenv("DEVELOPER_KEY")
    bucket_name = os.getenv("DATALAKE_BUCKET")
    configuration = {
        "file_name": {
            "channels": "channels_information.csv",
            "trending_videos": "trending_videos.csv"
        },
        "api_service_name": "youtube",
        "api_version": "v3"
    }

    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    youtube = (googleapiclient.discovery.build(
        configuration['api_service_name'],
        configuration['api_version'],
        developerKey=developer_key)
    )

    data_manager = BaseCSVManager(
        file_name=configuration['file_name']['channels'],
        bucket_name=bucket_name
    )

    trending_videos_data_manager = BaseCSVManager(
        file_name=configuration['file_name']['trending_videos'],
        bucket_name=bucket_name)

    trending_video_data = trending_videos_data_manager.load_data()
    channel_ids = trending_video_data['channel_id'].tolist()
    print(channel_ids)

    fetcher = ChannelsInformation(youtube=youtube, data_manager=data_manager)
    fetcher.execute(channel_ids)

    data_manager.load_data()
