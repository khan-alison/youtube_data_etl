from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os

load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class ChannelsInformationFetcher(YoutubeFetcher):
    def __init__(self, data_manager):
        trending_videos_data_manager = BaseCSVManager(
            file_name="trending_videos.csv",
            bucket_name=bucket_name)
        trending_video_data = trending_videos_data_manager.load_data()
        channel_ids = trending_video_data['channel_id'].tolist()
        unique_channel_ids = list(set(channel_ids))
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(unique_channel_ids)
        }

        formatter = YouTubeHelper().format_channel_info_data

        super().__init__(data_manager=data_manager, endpoint_name='channels', params=params, formatter=formatter)


if __name__ == "__main__":
    data_manager = BaseCSVManager(
        file_name="channels_information.csv",
        bucket_name=bucket_name
    )

    executor = ChannelsInformationFetcher(data_manager=data_manager)
    executor.execute()
