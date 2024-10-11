from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os

load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class ChannelsInformationFetcher(YoutubeFetcher):
    def __init__(self, data_manager, ids):
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(ids)
        }
        formatter = YouTubeHelper().format_channel_info_data

        super().__init__(data_manager=data_manager, endpoint_name='channels', params=params, formatter=formatter)


def fetch_and_save_channels_information(folder_name):
    trending_videos_data_manager = BaseCSVManager(
        folder_name=folder_name,
        file_name="trending_videos.csv",
        bucket_name=bucket_name)
    trending_video_data = trending_videos_data_manager.load_data()
    print(f"trending_video_data {trending_video_data}")
    
    if trending_video_data is not None:
        trending_video_data = trending_video_data.dropna(subset=['channel_id'])
        
        channel_ids = trending_video_data['channel_id'].astype(str).tolist()
        
        unique_channel_ids = list(set(channel_ids))
        
        data_manager = BaseCSVManager(
            folder_name=folder_name,
            file_name="channels_information.csv",
            bucket_name=bucket_name
        )

        executor = ChannelsInformationFetcher(data_manager=data_manager, ids=unique_channel_ids)
        executor.execute()
    else:
        print("No data found in trending_videos.csv")

fetch_and_save_channels_information('2024-10-11-09-27-29')

