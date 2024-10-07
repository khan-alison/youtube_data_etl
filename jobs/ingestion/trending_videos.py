from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os

load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class TrendingVideosFetcher(YoutubeFetcher):
    def __init__(self, data_manager):
        params = {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": "US",
            "maxResults": 20
        }

        formatter = YouTubeHelper().format_trending_videos
        super().__init__(data_manager=data_manager, endpoint_name='videos', params=params, formatter=formatter)


if __name__ == "__main__":
    data_manager = BaseCSVManager(
        file_name="trending_videos.csv",
        bucket_name=bucket_name
    )
    executor = TrendingVideosFetcher(data_manager=data_manager)
    executor.execute()
