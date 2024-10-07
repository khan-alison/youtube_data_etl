from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os

load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class CategoriesFetcher(YoutubeFetcher):
    def __init__(self, data_manager):
        params = {
            "part": "snippet",
            "regionCode": "VN"
        }
        formatter = YouTubeHelper().format_categories_data
        super().__init__(data_manager=data_manager, endpoint_name='videoCategories', params=params, formatter=formatter)


if __name__ == "__main__":
    data_manager = BaseCSVManager(
        file_name="categories.csv",
        bucket_name=bucket_name
    )

    executor = CategoriesFetcher(data_manager)
    executor.execute()

