import math
from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import googleapiclient.discovery
import os
import pandas as pd
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)
load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class SearchRelateVideoCategories(YoutubeFetcher):
    def __init__(self, youtube, data_manager, trending_video_category_id):
        params = {
            "part": "snippet",
            "type": "video",
            "videoCategoryId": trending_video_category_id,  # Chỉ truyền một category_id duy nhất
            "maxResults": 20
        }
        formatter = YouTubeHelper().format_search_videos
        super().__init__(youtube, data_manager, endpoint=youtube.search(), params=params, formatter=formatter)


if __name__ == "__main__":
    youtube = googleapiclient.discovery.build(
        "youtube", "v3", developerKey=developer_key
    )

    trending_data_manager = BaseCSVManager(
        file_name="trending_videos.csv",
        bucket_name=bucket_name
    )

    data_manager = BaseCSVManager(
        file_name="relate_videos.csv",
        bucket_name=bucket_name
    )
    trending_data = trending_data_manager.load_data()
    categories_ids = trending_data['category_id'].tolist()

    valid_category_ids = [str(int(category_id)) for category_id in categories_ids if not math.isnan(float(category_id))]

    unique_category_ids = list(set(valid_category_ids))

    all_videos_data = []
    for category_id in unique_category_ids:
        logger.info(f"Fetching data for category_id: {category_id}")
        executor = SearchRelateVideoCategories(youtube, data_manager, trending_video_category_id=category_id)
        response = executor.fetch_data()

        if response:
            formatted_data = executor.format_data(response)
            if not formatted_data.empty:
                formatted_data['category_id'] = category_id
            logger.info(f"formatted_data {formatted_data}")
            all_videos_data.append(formatted_data)

    if all_videos_data:
        combined_df = pd.concat(all_videos_data, ignore_index=True)
        logger.info("Saving combined data...")
        data_manager.save_data(combined_df)
    else:
        logger.info("No data to save.")