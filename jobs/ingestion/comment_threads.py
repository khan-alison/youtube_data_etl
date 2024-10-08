from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os
import pandas as pd
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)
load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class CommentThreadsFetcher(YoutubeFetcher):
    def __init__(self, data_manager, video_id):
        params = {
            "part": "id,snippet,replies",
            "videoId": video_id,
            "maxResults": 20
        }

        formatter = YouTubeHelper().format_comment_threads_data
        super().__init__(data_manager=data_manager, endpoint_name='commentThreads', params=params, formatter=formatter)


if __name__ == "__main__":
    trending_videos_manager = BaseCSVManager(
        file_name="trending_videos.csv",
        bucket_name=bucket_name
    )
    trending_data = trending_videos_manager.load_data()
    video_ids = trending_data['video_id'].tolist()
    data_manager = BaseCSVManager(
        file_name="comment_threads.csv",
        bucket_name=bucket_name
    )

    all_comments_data = []

    for video_id in video_ids:
        logger.info(f"Fetching data for comment_id: {video_id}")
        executor = CommentThreadsFetcher(data_manager=data_manager, video_id=video_id)
        response = executor.fetch_data()

        if response:
            formatted_data = executor.format_data(response)
            if not formatted_data.empty:
                formatted_data['video_id'] = video_id
            all_comments_data.append(formatted_data)

    if all_comments_data:
        combined_df = pd.concat(all_comments_data, ignore_index=True)
        logger.info("Saving combined data...")
        data_manager.save_data(combined_df)
    else:
        logger.info("No data to save")
