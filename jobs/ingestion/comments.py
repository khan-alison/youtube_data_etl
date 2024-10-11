import pandas as pd

from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)
load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class CommentsFetcher(YoutubeFetcher):
    def __init__(self, data_manager, parentId):
        params = {
            "part": "snippet",
            "parentId": parentId,
            "maxResults": 10
        }

        formatter = YouTubeHelper().format_comments_data
        super().__init__(data_manager=data_manager, endpoint_name='comments', params=params, formatter=formatter)


def fetch_and_save_comments(folder_name):
    comment_threads_manager = BaseCSVManager(
        folder_name=folder_name,
        file_name='comment_threads.csv',
        bucket_name=bucket_name
    )

    comment_threads_data = comment_threads_manager.load_data()

    if comment_threads_data is not None:
        comment_threads_ids = comment_threads_data['comment_thread_id'].tolist()

        data_manager = BaseCSVManager(
            folder_name=folder_name,
            file_name='comments.csv',
            bucket_name=bucket_name
        )

        all_replies_data = []

        for thread_id in comment_threads_ids:
            logger.info(f"Fetching replies for comment thread: {thread_id}")
            executor = CommentsFetcher(data_manager=data_manager, parentId=thread_id)
            response = executor.fetch_data()

            if response:
                formatted_data = executor.format_data(response)
                if not formatted_data.empty:
                    formatted_data['comment_thread_id'] = thread_id
                all_replies_data.append(formatted_data)

        if all_replies_data:
            combined_replies_df = pd.concat(all_replies_data, ignore_index=True)
            logger.info("Saving combined replies data...")
            data_manager.save_data(combined_replies_df)
        else:
            print("No replies data to save")
