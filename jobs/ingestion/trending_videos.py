from common.raw_fetcher import RawDataFetcher
from common.base_manager import BaseCSVManager
import os
import googleapiclient.discovery
from dotenv import load_dotenv
import pandas as pd
from helper.youtube_helper import YouTubeHelper
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class TrendingVideosFetcher(RawDataFetcher):
    def __init__(self, youtube, data_manager):
        super().__init__(youtube, data_manager)

    def fetch_data(self):
        """
        Fetch the most popular videos in Vietnam region
        """
        logger.info("Fetching trending videos data...✅")
        try:
            request = self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                chart="mostPopular",
                regionCode="VN",
                maxResults=20
            )
            response = request.execute()
            return response
        except Exception as e:
            logger.error(f'Error fetching data: {str(e)}')
            return None

    def format_data(self, data):
        """
        Convert data from JSON to DataFrame with necessary columns
        """
        if not data or 'items' not in data:
            logger.error("No data to format. 🙂‍↔️❌")
            return pd.DataFrame()

        videos_data = []
        for item in data['items']:
            video_data = {
                "video_id": item['id'],
                "title": item['snippet']['title'],
                "view_count": item['statistics']['viewCount'],
                "like_count": item['statistics'].get('likeCount', 'N/A'),
                "comment_count": item['statistics'].get('commentCount', 'N/A'),
                "published_at": item['snippet']['publishedAt'],
                "duration": YouTubeHelper.convert_duration(item['contentDetails']['duration']),
                "dimension": item['contentDetails'].get('dimension', 'N/A'),
                "definition": item['contentDetails'].get('definition', 'N/A'),
                "licensed_content": item['contentDetails'].get('licensedContent', False),
                "channel_id": item['snippet']['channelId'],
                "channel_title": item['snippet']['channelTitle'],
                "tags":';'.join(item['snippet'].get('tags', [])),
                "category_id": item['snippet'].get('categoryId', 'N/A'),
                "audio_language": item['snippet'].get('defaultAudioLanguage', 'N/A')
            }
            videos_data.append(video_data)
        df = pd.DataFrame(videos_data)
        return df

    def save_data(self, data):
        formatted_data = self.format_data(data)
        self.data_manager.save_data(formatted_data)

    def execute(self):
        data = self.fetch_data()
        self.save_data(data)


if __name__ == '__main__':
    load_dotenv()
    developer_key = os.getenv("DEVELOPER_KEY")
    bucket_name = os.getenv("DATALAKE_BUCKET")
    configuration = {
        "file_name": "trending_videos.csv",
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
        file_name=configuration['file_name'],
        bucket_name=bucket_name
    )

    fetcher = TrendingVideosFetcher(youtube=youtube, data_manager=data_manager)
    fetcher.execute()
