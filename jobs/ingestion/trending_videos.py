from common.raw_fetcher import RawDataFetcher
from common.base_manager import BaseCSVManager
import os
import googleapiclient.discovery
from dotenv import load_dotenv
import pandas as pd


class TrendingVideosFetcher(RawDataFetcher):
    def __init__(self, youtube, data_manager):
        super().__init__(youtube, data_manager)

    def fetch_data(self):
        print(self.test)
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
        videos_data = []
        for item in data['items']:
            video_data = {
                "Video ID": item['id'],
                "Title": item['snippet']['title'],
                "Description": item['snippet'].get('description', 'N/A'),
                "View Count": item['statistics']['viewCount'],
                "Like Count": item['statistics'].get('likeCount', 'N/A'),
                "Comment Count": item['statistics'].get('commentCount', 'N/A'),
                "Published At": item['snippet']['publishedAt'],
                "Duration": item['contentDetails']['duration'],
                "Dimension": item['contentDetails'].get('dimension', 'N/A'),
                "Definition": item['contentDetails'].get('definition', 'N/A'),
                "Licensed Content": item['contentDetails'].get('licensedContent', False),
                "Channel ID": item['snippet']['channelId'],
                "Channel Title": item['snippet']['channelTitle'],
                "Tags": item['snippet'].get('tags', 'N/A'),
                "Category ID": item['snippet'].get('categoryId', 'N/A'),
                "Thumbnails": {
                    "Default": item['snippet']['thumbnails']['default']['url'],
                    "Medium": item['snippet']['thumbnails']['medium']['url'],
                    "High": item['snippet']['thumbnails']['high']['url']
                },
                "Audio Language": item['snippet'].get('defaultAudioLanguage', 'N/A')
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
