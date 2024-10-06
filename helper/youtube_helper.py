import datetime

from isodate import parse_duration
from helper.logger import LoggerSimple
import json
import pandas as pd
import re

logger = LoggerSimple.get_logger(__name__)


class YouTubeHelper:
    @staticmethod
    def clean_string(value):
        """
        Cleans the input string by removing or escaping problematic characters.
        """
        if isinstance(value, str):
            value = value.replace("\n", " ").replace("\r", " ").strip()
            value = value.replace('"', '""')
        return str(value) if value is not None else 'N/A'

    @staticmethod
    def clean_numeric(value):
        """
        Ensures that the value is a valid number or returns 'N/A' if not.
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            return 'N/A'

    @staticmethod
    def clean_id(video_id):
        """
        Clean the video_id to ensure it is a valid string without special characters or spaces.
        """
        if isinstance(video_id, str):
            video_id = video_id.strip()
        return video_id

    @staticmethod
    def convert_duration(duration):
        """
        Converts video duration or timestamp from ISO 8601 or timestamp format to seconds.
        :param duration: Video duration in ISO 8601 format (e.g., PT1H2M3S) or timestamp (e.g., 2024-09-29T06:00).
        :return: Duration in seconds (float) or None if error occurs.
        """
        try:
            if duration.startswith('PT'):
                parsed_duration = parse_duration(duration)
                return parsed_duration.total_seconds()
            parsed_time = datetime.strptime(duration, "%Y-%m-%dT%H:%M")
            epoch_time = (parsed_time - datetime(1970, 1, 1)).total_seconds()
            return epoch_time
        except Exception as e:
            logger.error(f"Error parsing duration: {e} ❌")
            return None

    def format_trending_videos(self, data):
        """
        Convert data from JSON to DataFrame with necessary columns
        """
        if not data or 'items' not in data:
            logger.error("No data to format. 🙂‍↔️❌")
            return pd.DataFrame()

        videos_data = []
        for item in data['items']:
            print(item)
            video_data = {
                "video_id": self.clean_id(item['id']),
                "title": self.clean_string(item['snippet']['title']),
                "view_count": self.clean_numeric(item['statistics']['viewCount']),
                "like_count": self.clean_numeric(item['statistics'].get('likeCount', 'N/A')),
                "comment_count": self.clean_numeric(item['statistics'].get('commentCount', 'N/A')),
                "published_at": self.clean_string(item['snippet']['publishedAt']),
                "duration": YouTubeHelper.convert_duration(item['contentDetails']['duration']),
                "dimension": self.clean_string(item['contentDetails'].get('dimension', 'N/A')),
                "definition": self.clean_string(item['contentDetails'].get('definition', 'N/A')),
                "licensed_content": item['contentDetails'].get('licensedContent', False),
                "channel_id": self.clean_string(item['snippet']['channelId']),
                "channel_title": self.clean_string(item['snippet']['channelTitle']),
                "tags": self.clean_string(';'.join(item['snippet'].get('tags', []))),
                "category_id": self.clean_string(item['snippet'].get('categoryId', 'N/A')),
                "audio_language": self.clean_string(item['snippet'].get('defaultAudioLanguage', 'N/A')),
                "live_broadcast_content": self.clean_string(item['snippet'].get('liveBroadcastContent', 'normal'))
            }
            videos_data.append(video_data)
        df = pd.DataFrame(videos_data)

        df.fillna({
            'channel_id': 'Unknown',
            'tags': 'N/A',
            'audio_language': 'N/A'}, inplace=True)
        return df

    def format_channel_info_data(self, data):
        """
        Convert data from JSON to DataFrame with necessary columns, ensuring data cleaning.
        """
        channels_data = []
        for item in data['items']:
            print(item)
            channel_data = {
                "channel_id": self.clean_id(item['id']),
                "title": self.clean_string(item['snippet']['title']),
                "description": self.clean_string(item['snippet'].get('description', 'N/A')),  # Clean description
                "published_at": item['snippet']['publishedAt'],
                "view_count": self.clean_numeric(item['statistics']['viewCount']),  # Clean view_count
                "subscriber_count": self.clean_numeric(item['statistics'].get('subscriberCount', 'N/A')),
                # Clean subscriber_count
                "video_count": self.clean_numeric(item['statistics'].get('videoCount', 'N/A')),  # Clean video_count
                "country": self.clean_string(item['snippet'].get('country', 'N/A'))  # Clean country
            }
            channels_data.append(channel_data)

        df = pd.DataFrame(channels_data)

        df.fillna({
            'channel_id': 'Unknown',
            'title': 'Unknown',
            'description': 'N/A',
            'country': 'N/A',
            'subscriber_count': 0,
            'video_count': 0
        }, inplace=True)

        return df

    def format_search_videos(self, data):
        """
        Convert search data from JSON to DataFrame with necessary columns
        """
        search_results = []
        if not data or 'items' not in data:
            logger.error("No data to format. ❌")
            return pd.DataFrame()

        for item in data['items']:
            video_data = {
                "video_id": item['id']['videoId'],
                "title": item['snippet']['title'],
                "description": item['snippet']['description'],
                "published_at": item['snippet']['publishedAt'],
                "channel_id": item['snippet']['channelId'],
                "channel_title": item['snippet']['channelTitle'],
                "thumbnails": item['snippet']['thumbnails']['high']['url'],
                "liveBroadcastContent": item['snippet']['liveBroadcastContent']
            }
            search_results.append(video_data)
        df = pd.DataFrame(search_results)
        return df

    def format_categories_data(self, data):
        """
        Convert search data from JSON to DataFrame with necessary columns
        """
        print(data)
        categories = []
        if not data or 'items' not in data:
            logger.error("No data to format. ❌")
            return pd.DataFrame()

        for item in data['items']:
            category_id = item['id']
            category_title = item['snippet']['title']
            categories.append({'category_id': category_id, 'category_title': category_title})
        df = pd.DataFrame(categories)
        return df


