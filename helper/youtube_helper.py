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
        Loại bỏ khoảng trắng ở đầu và cuối, và thay thế các ký tự đặc biệt.
        """
        if isinstance(value, str):
            # Loại bỏ khoảng trắng và các ký tự không mong muốn
            value = value.strip()
            # Thay thế khoảng trắng nhiều thành một khoảng trắng
            value = re.sub(r'\s+', ' ', value)
        return value

    @staticmethod
    def clean_numeric(value):
        """
        Chuyển đổi giá trị thành số. Nếu không chuyển đổi được thì trả về 0.
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def convert_duration(duration):
        """
        Converts video duration or timestamp from ISO 8601 or timestamp format to seconds.
        :param duration: Video duration in ISO 8601 format (e.g., PT1H2M3S) or timestamp (e.g., 2024-09-29T06:00).
        :return: Duration in seconds (float) or None if error occurs.
        """
        try:
            # Kiểm tra xem chuỗi có phải là định dạng ISO 8601 cho duration không
            if duration.startswith('PT'):
                parsed_duration = parse_duration(duration)
                return parsed_duration.total_seconds()

            # Nếu không, giả sử đó là một timestamp và chuyển đổi thành giây
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
            # item = json.dumps(item, indent=4)
            print(json.dumps(item, indent=4))
            video_data = {
                "video_id": self.clean_string(item['id']),
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
                "audio_language": self.clean_string(item['snippet'].get('defaultAudioLanguage', 'N/A'))
            }
            videos_data.append(video_data)
        df = pd.DataFrame(videos_data)
        df = pd.DataFrame(videos_data)

    # Thay thế NaN bằng giá trị mặc định
        df.fillna({'channel_id': 'Unknown', 'tags': 'N/A',
                  'audio_language': 'N/A'}, inplace=True)

    # In kiểu dữ liệu để kiểm tra
        print(df.dtypes)
        return df

    def format_channel_info_data(self, data):
        """
        Convert data from JSON to DataFrame with necessary columns.
        """
        channels_data = []
        for item in data['items']:
            channel_data = {
                "channel_id": item['id'],
                "title": item['snippet']['title'],
                "description": item['snippet'].get('description', 'N/A'),
                "published_at": item['snippet']['publishedAt'],
                "view_count": item['statistics']['viewCount'],
                "subscriber_count": item['statistics'].get('subscriberCount', 'N/A'),
                "video_count": item['statistics'].get('videoCount', 'N/A'),
                "country": item['snippet'].get('country', 'N/A')
            }
            channels_data.append(channel_data)
        df = pd.DataFrame(channels_data)
        return df
