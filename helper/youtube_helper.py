class YouTubeHelper:
    @staticmethod
    def convert_duration(duration):
        """
        Chuyển đổi thời gian video từ định dạng ISO 8601 thành giây.
        :param duration: Thời gian video ở định dạng ISO 8601 (ví dụ: PT1H2M3S)
        :return: Thời gian tính bằng giây (float)
        """
        try:
            parsed_duration = isodate.parse_duration(duration)
            return parsed_duration.total_seconds()
        except Exception as e:
            print(f"Error parsing duration: {e}")
            return None

    @staticmethod
    def analyze_tags_and_category(video_data):
        """
        Phân tích tags và category của video từ dữ liệu video.
        :param video_data: Dữ liệu video từ API YouTube.
        :return: Một tuple chứa danh sách các tags và ID danh mục.
        """
        tags = video_data.get('snippet', {}).get('tags', [])
        category_id = video_data.get('snippet', {}).get('categoryId', 'N/A')
        print(f"Tags: {tags}, Category ID: {category_id}")
        return tags, category_id
