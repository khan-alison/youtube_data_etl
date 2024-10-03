from isodate import parse_duration
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

class YouTubeHelper:
    @staticmethod
    def convert_duration(duration):
        """
        Converts video duration from ISO 8601 format to seconds.
        :param duration: Video duration in ISO 8601 format (e.g., PT1H2M3S).
        :return: Duration in seconds (float).
        """
        try:
            parsed_duration = parse_duration(duration)
            return parsed_duration.total_seconds()
        except Exception as e:
            logger.error(f"Error parsing duration: {e} ❌")
            return None

    