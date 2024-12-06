from common.spark_session import SparkSessionManager
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class VideoPerformanceMetricsJobs:
    def __init__(self, spark_session):
        self.spark = spark_session

    def read_input_data(self):
        logger.info("Reading tables from catalog. 🤖")
        self.trending_video_df = self.spark.table("youtube.trending_videos")
        self.categories_df = self.spark.table("youtube.categories")

        self.trending_video_df.show()
        self.categories_df.show()


def main():
    job = VideoPerformanceMetricsJobs(spark_session)
    job.read_input_data()


if __name__ == "__main__":
    try:
        spark_session = SparkSessionManager.get_session()
        main(spark_session)
    except Exception as e:
        logger.error(f"Error in VideoPerformanceMetricsJobs: {str(e)}")
        raise
    finally:
        SparkSessionManager.close_session()
