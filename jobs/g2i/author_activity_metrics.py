from common.spark_session import SparkSessionManager
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

class AuthorActivityMetricsJobs:
    def __init__(self, spark_session):
        self.spark = spark_session

    def read_input_data(self):
        logger.info("Reading tables from catalog. 🤖")
        self.authors_df = self.spark.table("youtube.authors")
        self.comment_threads_df = self.spark.table("youtube.comment_threads")
        self.replies_df = self.spark.table("youtube.replies")

        self.authors_df.show()
        self.comment_threads_df.show()
        self.replies_df.show()


def main():
    job = AuthorActivityMetricsJobs(spark_session)
    job.read_input_data()


if __name__ == "__main__":
    try:
        spark_session = SparkSessionManager.get_session()
        main(spark_session)
    except Exception as e:
        logger.error(f"Error in AuthorActivityMetricsJobs: {str(e)}")
        raise
    finally:
        SparkSessionManager.close_session()
