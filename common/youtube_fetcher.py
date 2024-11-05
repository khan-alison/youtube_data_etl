from pyspark.sql import SparkSession
from common.base_fetcher import BaseFetcher
import googleapiclient.discovery
import os
from dotenv import load_dotenv
from helper.logger import LoggerSimple
from contextlib import contextmanager
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")

logger = LoggerSimple.get_logger(__name__)


class YoutubeFetcher(BaseFetcher):
    """
    General class to fetch data from YouTube API.
    :param youtube: YouTube API client instance.
    :param data_manager: Object for handling data storage.
    :param endpoint: API endpoint (e.g., videos().list, channels().list).
    :param params: Parameters for the API request.
    :param formatter: Function for formatting the data.
    """

    def __init__(self, spark, data_manager, endpoint_name, params, formatter):
        super().__init__(data_manager)
        self.spark = spark
        self.youtube = googleapiclient.discovery.build(
            "youtube", "v3", developerKey=developer_key
        )
        self.endpoint = getattr(self.youtube, endpoint_name)()
        self.params = params
        self.formatter = formatter

    def fetch_data(self):
        """
        Calls the YouTube API to fetch data based on the endpoint and params.
        """
        logger.info("🌜Start fetching data from YouTube....")
        try:
            request = self.endpoint.list(**self.params)
            response = request.execute()
            return response
        except Exception as e:
            logger.error(f'💀Error fetching data: {str(e)}')
            return None

    def format_data(self, data):
        """
        Formats the data returned by the YouTube API into a pyspark DataFrame.
        """
        try:
            if not data or 'items' not in data or not data['items']:
                logger.info("No replies found, returning empty DataFrame. ✅")
                empty_schema = StructType([
                    StructField("video_id", StringType(), True),
                    StructField("comment_id", StringType(), True),
                    StructField("author", StringType(), True),
                    StructField("comment_text", StringType(), True),
                    StructField("like_count", IntegerType(), True)
                ])
                return self.spark.createDataFrame([], schema=empty_schema)

            if self.spark.sparkContext._jsc.sc().isStopped():
                raise Exception("SparkContext has already been stopped.")

            formatted_data = self.formatter(data)
            spark_df = self.spark.createDataFrame(formatted_data)
            print(f"spark_df {spark_df}")
            return spark_df
        except Exception as e:
            raise Exception(f'Error when formatting data. {str(e)}')

    def save_data(self, data):
        """
        Save the formatted data using the data manager.
        """
        formatted_data = self.format_data(data)
        if formatted_data.rdd.isEmpty():
            logger.error("No formatted data to save. ❌")
        else:
            logger.info("Saving formatted data...")
        self.data_manager.save_data(formatted_data)

    def execute(self):
        """
        Fetches, formats, and saves the data.
        """
        data = self.fetch_data()
        if data:
            self.save_data(data)
        else:
            logger.error("Failed to fetch any data. ❌")
