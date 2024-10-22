import argparse
from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from common.spark_session import SparkSessionManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, TimestampType
from helper.logger import LoggerSimple

load_dotenv()

logger = LoggerSimple.get_logger(__name__)

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class TrendingVideosFetcher(YoutubeFetcher):
    def __init__(self, spark, data_manager):
        params = {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": "US",
            "maxResults": 20
        }

        formatter = YouTubeHelper().format_trending_videos
        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='videos', params=params, formatter=formatter)


def fetch_and_save_trending_videos(current_date, batch_run_timestamp):
    try:

        spark = SparkSession.builder \
            .appName('Extract trending videos data. 📈') \
            .master('local[*]') \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true")\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config('spark.sql.warehouse.dir', f's3a://{os.getenv("DATALAKE_BUCKET")}/')\
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
            .config('spark.jars.packages', 'com.amazonaws:aws-java-sdk-bundle:1.12.367') \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config('spark.driver.extraClassPath', '/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/s3-2.18.41.jar:/opt/spark/jars/aws-java-sdk-1.12.367.jar:/opt/spark/jars/delta-core_2.12-2.3.0.jar:/opt/spark/jars/delta-storage-2.2.0.jar')\
            .config('spark.executor.extraClassPath', '/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/s3-2.18.41.jar:/opt/spark/jars/aws-java-sdk-1.12.367.jar:/opt/spark/jars/delta-core_2.12-2.3.0.jar:/opt/spark/jars/delta-storage-2.2.0.jar')\
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .enableHiveSupport()\
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database="trending",
            table="trending_videos",
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name
        )
        executor = TrendingVideosFetcher(
            spark=spark, data_manager=data_manager)

        executor.execute()

    except Exception as e:
        logger.error(f"Error in fetching or saving trending videos: {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fetch and save trending YouTube videos.')
    parser.add_argument('--batch_run_timestamp', type=str,
                        required=True, help='The batch run ID for the job.')
    parser.add_argument('--current_date', type=str,
                        required=True, help='The date run the job.')
    args = parser.parse_args()
    fetch_and_save_trending_videos(args.current_date, args.batch_run_timestamp)
