from common.youtube_fetcher import YoutubeFetcher
from common.base_manager import BaseCSVManager
from helper.youtube_helper import YouTubeHelper
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os
import argparse

load_dotenv()

developer_key = os.getenv("DEVELOPER_KEY")
bucket_name = os.getenv("DATALAKE_BUCKET")


class ChannelsInformationFetcher(YoutubeFetcher):
    def __init__(self, spark, data_manager, ids):
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(ids)
        }
        formatter = YouTubeHelper().format_channel_info_data

        super().__init__(spark=spark, data_manager=data_manager,
                         endpoint_name='channels', params=params, formatter=formatter)


def fetch_and_save_channels_information(current_date, batch_run_timestamp):
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

    trending_videos_data_manager = BaseCSVManager(
        spark=spark,
        source_system='youtube',
        database="trending",
        table="trending_videos",
        run_date=current_date,
        batch_run_id=batch_run_timestamp,
        bucket_name=bucket_name
    )
    trending_video_data = trending_videos_data_manager.load_data()
    print(f"trending_video_data {trending_video_data}")

    if trending_video_data is not None:

        channel_ids_rdd = trending_video_data.select(
            "channel_id").rdd.flatMap(lambda x: x)

        channel_ids = channel_ids_rdd.collect()

        unique_channel_ids = list(set(channel_ids))

        print(f"Unique channel IDs: {unique_channel_ids}")

        data_manager = BaseCSVManager(
            spark=spark,
            source_system='youtube',
            database="trending",
            table="channels_information",
            run_date=current_date,
            batch_run_id=batch_run_timestamp,
            bucket_name=bucket_name
        )

        executor = ChannelsInformationFetcher(spark=spark,
                                              data_manager=data_manager, ids=unique_channel_ids)
        executor.execute()
    else:
        print("No data found in trending_videos.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fetch and save YouTube channel information')
    parser.add_argument('--batch_run_timestamp', type=str,
                        required=True, help='The batch run ID for the job.')
    parser.add_argument('--current_date', type=str,
                        required=True, help='The date run the job.')
    args = parser.parse_args()
    fetch_and_save_channels_information(args.current_date, args.batch_run_timestamp)
