from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os


class SparkSessionManager:
    _instance = None

    def __init__(self):
        if SparkSessionManager._instance is not None:
            raise Exception(
                "This class is a singleton! Use `get_session()` method instead.")
        load_dotenv()
        self.spark = self._create_spark_session()
        SparkSessionManager._instance = self

    @staticmethod
    def _create_spark_session():
        """Internal method to create a SparkSession with necessary configurations."""
        spark = SparkSession.builder \
            .appName('Spark Application') \
            .master('local[*]') \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config('spark.sql.warehouse.dir', f's3a://{os.getenv("DATALAKE_BUCKET")}/') \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.316") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        return spark

    @staticmethod
    def get_session():
        """Retrieve the active Spark session or create a new one if none exists."""
        if SparkSessionManager._instance is None:
            SparkSessionManager()
        return SparkSessionManager._instance.spark

    @staticmethod
    def close_session():
        """Close the Spark session and clean up resources."""
        if SparkSessionManager._instance is not None:
            SparkSessionManager._instance.spark.stop()
            SparkSessionManager._instance = None
            print("Spark session closed.")
