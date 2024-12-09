from minio import Minio
from io import BytesIO
import pandas as pd
import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from helper.logger import LoggerSimple
import csv
from datetime import datetime
from contextlib import contextmanager
from pyspark.sql.types import StructType
import json

load_dotenv()


logger = LoggerSimple.get_logger(__name__)


class BaseManager(ABC):
    def __init__(self, spark, source_system, database, table, run_date, batch_run_id, bucket_name):
        self.spark = spark
        self.source_system = source_system
        self.database = database
        self.table = table
        self.run_date = run_date
        self.batch_run_id = batch_run_id
        self.bucket_name = bucket_name

    @abstractmethod
    def save_data(self):
        raise NotImplementedError

    @abstractmethod
    def load_data(self):
        raise NotImplementedError


class BaseCSVManager(BaseManager):
    def __init__(self, spark, source_system, database, table, run_date, batch_run_id, bucket_name):
        super().__init__(spark, source_system, database,
                         table, run_date, batch_run_id, bucket_name)
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )

    def ensure_bucket_exists(self):
        """
        Ensures the bucket exists, creates it if not.
        """
        try:
            logger.info(f"Checking if bucket {self.bucket_name} exists...")
            if not self.minio_client.bucket_exists(self.bucket_name):
                logger.info(
                    f"Bucket {self.bucket_name} not found. Creating bucket...")
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f'Bucket {self.bucket_name} created.')
            else:
                logger.info(f"Bucket {self.bucket_name} already exists.")
        except Exception as e:
            logger.error(f"Error checking or creating bucket: {str(e)}")
            raise

    def save_data(self, spark_df):
        """
        Save the given Spark DataFrame as CSV to MinIO.
        """
        try:
            self.ensure_bucket_exists()
            folder_path = f"{self.source_system}/{self.database}/{self.table}/data/date_{self.run_date}/batch_run_timestamp-{self.batch_run_id}"
            output_path = f"s3a://{self.bucket_name}/{folder_path}"
            print(f"Saving data to folder: {output_path}")
            spark_df.write.option("header", "true").option(
                "encoding", "UTF-8").csv(output_path, mode='overwrite')

            logger.info(f'Data saved to {output_path}')
        except Exception as e:
            logger.error(f"Failed to save data to MinIO: {str(e)}")
            raise

    def load_data(self, timestamp=None):
        """
        Load data based on paths from the metadata control file.
        """
        try:
            metadata_path = f"{self.source_system}/{self.database}/{self.table}/metadata/date_{self.run_date}/batch_run_timestamp-{self.batch_run_id}/control_file.json"
            logger.info(f"Loading metadata from {metadata_path}")
            metadata_object = self.minio_client.get_object(
                self.bucket_name, metadata_path)
            metadata_content = metadata_object.read()\
                .decode('utf-8')
            metadata_json = json.loads(metadata_content)

            file_paths = metadata_json.get('partitions')

            if not file_paths:
                logger.error("No file paths found in control_file")

            logger.info(f"Found file paths: {file_paths}")
            full_file_paths = [
                f"s3a://{self.bucket_name}/{file_path}" for file_path in file_paths]

            logger.info("Reading data from CSV files using Spark...")
            df = self.spark.read.csv(
                full_file_paths, header=True, inferSchema=True)
            logger.info(f"Data loaded successfully from files: {file_paths}")
            return df

        except Exception as e:
            logger.error(f"Error loading data from MinIO: {str(e)}")
            raise


class BaseDeltaManager(BaseManager):
    def __init__(self, spark, source_system, database, table, run_date, batch_run_id, bucket_name):
        super().__init__(spark, source_system, database,
                         table, run_date, batch_run_id, bucket_name)
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )

    def ensure_bucket_exists(self):
        """
        Ensures the bucket exists, creates it if not.
        """
        try:
            logger.info(f"Checking if bucket {self.bucket_name} exists...")
            if not self.minio_client.bucket_exists(self.bucket_name):
                logger.info(
                    f"Bucket {self.bucket_name} not found. Creating bucket...")
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f'Bucket {self.bucket_name} created.')
            else:
                logger.info(f"Bucket {self.bucket_name} already exists.")
        except Exception as e:
            logger.error(f"Error checking or creating bucket: {str(e)}")
            raise

    def save_data(self, spark_df):
        """
        Sa
        """
