from minio import Minio
from io import BytesIO
import pandas as pd
import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip
from helper.logger import LoggerSimple
import csv
from datetime import datetime
from contextlib import contextmanager
from pyspark.sql.types import StructType

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
        self.spark = spark
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )

    def ensure_bucket_exited(self):
        print(os.getenv("MINIO_ENDPOINT"))
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
        print(spark_df)
        try:
            self.ensure_bucket_exited()
            current_date = datetime.now().strftime('%Y%m%d')
            folder_path = f"{self.source_system}/{self.database}/{self.table}/data/date={self.run_date}/batch_run_id={self.batch_run_id}"
            output_path = f"s3a://{self.bucket_name}/{folder_path}"
            print(f"Saving data to folder: {output_path}")
            spark_df.write.option("header", "true").option(
                "encoding", "UTF-8").csv(output_path, mode='overwrite')

            logger.info(f'Data saved to {output_path}')
        except Exception as e:
            logger.error(f"Failed to save data to MinIO: {str(e)}")
            raise

    def load_data(self, timestamp=None):
        try:
            if not timestamp:
                timestamp = self.get_latest_timestamp()
                if not timestamp:
                    logger.info(
                        f"No timestamp folders available for loading data.")
                    return None
            folder_path = f"{self.source_system}/{self.database}/{self.table}/data/date={self.run_date}/batch_run_id={self.batch_run_id}"
            file_path = f"s3a://{self.bucket_name}/{folder_path}/*.csv"
            logger.info(f"Loading data from {file_path}")

            df = self.spark.read.csv(file_path, header=True, inferSchema=True)  
            if df is None or df.rdd.isEmpty():
                logger.error(f"No data found at {file_path}.")
                return None

            logger.info(f"Data loaded successfully from {file_path}")
            return df

        except Exception as e:
            logger.error(f"Error loading data from MinIO: {str(e)}")
            return None