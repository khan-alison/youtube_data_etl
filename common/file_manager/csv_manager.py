from minio import Minio
from pyspark.sql import DataFrame, SparkSession
import os
import json
from typing import Generator
from helper.logger import LoggerSimple
from abc import ABC, abstractmethod
from common.file_manager.base_manager import BaseManager

logger = LoggerSimple.get_logger(__name__)


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
        self.metadata_cache = None

    def _load_metadata(self) -> dict:
        """
        Load and cache the metadata control file for the current batch.
        """
        if self.metadata_cache == None:
            try:
                metadata_path = (
                    f"{self.source_system}/{self.database}/{self.table}/metadata/"
                    f"date_{self.run_date}/batch_run_timestamp-{self.batch_run_id}/control_file.json"
                )
                logger.info(f"Fetching metadata from: {metadata_path}")
                metadata_object = self.minio_client.get_object(
                    self.bucket_name, metadata_path)
                self.metadata_cache = json. \
                    loads(metadata_object.read().decode("utf-8"))
            except Exception as e:
                logger.error(f"Failed to load metadata: {str(e)}")
                raise RuntimeError("Failed to load metadata.")
        return self.metadata_cache

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
            folder_path = (
                f"{self.source_system}/{self.database}/{self.table}/data/"
                f"date_{self.run_date}/batch_run_timestamp-{self.batch_run_id}"
            )
            output_path = f"s3a://{self.bucket_name}/{folder_path}"
            logger.info(f"Saving data to folder: {output_path}")
            spark_df.write.option("header", "true") \
                .option("encoding", "UTF-8") \
                    .csv(output_path, mode="overwrite")
            logger.info(f"Data saved to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save data to MinIO: {str(e)}")
            raise

    def load_data(self) -> DataFrame:
        """
        Load all data based on the paths from the metadata control file into a single DataFrame.
        """
        try:
            metadata = self._load_metadata()
            file_paths = metadata.get("partitions", [])

            if not file_paths:
                logger.error("No file paths found in the control file.")
                raise ValueError("No file paths available to load data.")

            full_file_paths = [
                f"s3a://{self.bucket_name}/{file}" for file in file_paths]

            df = self.spark.read.csv(
                full_file_paths, header=True, inferSchema=True)

            logger.info(
                f"Loaded {df.count()} rows from {len(file_paths)} files into a single DataFrame."
            )

            return df

        except Exception as e:
            logger.error(f"Error loading data from MinIO: {str(e)}")
            raise
