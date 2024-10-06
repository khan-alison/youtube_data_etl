from minio import Minio
from io import BytesIO
import pandas as pd
import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from helper.logger import LoggerSimple
import csv

logger = LoggerSimple.get_logger(__name__)


class BaseManager(ABC):
    def __init__(self, file_name, bucket_name):
        self.file_name = file_name
        self.bucket_name = bucket_name

    @abstractmethod
    def save_data(self):
        raise NotImplementedError

    @abstractmethod
    def load_data(self):
        raise NotImplementedError


class BaseCSVManager(BaseManager):
    def __init__(self, file_name, bucket_name):
        super().__init__(file_name, bucket_name)
        load_dotenv()
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )

    def ensure_bucket_exited(self):
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)
            logger.info(f'Bucket {self.bucket_name} created.')

    def save_data(self, data):
        try:
            self.ensure_bucket_exited()
            df = pd.DataFrame(data)
            csv_data = df.to_csv(
                index=False,
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                escapechar='\\',
                sep=',',
                encoding='utf-8'
            )
            csv_bytes = BytesIO(csv_data.encode('utf8'))
            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=self.file_name,
                data=csv_bytes,
                length=len(csv_data),
                content_type='application/csv'
            )
            logger.info(f'Data saved to {self.bucket_name}/{self.file_name}.')
        except Exception as e:
            logger.error(f"Failed to save data to MinIO: {str(e)}")
            raise

    def load_data(self):
        try:
            logger.info(
                f"Loading data from {self.bucket_name}/{self.file_name}...")
            response = self.minio_client.get_object(
                self.bucket_name, self.file_name)
            csv_data = response.read()

            if not csv_data:
                logger.error(
                    f"Empty file found: {self.bucket_name}/{self.file_name}.")
                return None

            data = pd.read_csv(
                BytesIO(csv_data), on_bad_lines='skip',
                delimiter=',', quoting=csv.QUOTE_ALL,
                encoding='utf-8', lineterminator='\n')
            logger.info(
                f"Data loaded successfully from {self.bucket_name}/{self.file_name}.")
            return data

        except FileNotFoundError:
            logger.error(
                f'File {self.bucket_name}/{self.file_name} not found.❌')
            return None
        except pd.errors.ParserError as e:
            logger.error(f"ParserError: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"❌❌❌Error loading data from MinIO: {str(e)}")
            return None
