from minio import Minio
from io import BytesIO
import pandas as pd
import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from helper.logger import LoggerSimple
import csv
from datetime import datetime

logger = LoggerSimple.get_logger(__name__)


class BaseManager(ABC):
    def __init__(self, folder_name, file_name, bucket_name):
        self.folder_name = folder_name
        self.file_name = file_name
        self.bucket_name = bucket_name

    @abstractmethod
    def save_data(self):
        raise NotImplementedError

    @abstractmethod
    def load_data(self):
        raise NotImplementedError


class BaseCSVManager(BaseManager):
    def __init__(self,folder_name, file_name, bucket_name):
        super().__init__(folder_name, file_name, bucket_name)
        load_dotenv()
        self.folder_name = folder_name
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
            )
            csv_bytes = BytesIO(csv_data.encode('utf8'))

            if self.folder_name:
                object_name = f"{self.folder_name}/{self.file_name}"
            else:
                object_name = self.file_name

            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=csv_bytes,
                length=len(csv_data),
                content_type='application/csv'
            )
            logger.info(f'Data saved to {self.bucket_name}/{object_name}.')
        except Exception as e:
            logger.error(f"Failed to save data to MinIO: {str(e)}")
            raise


    def get_latest_timestamp(self):
        """
        Get folder with timestamps from MinIO
        """
        try:
            objects = self.minio_client.list_objects(self.bucket_name, recursive=True)
            logger.info(f"oBJ: {objects}")
            timestamps = []
            for obj in objects:
                print(obj)
                folder_name = obj.object_name.split('/')[0]
                try:
                    datetime.strptime(folder_name, '%Y-%m-%d-%H-%M-%S')
                    timestamps.append(folder_name)
                except ValueError:
                    continue
            if not timestamps:
                logger.info("No timestamps found.")
                return None
            latest_timestamp = max(timestamps)
            return latest_timestamp
        except Exception as e:
            logger.error(f"Error getting latest timestamp: {str(e)}")
            return None

    def load_data(self, timestamp=None):
        try:
            logger.info(
                f"Loading data from {self.bucket_name}/{self.file_name}...")

            if not timestamp:
                timestamp = self.get_latest_timestamp()
                if not timestamp:
                    logger.info(
                        f"No timestamp folders available for loading data.")
                    return None
            file_path = f"{timestamp}/{self.file_name}"
            response = self.minio_client.get_object(
                self.bucket_name, file_path)
            csv_data = response.read()

            if not csv_data:
                logger.error(
                    f"Empty file found: {self.bucket_name}/{file_path}.")
                return None

            data = pd.read_csv(BytesIO(csv_data))
            logger.info(
                f"Data loaded successfully from {self.bucket_name}/{file_path}.")
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
