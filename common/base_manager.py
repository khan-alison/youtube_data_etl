from minio import Minio
from io import BytesIO
import pandas as pd
import os
from abc import ABC, abstractmethod
from helper.logger import LoggerSimple
from dotenv import load_dotenv

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
            secure=False  # True if you use HTTPS
        )

    def ensure_bucket_exited(self):
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)
            logger.info(f'Bucket {self.bucket_name} created.')

    def save_data(self, data):
        self.ensure_bucket_exited()
        df = pd.DataFrame(data)
        csv_data = df.to_csv(index=False)
        csv_bytes = BytesIO(csv_data.encode('utf8'))
        self.minio_client.put_object(
            bucket_name=self.bucket_name,
            object_name=self.file_name,
            data=csv_bytes,
            length=len(csv_data),
            content_type='application/csv'
        )
        logger.info(f'Data saved to {self.bucket_name}/{self.file_name}.')

    def load_data(self):
        try:
            response = (self.minio_client.get_object(
                self.bucket_name, self.file_name))
            data = pd.read_csv(ByteIO(response.read()))
            logger.info(
                f'Data loaded from {self.bucket_name}/{self.file_name}.')
            return data
        except FileNotFoundError:
            logger.error(
                f'File {self.bucket_name}/{self.file_name} not found.')
            return None
