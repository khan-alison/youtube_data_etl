from pathlib import Path
from minio import Minio
import json
from helper.logger import LoggerSimple
import os

logger = LoggerSimple.get_logger(__name__)


class ConfigManager:
    '''Manages configuration file handling and combining for ETL processes'''
    BASE_CONFIG_PATH = "/opt/airflow/job_entries/"

    def __init__(self, control_file_path: str, bucket_name: str):
        self.control_file_path = control_file_path
        self.bucket_name = bucket_name
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )

    def _read_control_file(self):
        """Read control file from MinIO"""
        try:
            logger.info(f"Reading control file: {self.control_file_path}")
            control_file = self.minio_client.get_object(
                self.bucket_name,
                self.control_file_path
            )
            control_content = control_file.read().decode('utf-8')
            return json.loads(control_content)
        except Exception as e:
            logger.error(f"Error reading control file: {str(e)}")
            raise

    def _read_config_file(self, source_system: str, table_name: str):
        """Read config file from local filesystem"""
        try:
            config_path = self.get_config_path(source_system, table_name)
            logger.info(f"Reading config file: {config_path}")
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading config file: {str(e)}")
            raise

    @classmethod
    def get_config_path(cls, source_system: str, table_name: str) -> str:
        """Get local config file path"""
        config_path = Path(cls.BASE_CONFIG_PATH) / \
            source_system / table_name / "config.json"
        logger.info(f"Config path: {str(config_path)}")
        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found at {config_path}")
        return str(config_path)

    def combine_config(self, source_system: str, table_name: str) -> dict:
        """Combine control file paths with config"""
        try:
            control_json = self._read_control_file()

            config = self._read_config_file(source_system, table_name)

            data_file_paths = control_json.get('partitions', [])
            full_file_paths = [
                f"s3a://{self.bucket_name}/{file_path}"
                for file_path in data_file_paths
            ]

            config['input']['path'] = full_file_paths
            logger.info(
                f"Combined config created with {len(full_file_paths)} input paths")

            return config

        except Exception as e:
            logger.error(f"Error combining config for {table_name}: {str(e)}")
            raise
