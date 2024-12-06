from pathlib import Path
from minio import Minio
import json
from helper.logger import LoggerSimple
import os

logger = LoggerSimple.get_logger(__name__)


class ConfigManager:
    '''Manages configuration file handling and combining for ETL processes'''
    BASE_CONFIG_PATH = "/opt/airflow/job_entries/"

    def __init__(self, control_file_path: str, bucket_name: str, process_type: str):
        self.control_file_path = control_file_path
        self.bucket_name = bucket_name
        self.process_type = process_type
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

    def _read_config_file(self, table_name: str):
        """Read config file from local filesystem"""
        try:
            config_path = self.get_config_path(table_name, self.process_type)
            logger.info(f"Reading config file: {config_path}")
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading config file: {str(e)}")
            raise

    @classmethod
    def get_config_path(cls, table_name: str, process_type: str) -> str:
        """Get local config file path"""
        config_path = Path(cls.BASE_CONFIG_PATH) / \
            process_type / table_name / "config.json"
        logger.info(f"Config path: {str(config_path)}")
        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found at {config_path}")
        return str(config_path)

    def combine_config(self, table_name: str) -> dict:
        """Combine control file paths with config"""
        try:
            control_json = self._read_control_file()

            config = self._read_config_file(table_name)

            if self.process_type == 'r2g':
                data_file_paths = control_json.get('partitions', [])
                full_file_paths = [
                    f"s3a://{self.bucket_name}/{file_path}"
                    for file_path in data_file_paths
                ]
                config['input']['path'] = full_file_paths
                logger.info(
                    f"Combined R2G config created with {len(full_file_paths)} input paths")
            elif self.process_type == 'g2i':
                logger.info("Combined G2I config created")
            else:
                raise ValueError(f"Invalid process type: {self.process_type}")

            return config

        except Exception as e:
            logger.error(f"Error combining config for {table_name}: {str(e)}")
            raise
