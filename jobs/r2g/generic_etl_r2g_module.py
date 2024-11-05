from common.spark_session import SparkSessionManager
from common.config_manager import ConfigManager
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from minio import Minio
from dotenv import load_dotenv
from helper.logger import LoggerSimple
import os
import json

load_dotenv()
logger = LoggerSimple.get_logger(__name__)


class GenericETLTransformer:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def process_input_path(self):
        try:
            logger.info(json.dumps(self.config.get('input'), indent=4))
            input_config = self.config.get('input')
            data_format = input_config.get('format', 'csv')
            data_paths = input_config.get('path')
            options = input_config.get('options', {})
            cache_enabled = input_config.get('cache', False)
            persistence_level = input_config.get(
                'data_persistence', 'MEMORY_AND_DISK')
            selected_columns = input_config.get('select_columns', 'all')

            if data_format == 'csv':
                df = self.spark.read.options(**options).csv(data_paths)
            else:
                raise ValueError(
                    'Unsupported data format: {data_format}'.format)

            if selected_columns != 'all':
                df = df.selectExpr(*selected_columns)

            if cache_enabled:
                persistence_map = {
                    'MEMORY_ONLY': StorageLevel.MEMORY_ONLY,
                    'MEMORY_AND_DISK': StorageLevel.MEMORY_AND_DISK,
                    'DISK_ONLY': StorageLevel.DISK_ONLY,
                    'OFF_HEAP': StorageLevel.OFF_HEAP
                }
                persistance_level = persistence_map.get(
                    persistence_level, StorageLevel.MEMORY_AND_DISK)
                df = df.persist(persistance_level)
                logger.info(
                    f"Data cached with persistence level: {persistence_level}")

            logger.info(
                f"Data loaded successfully with schema: {df.schema}")
            df.show()
            return df
        except Exception as e:
            logger.error(f"Error processing input path: {str(e)}")
            raise

    def execute(self):
        try:
            self.process_input_path()

        except Exception as e:
            logger.error(f"Error in ETL execution: {str(e)}")
            raise


def transform_data(control_file_path: str, source_system: str, table_name: str, bucket_name: str):
    """Main transformation function"""
    try:
        logger.info(f"""
            Starting transformation:
                Control file: {control_file_path}
                Source system: {source_system}
                Table: {table_name}
                Bucket: {bucket_name}
                """)

        config_manager = ConfigManager(
            control_file_path=control_file_path,
            bucket_name=bucket_name
        )

        processed_config = config_manager.combine_config(
            source_system=source_system,
            table_name=table_name
        )

        spark = SparkSessionManager.get_session()
        executor = GenericETLTransformer(spark=spark, config=processed_config)
        executor.execute()

    except Exception as e:
        logger.error(f"Error in transform_data: {str(e)}")
        raise
    finally:
        SparkSessionManager.close_session()


if __name__ == '__main__':
    parser = ArgumentParser(description='ETL Transform Data')
    parser.add_argument('--control_file_path', type=str, required=True,
                        help='Path to control file in MinIO')
    parser.add_argument('--source_system', type=str, required=True,
                        help='Source system name (e.g., youtube)')
    parser.add_argument('--table_name', type=str, required=True,
                        help='Table name')
    parser.add_argument('--bucket_name', type=str, required=True,
                        help='MinIO bucket name')

    args = parser.parse_args()

    logger.info(f"Starting transformation with arguments: {args}")
    transform_data(
        control_file_path=args.control_file_path,
        source_system=args.source_system,
        table_name=args.table_name,
        bucket_name=args.bucket_name
    )
