import argparse
import json
from common.spark_session import SparkSessionManager
from common.trino_table_manager import TrinoTableManager
from helper.logger import LoggerSimple
from typing import Dict, Any, Union


logger = LoggerSimple.get_logger(__name__)


def process_config(config: dict, trino_table_manager: TrinoTableManager) -> None:
    """
    Process a single configuration to write data to the target location and create/repair Trino tables.
    """
    try:
        logger.info(f"Processing config: {config}")
        dataframe_name = config.get('dataframe')
        table_path = config.get('path')
        schema = config.get('schema')
        partition_by = config.get('partition_by', [])
        store_type = config.get('store_type', 'SCD1')

        path_parts = table_path.rstrip('/').split('/')

        logger.info(f"path_parts {path_parts}")
        if not all([dataframe_name, table_path, schema]):
            raise ValueError(
                "Missing required configuration parameters.")
        table_name = table_path.rstrip('/').split('/')[-1]
        logger.info(f"Table name: {table_name}")
        if store_type == 'SCD4':
            base_path = '/'.join(path_parts[:-1])
            table_name = path_parts[-2]
            logger.info(f"Table name1: {table_name}")
            table_path = table_path.replace('/current', '')
        logger.info(
            f"Creating/repairing table {table_name} with path {table_path}")
        results = trino_table_manager.create_or_repair_table(
            table_name=table_name,
            schema=schema,
            location=table_path,
            partition_by=partition_by,
            store_type=store_type
        )

        if results:
            logger.info(f"Successfully processed table {table_name}")
        else:
            logger.warning(
                f"Table {table_name} processing completed with warnings.")
    except Exception as e:
        logger.error(f"Error processing config: {config}, error: {str(e)}")
        raise


def create_or_repair_table(configs_dict: Union[Dict, str]) -> None:
    """
    Process configurations to create or repair Trino tables.

    Args:
        configs_dict (Union[Dict, str]): Configuration dictionary or JSON string
    """
    try:
        # Handle both dictionary and string inputs
        if isinstance(configs_dict, str):
            configs = json.loads(configs_dict)
        else:
            configs = configs_dict

        logger.info(f"Processing configs: {configs}")
        trino_manager = TrinoTableManager()

        output_configs = configs.get('output_configs', [])
        if not output_configs:
            raise ValueError("No 'output_configs' provided in configs")

        for config in output_configs:
            logger.info(f"Config {config}")
            process_config(config, trino_manager)
    except Exception as e:
        logger.error(f"Error in create_or_repair_table: {str(e)}")
        raise
