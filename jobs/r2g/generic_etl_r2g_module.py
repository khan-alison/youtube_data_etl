import os
import json
from argparse import ArgumentParser
from typing import Dict, Any, Tuple, List
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from common.spark_session import SparkSessionManager
from common.config_manager import ConfigManager
from common.handle_scds import SCDHandler
from common.transformation_registry import TransformationRegistry
from common.registered_function import *
from helper.logger import LoggerSimple

load_dotenv()
logger = LoggerSimple.get_logger(__name__)


class GenericETLTransformer:
    """
    A generic ETL transformer that processes input data, applies transformations,
    and outputs the result based on a given configuration.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.transformation_registry = TransformationRegistry._transformations
        self.dataframes: Dict[str, DataFrame] = {}
        logger.info(
            f"Available transformations: {TransformationRegistry.list_transformations()}")

    def process_input(self) -> DataFrame:
        """
        Processes the input data based on the configuration.

        Returns:
            DataFrame: The loaded input DataFrame.
        """
        try:
            input_config = self.config.get('input', {})
            data_format = input_config.get('format', 'csv')
            data_paths = input_config.get('path')
            options = input_config.get('options', {})
            cache_enabled = input_config.get('cache', False)
            persistence_level = input_config.get(
                'data_persistence', 'MEMORY_AND_DISK')
            selected_columns = input_config.get('select_columns', 'all')

            logger.info(
                f"Processing input with configuration: {json.dumps(input_config, indent=4)}")

            if data_format == 'csv':
                df = self.spark.read.options(**options).csv(data_paths)
            else:
                raise ValueError(f"Unsupported data format: {data_format}")

            if selected_columns != 'all':
                df = df.select(*selected_columns)

            if cache_enabled:
                persistence_map = {
                    'MEMORY_ONLY': StorageLevel.MEMORY_ONLY,
                    'MEMORY_AND_DISK': StorageLevel.MEMORY_AND_DISK,
                    'DISK_ONLY': StorageLevel.DISK_ONLY,
                    'OFF_HEAP': StorageLevel.OFF_HEAP,
                }
                storage_level = persistence_map.get(
                    persistence_level, StorageLevel.MEMORY_AND_DISK)
                df = df.persist(storage_level)
                logger.info(
                    f"Data cached with persistence level: {persistence_level}")

            logger.info(f"Data loaded successfully with schema: {df.schema}")
            self.dataframes['input_df'] = df
            return df

        except Exception as e:
            logger.error(f"Error processing input data: {str(e)}")
            raise

    def process_transformation(self, df: DataFrame) -> DataFrame:
        """
        Applies a series of transformations to the input DataFrame.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        try:
            transformations = self.config.get('transformations', [])
            logger.info(f"Starting {len(transformations)} transformations.")

            current_df_name = 'input_df'

            for index, transform_config in enumerate(transformations, 1):
                name = transform_config.get('name')
                params = transform_config.get('params', {})
                output_names = transform_config.get('output_names', [])
                multi_output = transform_config.get('multi_output', False)
                target = transform_config.get('target', current_df_name)
                show_sample = transform_config.get('show_sample', False)

                logger.info(
                    f"Applying transformation {index}/{len(transformations)}: '{name}' to target '{target}'")
                logger.info(f"Parameters: {json.dumps(params, indent=2)}")

                current_df = self.dataframes.get(target)
                if current_df is None:
                    raise ValueError(f"Target DataFrame '{target}' not found.")

                transform_func = self.transformation_registry.get(name)
                if not transform_func:
                    raise ValueError(
                        f"Transformation function '{name}' not found in the registry.")

                result = transform_func(current_df, **params)

                if multi_output:
                    if not isinstance(result, tuple) or len(output_names) != len(result):
                        raise ValueError(
                            f"Transformation '{name}' expected to return multiple DataFrames matching 'output_names'."
                        )
                    for df_output, output_name in zip(result, output_names):
                        self.dataframes[output_name] = df_output
                        logger.info(
                            f"Stored DataFrame '{output_name}' from transformation '{name}'")
                    current_df_name = output_names[0]
                else:
                    if not isinstance(result, DataFrame):
                        raise ValueError(
                            f"Transformation '{name}' expected to return a DataFrame but got {type(result)}."
                        )
                    output_name = output_names[0] if output_names else target
                    self.dataframes[output_name] = result
                    current_df_name = output_name
                    logger.info(
                        f"Stored DataFrame '{output_name}' from transformation '{name}'")

                if multi_output:
                    for df_output, output_name in zip(result, output_names):
                        logger.info(
                            f"Schema of '{output_name}': {df_output.schema}")
                        if show_sample:
                            df_output.show(30, truncate=False)
                else:
                    logger.info(f"Schema of '{output_name}': {result.schema}")
                    if show_sample:
                        result.show(30, truncate=False)

            logger.info(
                f"All transformations completed successfully. Final DataFrame: '{current_df_name}'")
            return self.dataframes.get(current_df_name)

        except Exception as e:
            logger.error(f"Error in transformation processing: {str(e)}")
            raise

    def process_output(self):
        """
        Processes the output DataFrames using SCDHandler.
        """
        try:
            output_configs = []
            outputs_config = self.config.get('output', [])
            if not outputs_config:
                logger.info("No output configurations found.")
                return

            scd_handler = SCDHandler(self.spark)

            for output_config in outputs_config:
                df_name = output_config.get('dataframe', 'transformed_df')
                df_to_save = self.dataframes.get(df_name)
                if df_to_save is None:
                    logger.error(
                        f"DataFrame '{df_name}' not found for output.")
                    continue

                logger.info(
                    f"Processing output for DataFrame '{df_name}' with SCD type '{output_config.get('store_type', 'SCD1')}'")
                scd_handler.process(df_to_save, output_config)
                logger.info(
                    f"DataFrame '{df_name}' saved successfully to '{output_config['path']}'")
                output_config_with_schema = output_config.copy()
                # output_config_with_schema['schema'] = df_to_save.schema.jsonValue()
                output_configs.append(output_config_with_schema)
            return output_configs
        except Exception as e:
            logger.error(f"Error in output processing: {str(e)}")
            raise

    def execute(self) -> DataFrame:
        """
        Executes the ETL process: input processing, transformations, and output processing.

        Returns:
            DataFrame: The final transformed DataFrame.
        """
        try:
            df = self.process_input()
            self.process_transformation(df)
            return self.process_output()
        except Exception as e:
            logger.error(f"Error in ETL execution: {str(e)}")
            raise


def transform_data(control_file_path: str, source_system: str, table_name: str, bucket_name: str):
    """
    Main transformation function.

    Args:
        control_file_path (str): Path to the control file in MinIO.
        source_system (str): Source system name (e.g., 'youtube').
        table_name (str): Table name.
        bucket_name (str): MinIO bucket name.
    """
    try:
        logger.info(
            f"Starting transformation:\n"
            f"  Control file: {control_file_path}\n"
            f"  Source system: {source_system}\n"
            f"  Table: {table_name}\n"
            f"  Bucket: {bucket_name}\n"
        )

        config_manager = ConfigManager(
            control_file_path=control_file_path, bucket_name=bucket_name)
        processed_config = config_manager.combine_config(
            source_system=source_system, table_name=table_name)

        spark = SparkSessionManager.get_session()
        executor = GenericETLTransformer(spark=spark, config=processed_config)
        output_configs = executor.execute()

        conf = {
            "output_configs": output_configs
        }

        from airflow.api.client.local_client import Client
        import pendulum

        client = Client(None)
        client.trigger_dag(
            dag_id='orchestration_g2i_wrapper',
            conf=conf,
            run_id=pendulum.now().isoformat(),
        )
        return True
    except Exception as e:
        logger.error(f"Error in transform_data: {str(e)}")
        raise
    finally:
        SparkSessionManager.close_session()


if __name__ == '__main__':
    parser = ArgumentParser(description='ETL Transform Data')
    parser.add_argument('--control_file_path', type=str,
                        required=True, help='Path to control file in MinIO')
    parser.add_argument('--source_system', type=str, required=True,
                        help='Source system name (e.g., youtube)')
    parser.add_argument('--table_name', type=str,
                        required=True, help='Table name')
    parser.add_argument('--bucket_name', type=str,
                        required=True, help='MinIO bucket name')

    args = parser.parse_args()

    logger.info(f"Starting transformation with arguments: {args}")
    transform_data(
        control_file_path=args.control_file_path,
        source_system=args.source_system,
        table_name=args.table_name,
        bucket_name=args.bucket_name
    )
