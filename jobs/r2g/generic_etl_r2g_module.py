import os
import json
from argparse import ArgumentParser
from typing import Dict, Any, Tuple, List
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark import StorageLevel
from common.spark_session import SparkSessionManager
from common.config_manager import ConfigManager
from common.handle_scds import SCDHandler
from common.transformation_registry import TransformationRegistry
from common.registered_function import *
from helper.logger import LoggerSimple
from dags.libs_dag.logging_manager import LoggingManager, CreateGoldenDataset
from dags.libs_dag.dataset_creation_producer import DatasetCreationProducer
load_dotenv()
logger = LoggerSimple.get_logger(__name__)


class GenericETLTransformer:
    """
    A generic ETL transformer that processes input data, applies transformations,
    and outputs the result based on a given configuration.
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any], execution_id: str, task_run_id: str):
        self.spark = spark
        self.config = config
        self.execution_id = execution_id
        self.task_run_id = task_run_id
        self.transformation_registry = TransformationRegistry._transformations
        self.dataframes: Dict[str, DataFrame] = {}
        self.logging_manager = LoggingManager()
        self.dataset_run_ids = {}
        self.output_configs = None
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
            for dataset in self.dataframes:
                logger.info(
                    f"DataFrame '{dataset}' transformation completed successfully")
                dataset_run = CreateGoldenDataset(
                    execution_id=self.execution_id,
                    task_run_id=self.task_run_id,
                    task_id='create_delta_file_in_golden_zone',
                    dataset=dataset,
                    start_time=datetime.utcnow(),
                    status='RUNNING',
                )
                dataset_run_id = self.logging_manager.log_dataset_run(
                    dataset_run=dataset_run)
                self.dataset_run_ids[dataset] = dataset_run_id

            return self.dataframes.get(current_df_name)
        except Exception as e:
            logger.error(f"Error in transformation processing: {str(e)}")
            raise

    def process_output(self):
        """
        Processes the output DataFrames using SCDHandler.
        """
        try:
            outputs_config = self.config.get('output', [])
            if not outputs_config:
                logger.info("No output configurations found.")
                return
            output_configs_with_schema = []
            self.output_configs = outputs_config

            enhanced_outputs = []
            scd_handler = SCDHandler(self.spark)

            for output_config in outputs_config:
                df_name = output_config.get('dataframe', 'transformed_df')
                df_to_save = self.dataframes.get(df_name)
                if df_to_save is None:
                    logger.error(
                        f"DataFrame '{df_name}' not found for output.")
                    continue

                scd_handler.process(df_to_save, output_config)
                logger.info(
                    f"DataFrame '{df_name}' saved successfully to '{output_config['path']}'")

                with DatasetCreationProducer() as producer:
                    source_system = self.config.get(
                        'source_system', 'unknown_system')
                    database = self.config.get('database', 'unknown_database')
                    bucket_name = self.config.get(
                        'bucket_name', 'unknown_bucket')
                    config_file_path = self.config.get(
                        'config_file_path', 'unknown_config')
                    producer.send_dataset_completion_events(
                        dataset=df_name,
                        source_system=source_system,
                        database=database,
                        bucket_name=bucket_name,
                        config_file_path=config_file_path
                    )
                if df_name in self.dataset_run_ids:
                    dataset_run_id = self.dataset_run_ids[df_name]
                    self.logging_manager.update_dataset_run(
                        dataset_run_id=dataset_run_id, end_time=datetime.utcnow(), status='SUCCESS')
                    logger.info(
                        f"Updated table run {dataset_run_id} for '{df_name}' to SUCCESS")
            self.output_configs = enhanced_outputs
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
            self.process_output()

            result = {
                "output_configs": self.output_configs,
                "status": "success"
            }

            logger.info(f"ETL process completed successfully. {result}")

            dataset = self.config.get("dataset", "")
            logger.info(f"dataset: {dataset}")
            with open(f'/tmp/{dataset}_output.json', 'w') as f:
                json.dump(result, f)

            return result
        except Exception as e:
            logger.error(f"Error in ETL execution: {str(e)}")
            raise


def transform_data(control_file_path: str, source_system: str, table_name: str, bucket_name: str, execution_id: int, task_run_id: int):
    """
    Main transformation function.

    Args:
        control_file_path (str): Path to the control file in MinIO.
        source_system (str): Source system name (e.g., 'youtube').
        table_name (str): Table name.
        bucket_name (str): MinIO bucket name.
    """
    try:
        config_manager = ConfigManager(
            control_file_path=control_file_path,
            bucket_name=bucket_name,
            process_type='r2g'
        )
        processed_config = config_manager.combine_config(
            table_name=table_name,
        )

        spark = SparkSessionManager.get_session()
        executor = GenericETLTransformer(
            spark=spark, config=processed_config, execution_id=execution_id, task_run_id=task_run_id)
        output_configs = executor.execute()

        logger.info(json.dumps(output_configs))
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
    parser.add_argument('--execution_id', type=int,
                        required=True, help='Execution ID from Airflow')
    parser.add_argument('--task_run_id', type=int,
                        required=True, help='Task Run ID from Airflow')

    args = parser.parse_args()

    logger.info(f"Starting transformation with arguments: {args}")
    transform_data(
        control_file_path=args.control_file_path,
        source_system=args.source_system,
        table_name=args.table_name,
        bucket_name=args.bucket_name,
        execution_id=args.execution_id,
        task_run_id=args.task_run_id
    )
