from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Dict, Any
from common.file_manager.handle_scds import SCDHandler
from helper.logger import LoggerSimple
import json

logger = LoggerSimple.get_logger(__name__)


class BaseDeltaManager:
    def __init__(self, spark: SparkSession, source_system: str, database: str, table: str, bucket_name: str):
        """
        Initialize the BaseDeltaManager with required configurations.
        """
        self.spark = spark
        self.source_system = source_system
        self.database = database
        self.table = table
        self.bucket_name = bucket_name
        self.scd_handler = SCDHandler(spark)


    def save_data(self, df: DataFrame, output_config: Dict[str, Any], batch_size: Optional[int] = None):
        """
        Save data to Delta format using the SCDHandler.
        Args:
            df (DataFrame): The DataFrame to save.
            output_config (Dict[str, Any]): Configuration for SCD handling and write options.
            batch_size (Optional[int]): The number of rows to write in each batch (default: None for full DataFrame).
        """
        try:
            logger.info(
                f"Saving data with SCD type: {output_config.get('store_type', 'SCD1')}")

            if batch_size:
                logger.info(f"Writing data in batches of size: {batch_size}")
                for idx, batch_df in enumerate(df.randomSplit([batch_size] * (df.count() // batch_size + 1))):
                    logger.info(f"Writing batch {idx + 1}")
                    self.scd_handler.process(batch_df, output_config)
            else:
                self.scd_handler.process(df, output_config)

            logger.info("Data saved successfully to Delta format.")
        except Exception as e:
            logger.error(f"Failed to save data: {str(e)}")
            raise

    def load_data(self, filter_conditions: Optional[str] = None, partitions: Optional[Dict[str, str]] = None) -> Optional[DataFrame]:
        """
        Load data from the Delta table with optional filtering and partition pruning.
        Args:
            filter_conditions (str): SQL-style filter conditions for filtering the loaded data.
            partitions (Dict[str, str]): A dictionary of partition column-value pairs for pruning.

        Returns:
            DataFrame: The loaded DataFrame or None if the table does not exist.
        """
        try:
            data_path = self._get_data_path()
            if not DeltaTable.isDeltaTable(self.spark, data_path):
                logger.warning(f"No Delta table found at path: {data_path}")
                return None

            logger.info(f"Loading data from Delta table at: {data_path}")
            df = self.spark.read.format("delta").load(data_path)

            if partitions:
                partition_conditions = " AND ".join(
                    [f"{k} = '{v}'" for k, v in partitions.items()])
                logger.info(
                    f"Applying partition pruning: {partition_conditions}")
                df = df.filter(partition_conditions)

            if filter_conditions:
                logger.info(f"Applying filter conditions: {filter_conditions}")
                df = df.filter(filter_conditions)

            logger.info(f"Data loaded successfully with {df.count()} rows.")
            return df
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise

    def validate_schema(self, df: DataFrame, expected_schema: Dict[str, str]):
        """
        Validate the schema of the DataFrame against an expected schema.
        Args:
            df (DataFrame): The DataFrame to validate.
            expected_schema (Dict[str, str]): A dictionary defining expected column names and types.
        """
        try:
            for column, data_type in expected_schema.items():
                if column not in df.columns:
                    raise ValueError(
                        f"Column {column} is missing in the DataFrame.")
                if str(df.schema[column].dataType) != data_type:
                    raise ValueError(
                        f"Column {column} has mismatched type. Expected: {data_type}, Got: {df.schema[column].dataType}")
            logger.info("Schema validation passed.")
        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            raise

    def delete_data(self, delete_condition: str):
        """
        Delete data from the Delta table based on a condition.
        Args:
            delete_condition (str): SQL-style condition for deleting rows.
        """
        try:
            data_path = self._get_data_path()
            if not DeltaTable.isDeltaTable(self.spark, data_path):
                logger.warning(
                    f"No Delta table found at path: {data_path}. Cannot perform delete operation.")
                return

            delta_table = DeltaTable.forPath(self.spark, data_path)
            logger.info(
                f"Deleting data from Delta table where: {delete_condition}")
            delta_table.delete(delete_condition)
            logger.info("Data deletion completed.")
        except Exception as e:
            logger.error(f"Failed to delete data: {str(e)}")
            raise

    def get_table_version(self) -> int:
        """
        Retrieve the current version of the Delta table.
        """
        try:
            data_path = self._get_data_path()
            delta_table = DeltaTable.forPath(self.spark, data_path)
            version = delta_table.history(1).collect()[0].version
            logger.info(f"Delta table is currently at version: {version}")
            return version
        except Exception as e:
            logger.error(f"Failed to retrieve table version: {str(e)}")
            raise
