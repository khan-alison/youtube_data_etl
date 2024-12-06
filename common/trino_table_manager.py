from helper.logger import LoggerSimple
from typing import List, Dict, Any, Optional
import trino
import json
import os

logger = LoggerSimple.get_logger(__name__)


class TrinoTableManager:
    def __init__(self, host: str = 'yt-trino', port: str = '8080', user: str = "trino", catalog: str = "minio", schema: str = 'youtube'):
        """
        Initialize the TrinoTableManager with connection parameters
        """
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema

    def _get_connection(self):
        """
        Create and return a Trino connection
        """
        try:
            return trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema
            )
        except Exception as e:
            logger.error(f"Failed to connect to Trino: {str(e)}")
            raise e

    def _execute_query(self, query: str, fetch: bool = True) -> Optional[List[Any]]:
        """
        Execute a query and optionally fetch results.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            logger.info(f"Execution query: {query}")
            cursor.execute(query)

            if fetch:
                results = cursor.fetchall()
                return results
            return None
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise e
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    def _create_column_definitions(self, schema: List[Dict[str, str]], partition_by: Optional[List[str]] = None) -> str:
        """
        Convert schema definition to Trino column definitions, placing partition columns at the end.
        """
        type_mapping = {
            'StringType()': 'VARCHAR',
            'IntegerType()': 'INTEGER',
            'LongType()': 'BIGINT',
            'DoubleType()': 'DOUBLE',
            'BooleanType()': 'BOOLEAN',
            'TimestampType()': 'TIMESTAMP',
            'DataType()': 'DATE',
            'DecimalType()': 'DECIMAL',
        }

        partition_cols = set(partition_by) if partition_by else set()
        regular_columns = []
        partition_columns = []

        for column in schema:
            col_name = column['name']
            col_type = column['type']
            trino_type = type_mapping.get(col_type, 'VARCHAR')
            logger.info(f"Col {trino_type}")

            if col_name in partition_cols:
                partition_columns.append(f"{col_name} {trino_type}")
            else:
                regular_columns.append(f"{col_name} {trino_type}")

        all_columns = regular_columns + partition_columns
        return ',\n  '.join(all_columns)

    def schema_exists(self) -> bool:
        """
        Check if the schema exists in the catalog.
        """
        try:
            query = f"""
                SELECT table_name
                FROM {self.catalog}.information_schema.tables
                WHERE table_schema = '{self.schema}'
            """

            results = self._execute_query(query)
            return len(results) > 0
        except Exception as e:
            logger.error(f"Failed to check schema existence: {str(e)}")
            raise False

    def create_schema(self) -> bool:
        """
        Create the schema if it doesn't exist.
        """
        try:
            if not self.schema_exists():
                query = f"""
                    CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}
                """
                self._execute_query(query, fetch=False)
                logger.info(f"Schema '{self.schema}' created successfully!")
            else:
                logger.info(f"Schema '{self.schema}' already exists!")
            return True
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise False

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the current schema.
        """
        try:
            if not self.schema_exists():
                return False
            
            query = f"""
                SELECT table_name
                FROM {self.catalog}.information_schema.tables
                WHERE table_schema = '{self.schema}'
                AND table_name = '{table_name}'
            """

            results = self._execute_query(query)
            return len(results) > 0
        except Exception as e:
            logger.error(f"Failed to check table existence: {str(e)}")
            raise False

    def create_table(self, table_name: str, schema: List[Dict[str, str]], location: str, partition_by: Optional[List[str]] = None) -> bool:
        """
        Create a new table in Trino pointing to the specified location.
        """
        try:
            is_partitioned = bool(partition_by)
            if not self.schema_exists():
                self.create_schema()
                
            column_definition = self._create_column_definitions(
                schema, partition_by)
            if partition_by:
                partition_columns = ', '.join(
                    [f"'{col}'" for col in partition_by])
                partition_clause = f",\n    partitioned_by = ARRAY[{partition_columns}]"
            else:
                partition_clause = ""

            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.{table_name} (
                    {column_definition}
                )
                WITH (
                    external_location = '{location}',
                    format = 'PARQUET'{partition_clause}
                )
            """

            self._execute_query(create_table_query, fetch=False)
            logger.info(f"Successfully created table {table_name}")
            if is_partitioned:
                logger.info(f"Table {table_name} is partitioned. Running initial partition repair...")
                self.repair_table(table_name, is_partitioned)
            return True
        except Exception as e:
            logger.error(f"Failed to create table: {str(e)}")
            raise False

    def repair_table(self, table_name: str, is_partitioned: bool) -> bool:
        """
        Repair a table by synchronizing the metastore with the actual data.
        Only applicable for partitioned tables.
        """
        if not is_partitioned:
            logger.info(
                f"Table {table_name} is not partitioned. Skipping repair.")
            return True

        try:
            msck_repair_query = f"""
                CALL {self.catalog}.system.sync_partition_metadata(
                    schema_name => '{self.schema}',
                    table_name => '{table_name}',
                    mode => 'ADD'
                )
            """
            self._execute_query(msck_repair_query, fetch=False)
            logger.info(f"Successfully repaired table {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to repair table {table_name}: {str(e)}")
            raise Exception(f"Failed to repair table {table_name}: {str(e)}")

    def get_table_location(self, base_location: str, store_type: str) -> str:
        """
        Get the appropriate table location based on store type.
        For SCD4, points to the current directory.
        """
        return f"{base_location}/current" if store_type == 'SCD4' else base_location

    def create_or_repair_table(self, table_name: str, schema: List[Dict[str, str]], location: str, partition_by: Optional[List[str]] = None, store_type: str = 'SCD1') -> bool:
        """
        Create a table if it doesn't already exist, or repair it if it does.
        For SCD4, only handles the current table
        """
        try:
            is_partitioned = bool(partition_by)
            actual_location = self.get_table_location(location, store_type)

            logger.info(
                f"Processing table {table_name} at location {actual_location}.")
            logger.info(f"Successfully repaired table {table_name}.")

            if self.table_exists(table_name):
                logger.info(
                    f"Table {table_name} already exists. Repairing...")
                return self.repair_table(table_name, is_partitioned)
            else:
                logger.info(
                    f"Table {table_name} does not exist. Creating...")
                return self.create_table(table_name, schema, actual_location, partition_by)
        except Exception as e:
            logger.error(
                f"Failed to create or repair table {table_name}: {str(e)}")
            raise False
