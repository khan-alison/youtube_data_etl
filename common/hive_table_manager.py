from helper.logger import LoggerSimple
from pyhive import hive

logger = LoggerSimple.get_logger(__name__)


class HiveTableManager:
    def __init__(self):
        self.hive_conn = None

    def _get_connection(self) -> hive.Connection:
        """Create a new Hive connection if none exists"""
        if not self.hive_conn:
            self.hive_conn = hive.Connection(
                host='yt-hive-metastore',
                port=9083,
                username='admin'
            )
        return self.hive_conn

    def _close_connection(self):
        if self.hive_conn:
            try:
                self.hive_conn.close()
            except Exception as e:
                logger.error(f"Failed to close Hive connection: {str(e)}")
            finally:
                self.hive_conn = None

    def _verify_golden_data(self, s3_path: str) -> bool:
        """Verify that data exists in the golden zone"""
        try:
            with self._get_connection().cursor() as cursor:
                check_query = f"DESCRIBE FORMATTED `{s3_path}`"
                cursor.execute(check_query)
                return bool(cursor.fetchall())
        except Exception as e:
            logger.error(f"Error verifying golden data: {str(e)}")
            return False

    def create_and_repair_table(self, source_system, table_name, s3_path, schema_name=None):
        """Create table if not exists and repair using MSCK REPAIR TABLE"""
        try:
            schema_name = schema_name or f"{source_system}_golden"

            # Ensure connection is established
            conn = self._get_connection()
            cursor = conn.cursor()

            # Create schema if not exists
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`")
            logger.info(f"Ensured database exists: {schema_name}")

            # Create external table
            create_table_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS `{schema_name}`.`{table_name}` (
                example_column STRING
            )
            STORED AS PARQUET
            LOCATION '{s3_path}'
            """
            cursor.execute(create_table_sql)
            logger.info(f"Created/verified table: {schema_name}.{table_name}")

            # Run MSCK REPAIR TABLE
            cursor.execute(f"MSCK REPAIR TABLE `{schema_name}`.`{table_name}`")
            logger.info(f"Repaired table: {schema_name}.{table_name}")

            return True, f"Successfully created and repaired table {schema_name}.{table_name}"

        except Exception as e:
            logger.error(f"Error creating/repairing table: {str(e)}")
            return False, str(e)
        finally:
            self._close_connection()
