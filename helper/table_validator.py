from typing import Dict, Any
from helper.logger import LoggerSimple
from dags.libs_dag.logging_manager import LoggingManager

logger = LoggerSimple.get_logger(__name__)


class TableValidator:
    def __init__(self):
        self.logging_manager = LoggingManager()

    def validate_dependencies(self, table_config: Dict[str, Any]) -> bool:
        """
        Validate if all required tables are ready
        """
        try:
            dependencies = table_config.get('dependencies', {})
            r2g_tables = dependencies.get('r2g_tables', {})

            for table in r2g_tables:
                status = self.logging_manager.get_dataset_status(table)
                if not status or status != 'SUCCESS':
                    logger.info(
                        f"Dependency table {table} not ready. Status: {status}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Failed to validate dependencies: {str(e)}")
            raise False
