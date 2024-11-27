from helper.logger import LoggerSimple
import yaml
from typing import List, Dict, Any
from helper.data_quality.quality_rule import QualityRule

logger = LoggerSimple.get_logger(__name__)


class DataQualityConfigLoader:
    """
    Loads and parses data quality rules from configuration files.
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.rules = self.load_rules()

    def load_rules(self) -> Dict[str, List[QualityRule]]:
        """
        Loads the data quality rules from the specified configuration file.
        Returns a dictionary mapping table names to lists of QualityRule objects.
        """
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)

            table_rules = {}
            for table_name, table_config in config.items():
                rules = []
                for rule_config in table_config.get('rules', []):
                    rule = QualityRule.from_config(rule_config)
                    rules.append(rule)
                table_rules[table_name] = rules

            logger.info(
                f"Successfully loaded rules for {len(table_rules)} tables")
            return table_rules
        except Exception as e:
            logger.error(f"Error loading rules: {str(e)}")
            raise

    def get_table_rules(self, table_name: str) -> List[QualityRule]:
        """
        Retrieves the validation rules for a specific table.
        """
        rules = self.rules.get(table_name, [])
        logger.info(f"Retrieved {len(rules)} rules for table {table_name}")
        return rules
