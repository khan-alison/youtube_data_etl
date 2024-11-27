from typing import Any, Dict
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class QualityRule(ABC):
    """
    Abstract base class for data quality rules.
    """

    def __init__(self, column: str, rule_type: str, parameters: Dict[str, Any]):
        self.column = column
        self.rule_type = rule_type
        self.parameters = parameters

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """
        Applies the rule to the given DataFrame and returns the result.
        """
        pass

    @staticmethod
    def from_config(config: Dict[str, Any]) -> 'QualityRule':
        """
        Creates a QualityRule instance from the given configuration.
        """
        rule_type = config.get('type')

        try:
            if rule_type == 'numeric':
                return NumericRule(
                    column=config['column'],
                    parameters=config
                )
            elif rule_type == 'string':
                return StringRule(
                    column=config['column'],
                    parameters=config
                )
            elif rule_type == 'timestamp':
                return TimestampRule(
                    column=config['column'],
                    parameters=config
                )
            else:
                raise ValueError(f"Unsupported rule type: {rule_type}")
        except Exception as e:
            logger.error(f"Failed to create QualityRule from config: {config}")
            raise


class NumericRule(QualityRule):
    def __init__(self, column: str, parameters: Dict[str, Any]):
        super().__init__(column, 'numeric', parameters)

    def apply(self, df: DataFrame) -> Dict[str, Any]:
        """Apply numeric validation rules"""
        result = {}
        try:
            if not self.parameters.get('null_allowed', True):
                null_count = df.filter(F.col(self.column).isNull()).count()
                result['null_count'] = null_count
                result['null_allowed'] = self.parameters.get('null_allowed')
                result['null_check_passed'] = null_count == 0
                if not result['null_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {null_count} null values.")

            if 'min_value' in self.parameters:
                min_value = self.parameters['min_value']
                below_min_count = df.filter(
                    F.col(self.column) < min_value).count()
                result['min_value'] = min_value
                result['below_min_count'] = below_min_count
                result['below_min_check_passed'] = below_min_count == 0
                if not result['below_min_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {below_min_count} values below {min_value}.")

            if 'max_value' in self.parameters:
                max_value = self.parameters['max_value']
                above_max_count = df.filter(
                    F.col(self.column) > max_value).count()
                result['max_value'] = max_value
                result['above_max_count'] = above_max_count
                result['max_value_check_passed'] = above_max_count == 0
                if not result['max_value_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {above_max_count} values above {max_value}")

            result['rule_passed'] = all(
                [
                    result.get('null_check_passed', True),
                    result.get('min_value_check_passed', True),
                    result.get('max_value_check_passed', True),
                ]
            )

            return {self.column: result}
        except Exception as e:
            logger.error(
                f"Error applying numeric rule to {self.column}: {str(e)}")
            raise


class StringRule(QualityRule):
    def __init__(self, column: str, parameters: Dict[str, Any]):
        super().__init__(column, 'string', parameters)

    def apply(self, df: DataFrame) -> Dict[str, Any]:
        """Apply string validation rules"""
        result = {}

        try:
            if not self.parameters.get('null_allowed', True):
                null_count = df.filter(F.col(self.column).isNull()).count()
                result['null_count'] = null_count
                result['null_allowed'] = self.parameters.get('null_allowed')
                result['null_check_passed'] = null_count == 0
                if not result['null_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {null_count} null values.")

            if self.parameters.get('unique', False):
                total_count = df.count()
                distinct_count = df.select(self.column).distinct().count()
                duplicate_count = total_count - distinct_count
                result['unique'] = self.parameters['unique']
                result['duplicate_count'] = duplicate_count
                result['unique_check_passed'] = duplicate_counter == 0
                if not result['unique_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {duplicate_count} duplicate values.")

            if 'pattern' in self.parameters:
                pattern = self.parameters['pattern']
                invalid_pattern_count = df.filter(
                    ~F.col(self.column).rlike(pattern) & F.col(
                        self.column).isNotNull()
                ).count()
                result['pattern'] = pattern
                result['invalid_pattern_count'] = invalid_pattern_count
                result['pattern_check_passed'] = invalid_pattern_count == 0
                if not result['pattern_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {invalid_pattern_count} values not matching pattern")

                result['rule_passed'] = all([
                    result.get('null_check_passed', True),
                    result.get('unique_check_passed', True),
                    result.get('pattern_check_passed', True)
                ])

            return {self.column: result}
        except Exception as e:
            logger.error(
                f"Error applying string rule to {self.column}: {str(e)}")
            raise


class TimestampRule(QualityRule):
    def __init__(self, column: str, parameters: Dict[str, Any]):
        super().__init__(column, 'timestamp', parameters)

    def apply(self, df: DataFrame) -> Dict[str, Any]:
        """
        Apply timestamp validation rules.
        """
        result = {}

        try:
            if not self.parameters.get('null_allowed', True):
                null_count = df.filter(F.col(self.column).isNull()).count()
                result['null_count'] = null_count
                result['null_allowed'] = self.parameters['null_allowed']
                result['null_check_passed'] = null_count == 0
                if not result['null_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {null_count} null values.")
            if 'max_age_days' in self.parameters:
                max_age_days = self.parameters['max_age_days']
                max_date = F.current_timestamp(
                ) - F.expr(f"INTERVAL {max_age_days} DAYS")
                old_records_count = df.filter(
                    F.col(self.column) < max_date).count()
                result['max_age_days'] = max_age_days
                result['old_records_count'] = old_records_count
                result['old_records_check_passed'] = old_records_count == 0
                if not result['old_records_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {old_records_count} records older than {max_age_days} days.")

            if not self.parameters.get('allow_future', False):
                future_count = df.filter(
                    F.col(self.column) > F.current_timestamp()).count()
                result['future_count'] = future_count
                result['future_check_passed'] = future_count == 0
                if not result['future_check_passed']:
                    logger.warning(
                        f"Column {self.column} has {future_count} future dates")

            result['rule_passed'] = all([
                result.get('null_check_passed', True),
                result.get('max_age_check_passed', True),
                result.get('future_check_passed', True)
            ])

            return {self.column: result}
        except Exception as e:
            logger.error(
                f"Error applying timestamp rule to {self.column}: {str(e)}")
            raise
