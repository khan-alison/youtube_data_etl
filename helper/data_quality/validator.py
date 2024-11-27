from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from data_quality.quality_rule import QualityRule
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class DataQualityValidator:
    """
    Orchestrates the application of validation rules to DataFrames.
    """

    def __init__(self, rules: List[QualityRule]):
        self.rules = rules

    def validate_schema(self, df: DataFrame, expected_schema: DataFrame.schema) -> Dict[str, Any]:
        """
        Checks if the DataFrame's schema matches the expected schema.
        """
        try:
            result = {}
            df_fields = set((field.name, str(field.dataType))
                            for field in df.schema.fields)
            expected_fields = set((field.name, str(field.dataType))
                                  for field in expected_schema.fields)

            missing_fields = expected_fields - df_fields
            extra_fields = df_fields - expected_fields

            result['missing_fields'] = list(missing_fields)
            result['extra_fields'] = list(extra_fields)
            result['schema_match'] = len(
                missing_fields) == 0 and len(extra_fields) == 0

            if not result['schema_match']:
                logger.error(f"Schema mismatch detected:")
                if missing_fields:
                    logger.error(f"Missing fields: {missing_fields}")
                if extra_fields:
                    logger.error(f"Extra fields: {extra_fields}")

            return result
        except Exception as e:
            logger.error(f"Error in schema validation: {str(e)}")
            raise

    def validate_join_keys(
        self,
        df1: DataFrame,
        df2: DataFrame,
        key_columns: List[str],
        threshold: float = 0.1
    ) -> Dict[str, Any]:
        """
        Validates the integrity of join keys between two DataFrames.
        """
        try:
            result = {}

            # Get distinct keys from both DataFrames
            df1_keys = df1.select(key_columns).dropDuplicates()
            df2_keys = df2.select(key_columns).dropDuplicates()

            df1_count = df1_keys.count()
            df2_count = df2_keys.count()

            # Find orphaned keys in df1
            orphaned_df1 = df1_keys.join(
                df2_keys,
                on=key_columns,
                how='left_anti'
            )
            orphaned_df1_count = orphaned_df1.count()

            # Find orphaned keys in df2
            orphaned_df2 = df2_keys.join(
                df1_keys,
                on=key_columns,
                how='left_anti'
            )
            orphaned_df2_count = orphaned_df2.count()

            df1_orphaned_ratio = orphaned_df1_count / df1_count if df1_count > 0 else 0
            df2_orphaned_ratio = orphaned_df2_count / df2_count if df2_count > 0 else 0

            result.update({
                'df1_total_keys': df1_count,
                'df2_total_keys': df2_count,
                'df1_orphaned_keys': orphaned_df1_count,
                'df2_orphaned_keys': orphaned_df2_count,
                'df1_orphaned_ratio': df1_orphaned_ratio,
                'df2_orphaned_ratio': df2_orphaned_ratio,
                'threshold': threshold,
                'join_keys_valid': max(df1_orphaned_ratio, df2_orphaned_ratio) <= threshold
            })

            # Log orphaned keys if threshold is exceeded
            if not result['join_keys_valid']:
                logger.warning(
                    f"Join key validation failed. "
                    f"DF1 orphaned ratio: {df1_orphaned_ratio:.2%}, "
                    f"DF2 orphaned ratio: {df2_orphaned_ratio:.2%}, "
                    f"Threshold: {threshold:.2%}"
                )

                # Log sample of orphaned keys for debugging
                if orphaned_df1_count > 0:
                    sample_orphaned_df1 = orphaned_df1.limit(5).collect()
                    logger.warning(
                        f"Sample orphaned keys from DF1: {sample_orphaned_df1}")
                if orphaned_df2_count > 0:
                    sample_orphaned_df2 = orphaned_df2.limit(5).collect()
                    logger.warning(
                        f"Sample orphaned keys from DF2: {sample_orphaned_df2}")

            return result
        except Exception as e:
            logger.error(f"Error in join key validation: {str(e)}")
            raise

    def run_validation(self, df: DataFrame) -> Dict[str, Any]:
        """
        Applies all quality rules to the DataFrame and returns aggregated results.
        """
        try:
            results = {
                'validation_time': F.current_timestamp(),
                'table_name': df.schema.simpleString(),
                'row_count': df.count(),
                'column_results': {},
                'overall_pass': True
            }

            # Apply each rule
            for rule in self.rules:
                rule_result = rule.apply(df)
                results['column_results'].update(rule_result)

                # Update overall pass status
                column_passed = rule_result[rule.column].get(
                    'rule_passed', True)
                results['overall_pass'] &= column_passed

                if not column_passed:
                    logger.warning(
                        f"Validation failed for column: {rule.column}, "
                        f"Details: {rule_result[rule.column]}"
                    )

            # Add summary statistics
            results['summary'] = {
                'total_rules': len(self.rules),
                'passed_rules': sum(
                    1 for result in results['column_results'].values()
                    if result.get('rule_passed', True)
                ),
                'failed_rules': sum(
                    1 for result in results['column_results'].values()
                    if not result.get('rule_passed', True)
                )
            }

            return results
        except Exception as e:
            logger.error(f"Error running validations: {str(e)}")
            raise

    def log_results(self, results: Dict[str, Any]) -> None:
        """
        Logs the validation results for monitoring and alerting.
        """
        try:
            if results.get('overall_pass'):
                logger.info(
                    f"Data quality validation passed. "
                    f"Total rules: {results['summary']['total_rules']}, "
                    f"All rules passed."
                )
            else:
                logger.error(
                    f"Data quality validation failed. "
                    f"Total rules: {results['summary']['total_rules']}, "
                    f"Failed rules: {results['summary']['failed_rules']}"
                )

                # Log detailed results for failed validations
                for column, result in results.get('column_results', {}).items():
                    if not result.get('rule_passed', True):
                        logger.error(
                            f"Failed validation for column '{column}':")
                        for key, value in result.items():
                            if key != 'rule_passed' and isinstance(value, (int, float, str, bool)):
                                logger.error(f"  {key}: {value}")

            # Log schema validation results if present
            if 'schema_match' in results:
                if results['schema_match']:
                    logger.info("Schema validation passed")
                else:
                    logger.error("Schema validation failed")
                    if results.get('missing_fields'):
                        logger.error(
                            f"Missing fields: {results['missing_fields']}")
                    if results.get('extra_fields'):
                        logger.error(
                            f"Extra fields: {results['extra_fields']}")

            # Log join validation results if present
            if 'join_keys_valid' in results:
                if results['join_keys_valid']:
                    logger.info("Join key validation passed")
                else:
                    logger.error(
                        f"Join key validation failed. "
                        f"Orphaned ratios - DF1: {results['df1_orphaned_ratio']:.2%}, "
                        f"DF2: {results['df2_orphaned_ratio']:.2%}"
                    )

        except Exception as e:
            logger.error(f"Error logging validation results: {str(e)}")
            raise

    def get_validation_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generates a summary of validation results suitable for reporting.
        """
        try:
            summary = {
                'validation_time': results.get('validation_time'),
                'table_name': results.get('table_name'),
                'row_count': results.get('row_count'),
                'overall_pass': results.get('overall_pass', False),
                'rules_summary': {
                    'total': results['summary']['total_rules'],
                    'passed': results['summary']['passed_rules'],
                    'failed': results['summary']['failed_rules']
                },
                'failed_columns': [
                    {
                        'column': col,
                        'details': res
                    }
                    for col, res in results.get('column_results', {}).items()
                    if not res.get('rule_passed', True)
                ]
            }

            return summary
        except Exception as e:
            logger.error(f"Error generating validation summary: {str(e)}")
            raise
