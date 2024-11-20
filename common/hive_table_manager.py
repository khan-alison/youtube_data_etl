from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class HiveTableManager:
    def __init__(self, spark, output_config):
        self.spark = spark
        self.output_config = output_config

    def map_spark_type_to_hive(self, spark_type):
        """
        Maps Spark data types to Hive data types.
        """
        type_mapping = {
            'string': 'STRING',
            'integer': 'INT',
            'long': 'BIGINT',
            'double': 'DOUBLE',
            'float': 'FLOAT',
            'boolean': 'BOOLEAN',
            'timestamp': 'TIMESTAMP',
            'date': 'DATE',
        }
        return type_mapping.get(spark_type.lower(), 'STRING')


def main(output_configs):
    print(f"output_configs {output_configs}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Hive table manager.")
    parser.add_argument("--output_configs", type=str, required=True)
    args = parser.parse_args()
    main(args.output_configs)
