import argparse
import json
from common.spark_session import SparkSessionManager
from common.hive_table_manager import HiveTableManager
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


def main(configs_str):
    spark = SparkSessionManager.get_session()
    try:
        configs = json.loads(configs_str) if configs_str else []
        logger.info(f"Processing configs: {configs}")

        if not configs:
            raise ValueError("No configs provided")

        for config in configs:
            logger.info(f"Config {config}")
    finally:
        SparkSessionManager.close_session()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--configs', type=str, required=True)
    args = parser.parse_args()
    main(args.configs)
