from common.spark_session import SparkSessionManager
from pyspark.sql import functions as F
import json
from helper.logger import LoggerSimple


logger = LoggerSimple.get_logger(__name__)


class VideoPerformanceMetricsJobs:
    def __init__(self, spark_session, dataset_name, config_path):
        self.spark = spark_session
        self.dataset_name = dataset_name
        self.config = self.load_config_path(config_path)

    def load_config_path(self, config_path):
        """
        Load configuration from the provided path.
        """
        try:
            with open(config_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {config_path}")
            raise

    def read_input_dataset(self):
        """
        Read datasets based on the loaded configuration
        """
        logger.info(
            "Reading datasets from the golden zone based on configuration.")
        dependencies = self.config['dependencies']
        logger.info(f"Dependencies {dependencies}")


def main(spark_session, dataset_name, config_path):
    job = VideoPerformanceMetricsJobs(
        spark_session,  dataset_name, config_path)
    job.read_input_data()


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Video performance metrics")
    parser.add_argument("--dataset_name", required=True,
                        help="Dataset Name")
    parser.add_argument("--config_path", required=True,
                        help="Path to the configuration file")
    args = parser.parse_args()
    try:
        spark_session = SparkSessionManager.get_session()
        main(spark_session, args.dataset_name, args.config_path)
    except Exception as e:
        logger.error(f"Error in VideoPerformanceMetricsJobs: {str(e)}")
        raise
    finally:
        SparkSessionManager.close_session()
