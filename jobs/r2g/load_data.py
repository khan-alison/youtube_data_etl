import json
import os
import sys
from common.spark_session import SparkSessionManager
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)
bucket_name = os.getenv("DATALAKE_BUCKET")


class R2GLoadData:
    def __init__(self, spark, control_file):
        self.spark = spark
        self.control_file = control_file

    def load_data(self):
        with open(self.control_file, 'r') as file:
            data = json.load(file)
        print(data['date'])
        print(data['batch_run_timestamp'])
        print(data['partitions'])
        file_paths = data['partitions']
        full_file_paths = [
            f"s3a://{bucket_name}/{file_path}" for file_path in file_paths
        ]
        logger.info(f"file_path {full_file_paths}")
        df = self.spark.read.csv(
            full_file_paths, header=True, inferSchema=True)
        df.show(20)


if __name__ == '__main__':
    # Get control file path from command-line argument
    control_file = sys.argv[1]
    spark = SparkSessionManager.get_session()
    loader = R2GLoadData(spark, control_file)
    loader.load_data()
