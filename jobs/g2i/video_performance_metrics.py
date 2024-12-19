import json
from argparse import ArgumentParser
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from common.spark_session import SparkSessionManager
from helper.logger import LoggerSimple
from common.trino_table_manager import TrinoTableManager

logger = LoggerSimple.get_logger(__name__)


class VideoPerformanceMetricsJobs:
    def __init__(self, spark_session, dataset_name, config_path):
        self.spark = spark_session
        self.dataset_name = dataset_name
        self.config = self.load_config_path(config_path)
        self.trending_videos_df = None
        self.categories_df = None
        self.channels_df = None

    def load_config_path(self, config_path):
        """
        Load configuration from the provided path.
        """
        try:
            with open(config_path, 'r') as file:
                config = json.load(file)
                logger.info(
                    f"Configuration loaded from {config_path}:\n{json.dumps(config, indent=2)}")
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found at {config_path}")
            raise

    def read_input_data(self):
        """
        Read datasets based on the loaded configuration.
        """
        dependencies = self.config.get('dependencies', {})

        if not dependencies:
            raise ValueError(f"Configuration file not found at {config_path}.")

        trending_path = dependencies.get('trending_videos')
        categories_path = dependencies.get('categories')
        channels_path = dependencies.get('channels')

        if not trending_path or not categories_path or not channels_path:
            raise ValueError(
                "Missing one of required dependencies: trending videos, categories or channels.")

        self.trending_videos_df = self.spark.read.parquet(trending_path)
        self.trending_videos_df.printSchema()
        logger.info(
            f"Loaded trending_videos from {trending_path}, count: {self.trending_videos_df.count()}")

        self.categories_df = self.spark.read.parquet(categories_path)
        self.categories_df.printSchema()
        logger.info(
            f"Loaded categories from {categories_path}, count: {self.categories_df.count()}")

        self.channels_df = self.spark.read.parquet(channels_path)
        self.channels_df.printSchema()
        logger.info(
            f"Loaded channels from {channels_path}, count: {self.channels_df.count()}")

    def join_data(self):
        """
        Perform joins based on join_keys in the config.
        We'll broadcast categories_df to ensure a broadcast hash join is used.
        """
        validations = self.config.get('validations', {})
        join_keys = validations.get('join_keys', {})

        # Rename conflicting columns in categories DataFrame
        categories_renamed_df = self.categories_df.withColumnRenamed(
            "category_id", "categories_category_id")

        # Start with the trending_videos DataFrame
        final_df = self.trending_videos_df

        # Perform the join with categories
        final_df = final_df.join(
            F.broadcast(categories_renamed_df),
            final_df["category_id"] == categories_renamed_df["categories_category_id"],
            "inner"
        )

        # Select required columns from the joined DataFrame
        final_df = final_df.select(
            "video_id",
            "channel_id",  # From trending_videos
            "category_id",  # Keep category_id from trending_videos
            "title",
            "views",
            "likes",
            "comments",
            "engagement_rate",
            "year",
            F.col("category_name")  # From categories
        )

        return final_df

    def write_data_and_register_table(self, final_df):
        """
        Write the resulting DataFrame to the output path specified in config, then
        create or repair the table in Trino.
        """
        output_config = self.config.get('output', {})

        if not output_config:
            raise ValueError(
                "No configuration found in config.")

        output_path = output_config.get('path')
        if not output_path:
            raise ValueError("No output path specified in output config.")

        final_df.write.mode("overwrite").format("delta").save(output_path)
        logger.info(f"Data written to {output_path} successfully.")

        schema_info = []
        for field in final_df.schema.fields:
            field_info = {
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable
            }
            schema_info.append(field_info)

        partition_by = []
        store_type = "SCD1"

        table_name = self.dataset_name
        trino_manager = TrinoTableManager()
        trino_manager.create_or_repair_table(
            table_name=table_name,
            schema=schema_info,
            location=output_path,
            partition_by=partition_by,
            store_type=store_type
        )
        logger.info(f"Table {table_name} registered in Trino.")


def main(spark_session: SparkSession, dataset_name: str, config_path: str):
    job = VideoPerformanceMetricsJobs(spark_session, dataset_name, config_path)
    job.read_input_data()
    final_df = job.join_data()
    job.write_data_and_register_table(final_df)


if __name__ == "__main__":
    parser = ArgumentParser(description="Video performance metrics")
    parser.add_argument("--dataset_name", required=True, help="Dataset Name")
    parser.add_argument("--config_path", required=True,
                        help="Path to the configuration file")

    args = parser.parse_args()
    logger.info(
        f"Starting VideoPerformanceMetricsJobs for {args.dataset_name}")
    try:
        spark_session = SparkSessionManager.get_session()
        main(spark_session, args.dataset_name, args.config_path)
    except Exception as e:
        logger.error(
            f"Error in VideoPerformanceMetricsJobs: {str(e)}", exc_info=True)
        raise
    finally:
        SparkSessionManager.close_session()
