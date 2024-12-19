import json
from argparse import ArgumentParser
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from common.spark_session import SparkSessionManager
from helper.logger import LoggerSimple
from common.trino_table_manager import TrinoTableManager

logger = LoggerSimple.get_logger(__name__)


class RelatedVideosNetworkMetricsJobs:
    def __init__(self, spark_session, dataset_name, config_path):
        self.spark = spark_session
        self.dataset_name = dataset_name
        self.config = self.load_config_path(config_path)
        self.related_videos_df = None
        self.trending_videos_df = None
        self.categories_df = None

    def load_config_path(self, config_path):
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
        dependencies = self.config.get('dependencies', {})

        if not dependencies:
            raise ValueError(f"Configuration file not found at {config_path}.")

        related_videos_path = dependencies.get('related_videos')
        trending_videos_path = dependencies.get('trending_videos')
        categories_path = dependencies.get('categories')

        self.related_videos_df = self.spark.read.parquet(related_videos_path)
        self.related_videos_df.printSchema()
        logger.info(
            f"Loaded related_videos from {related_videos_path}, count: {self.related_videos_df.count()}")

        self.trending_videos_df = self.spark.read.parquet(trending_videos_path)
        self.trending_videos_df.printSchema()
        logger.info(
            f"Loaded trending_videos from {trending_videos_path}, count: {self.trending_videos_df.count()}")

        self.categories_df = self.spark.read.parquet(categories_path)
        self.categories_df.printSchema()
        logger.info(
            f"Loaded categories from {categories_path}, count: {self.categories_df.count()}")

    def join_data(self):
        related_videos_df = self.related_videos_df.alias("related_videos")
        trending_videos_df = self.trending_videos_df.alias("trending_videos")
        categories_df = self.categories_df.alias("categories")

        final_df = related_videos_df.join(
            trending_videos_df,
            related_videos_df["video_id"] == trending_videos_df["video_id"],
            "inner"
        ).join(
            F.broadcast(categories_df),
            related_videos_df["category_id"] == categories_df["category_id"],
            "inner"
        )

        final_df = final_df.select(
            F.col("related_videos.video_id").alias("related_video_id"),
            F.col("related_videos.title").alias("related_video_title"),
            F.col("related_videos.category_id").alias("related_category_id"),
            F.col("categories.category_name").alias("related_category_name"),
            F.col("trending_videos.video_id").alias("trending_video_id"),
            F.col("trending_videos.title").alias("trending_video_title"),
            F.col("trending_videos.views").alias("trending_video_views"),
            F.col("related_videos.channel_id").alias("channel_id")
        )

        return final_df

    def write_data_and_register_table(self, final_df):
        output_config = self.config.get('output', {})
        output_path = output_config.get('path')
        if not output_path:
            raise ValueError("No output path specified in output config.")

        final_df.write.mode("overwrite").format("delta").save(output_path)
        logger.info(f"Data written to {output_path} successfully.")

        schema_info = [{"name": f.name, "type": str(
            f.dataType), "nullable": f.nullable} for f in final_df.schema.fields]

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
    job = RelatedVideosNetworkMetricsJobs(
        spark_session, dataset_name, config_path)
    job.read_input_data()
    final_df = job.join_data()
    job.write_data_and_register_table(final_df)


if __name__ == "__main__":
    parser = ArgumentParser(description="Related Videos Network Metrics")
    parser.add_argument("--dataset_name", required=True, help="Dataset Name")
    parser.add_argument("--config_path", required=True,
                        help="Path to the configuration file")

    args = parser.parse_args()
    logger.info(
        f"Starting RelatedVideosNetworkMetricsJobs for {args.dataset_name}")
    try:
        spark_session = SparkSessionManager.get_session()
        main(spark_session, args.dataset_name, args.config_path)
    except Exception as e:
        logger.error(
            f"Error in RelatedVideosNetworkMetricsJobs: {str(e)}", exc_info=True)
        raise
    finally:
        SparkSessionManager.close_session()
