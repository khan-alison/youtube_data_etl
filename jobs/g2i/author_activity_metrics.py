import json
from argparse import ArgumentParser
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from common.spark_session import SparkSessionManager
from helper.logger import LoggerSimple
from common.trino_table_manager import TrinoTableManager

logger = LoggerSimple.get_logger(__name__)


class AuthorActivityMetricsJobs:
    def __init__(self, spark_session, dataset_name, config_path):
        self.spark = spark_session
        self.dataset_name = dataset_name
        self.config = self.load_config_path(config_path)
        self.authors_df = None
        self.comment_threads_df = None
        self.replies_df = None

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

        authors_path = dependencies.get('authors')
        comment_threads_path = dependencies.get('comment_threads')
        replies_path = dependencies.get('replies')

        self.authors_df = self.spark.read.parquet(authors_path)
        self.authors_df.printSchema()
        logger.info(
            f"Loaded authors from {authors_path}, count: {self.authors_df.count()}")

        self.comment_threads_df = self.spark.read.parquet(comment_threads_path)
        self.comment_threads_df.printSchema()
        logger.info(
            f"Loaded comment_threads from {comment_threads_path}, count: {self.comment_threads_df.count()}")

        self.replies_df = self.spark.read.parquet(replies_path)
        self.replies_df.printSchema()
        logger.info(
            f"Loaded replies from {replies_path}, count: {self.replies_df.count()}")

    def join_data(self):
        comment_threads_df = self.comment_threads_df.alias("comment_threads")
        authors_df = self.authors_df.alias("authors")
        replies_df = self.replies_df.alias("replies")

        replies_renamed_df = replies_df.withColumnRenamed(
            "author_id", "reply_author_id")

        joined_df = comment_threads_df.join(
            authors_df,
            F.col("comment_threads.author_id") == F.col("authors.author_id"),
            "left"
        ).join(
            replies_renamed_df.alias("replies_renamed"),
            F.col("comment_threads.comment_thread_id") == F.col(
                "replies_renamed.comment_thread_id"),
            "left"
        )

        final_df = joined_df.select(
            F.col("comment_threads.comment_thread_id").alias(
                "comment_thread_id"),
            F.col("authors.author_name").alias("author_name"),
            F.col("comment_threads.comment_text").alias("comment_text"),
            F.col("comment_threads.comment_likes").alias("comment_likes"),
            F.col("comment_threads.total_reply_count").alias(
                "total_reply_count"),
            F.col("replies_renamed.reply_text").alias("reply_text"),
            F.col("replies_renamed.reply_likes").alias("reply_likes"),
            F.col("comment_threads.video_id").alias("video_id"),
            F.col("comment_threads.published_at").alias("published_at")
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
    job = AuthorActivityMetricsJobs(spark_session, dataset_name, config_path)
    job.read_input_data()
    final_df = job.join_data()
    job.write_data_and_register_table(final_df)


if __name__ == "__main__":
    parser = ArgumentParser(description="Author Activity Metrics")
    parser.add_argument("--dataset_name", required=True, help="Dataset Name")
    parser.add_argument("--config_path", required=True,
                        help="Path to the configuration file")

    args = parser.parse_args()
    logger.info(f"Starting AuthorActivityMetricsJobs for {args.dataset_name}")
    try:
        spark_session = SparkSessionManager.get_session()
        main(spark_session, args.dataset_name, args.config_path)
    except Exception as e:
        logger.error(
            f"Error in AuthorActivityMetricsJobs: {str(e)}", exc_info=True)
        raise
    finally:
        SparkSessionManager.close_session()
