from pyspark.sql import SparkSession
import os


class SparkSessionManager:
    _spark_session = None

    @staticmethod
    def get_spark_session():
        if SparkSessionManager._spark_session is None or SparkSessionManager._spark_session.sparkContext._jsc.sc().isStopped():
            SparkSessionManager._spark_session = SparkSession.builder \
                .appName('Ingest checkin table into bronze') \
                .master('spark://spark-master:7077') \
                .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
                .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
                .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config('spark.sql.warehouse.dir', f's3a://{os.getenv("DATALAKE_BUCKET")}/') \
                .enableHiveSupport() \
                .getOrCreate()
        return SparkSessionManager._spark_session
