

AWS_ACCESS_KEY = 'minioaws'
AWS_SECRET_KEY = 'minioaws'
AWS_S3_ENDPOINT = 'http://minio:9000'
AWS_BUCKET_NAME = 'lakehouse'


def spark_session(spark_session):

    spark = spark_session.builder \
        .appName('Ingest checkin table into bronze') \
        .master('spark://yt_spark_master:7077') \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)\
        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config('spark.sql.warehouse.dir', f's3a://{AWS_BUCKET_NAME}/')\
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')\
        .config('spark.driver.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:/opt/bitnami/spark/jars/s3-2.18.41.jar/opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar/opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar/opt/bitnami/spark/jars/delta-storage-2.2.0.jar')\
        .config('spark.executor.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:/opt/bitnami/spark/jars/s3-2.18.41.jar/opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar/opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar/opt/bitnami/spark/jars/delta-storage-2.2.0.jar')\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark