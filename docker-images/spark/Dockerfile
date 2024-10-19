FROM docker.io/spark:latest

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y curl software-properties-common

# Add Python 3.12 repository and install Python 3.12
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.12 python3.12-dev python3.12-venv

# Set Python 3.12 as the default Python version for Spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=/usr/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.12

# Alias python3 to point to python3.12
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# Download necessary libraries and dependencies for Spark, S3, Delta Lake, and MySQL
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.2/hadoop-common-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.2/hadoop-auth-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar \
    && mv s3-2.18.41.jar /opt/spark/jars \
    && mv aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars \
    && mv delta-core_2.12-2.3.0.jar /opt/spark/jars \
    && mv delta-storage-2.3.0.jar /opt/spark/jars \
    && mv mysql-connector-java-8.0.19.jar /opt/spark/jars \
    && mv hadoop-aws-3.3.2.jar /opt/spark/jars \
    && mv hadoop-common-3.3.2.jar /opt/spark/jars \
    && mv hadoop-auth-3.3.2.jar /opt/spark/jars \
    && mv jackson-databind-2.15.2.jar /opt/spark/jars

# Ensure the necessary dependencies for Hadoop S3A support are available