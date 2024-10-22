FROM docker.io/spark:latest

USER root

RUN apt-get update && apt-get install -y curl software-properties-common

RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.12 python3.12-dev python3.12-venv

ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=/usr/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.12

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar \
    && curl -O https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar \
    && mv s3-2.18.41.jar /opt/spark/jars/ \
    && mv aws-java-sdk-bundle-1.12.367.jar /opt/spark/jars/ \
    && mv delta-core_2.12-2.4.0.jar /opt/spark/jars/ \
    && mv delta-storage-2.3.0.jar /opt/spark/jars/ \
    && mv mysql-connector-java-8.0.19.jar /opt/spark/jars/ \
    && mv hadoop-aws-3.3.4.jar /opt/spark/jars/ \
    && mv hadoop-common-3.3.4.jar /opt/spark/jars/ \
    && mv hadoop-auth-3.3.4.jar /opt/spark/jars/ \
    && mv jackson-databind-2.15.2.jar /opt/spark/jars/
