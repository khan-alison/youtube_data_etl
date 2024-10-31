FROM docker.io/bitnami/spark:3.5.3

USER root

RUN apt-get update && \
    apt-get install -y \
    python3-dev \
    openjdk-17-jdk \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download required JARs for S3/MinIO integration
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

# Copy Bitnami Spark
COPY --from=bitnami/spark:3.5.3 /opt/bitnami/spark $SPARK_HOME

# Download required JARs
RUN cd $SPARK_HOME/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.316/aws-java-sdk-bundle-1.12.316.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt