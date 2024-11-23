FROM docker.io/bitnami/spark:3.5.3

USER root

RUN apt-get update && \
    apt-get install -y \
    python3-dev \
    openjdk-17-jdk \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH

COPY --from=bitnami/spark:3.5.3 /opt/bitnami/spark $SPARK_HOME

# RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.6.jar -P $SPARK_HOME/jars/ && \
#     wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.547/aws-java-sdk-bundle-1.12.547.jar -P $SPARK_HOME/jars/

COPY ./jars/aws-java-sdk-bundle-1.12.*.jar $SPARK_HOME/jars/
COPY ./jars/hadoop-aws-3.3.4.jar $SPARK_HOME/jars/
COPY ./jars/delta-core_2.12-2.4.0.jar $SPARK_HOME/jars/
COPY ./jars/delta-storage-2.3.0.jar $SPARK_HOME/jars/
COPY ./jars/mysql-connector-j-9.1.0.jar $SPARK_HOME/jars/
COPY ./jars/hadoop-common-3.3.4.jar $SPARK_HOME/jars/

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt