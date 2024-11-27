FROM apache/spark:3.5.3-scala2.12-java17-python3-r-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y \
    python3-dev \
    python3-pip \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./jars/aws-java-sdk-bundle-1.12.*.jar $SPARK_HOME/jars/
COPY ./jars/hadoop-aws-3.3.4.jar $SPARK_HOME/jars/
COPY ./jars/delta-spark_2.12-3.2.0.jar $SPARK_HOME/jars/
COPY ./jars/delta-storage-3.2.0.jar $SPARK_HOME/jars/
COPY ./jars/mysql-connector-j-9.1.0.jar $SPARK_HOME/jars/
COPY ./jars/hadoop-common-3.3.4.jar $SPARK_HOME/jars/
COPY ./jars/hive-exec-3.1.3.jar $SPARK_HOME/jars/
COPY ./jars/hive-common-3.1.3.jar $SPARK_HOME/jars/
COPY ./jars/hive-serde-3.1.3.jar $SPARK_HOME/jars/


ENV SPARK_HOME=/opt/spark
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

RUN mkdir -p /opt/spark/work-dir \
    && mkdir -p /opt/spark/logs

COPY requirements.txt /opt/spark/
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r /opt/spark/requirements.txt

RUN chmod -R 777 /opt/spark/work-dir \
    && chmod -R 777 /opt/spark/logs

WORKDIR /opt/spark/work-dir

USER 185