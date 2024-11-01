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

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt