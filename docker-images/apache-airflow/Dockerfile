FROM apache/airflow:2.10.2-python3.12

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

COPY ./jars/aws-java-sdk-bundle-1.12.*.jar /opt/airflow/jars/
COPY ./jars/delta-core_2.12-2.4.0.jar /opt/airflow/jars/
COPY ./jars/delta-storage-2.3.0.jar /opt/airflow/jars/
COPY ./jars/hadoop-aws-3.3.4.jar /opt/airflow/jars/
COPY ./jars/hadoop-common-3.3.4.jar /opt/airflow/jars/


# Run when you don't have jars installed

# RUN curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.316/aws-java-sdk-bundle-1.12.316.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
#     && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar \
#     && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar \
#     && mv aws-java-sdk-bundle-1.12.316.jar /opt/airflow/jars/ \
#     && mv delta-core_2.12-2.4.0.jar /opt/airflow/jars/ \
#     && mv delta-storage-2.3.0.jar /opt/airflow/jars/ \
#     && mv hadoop-aws-3.3.4.jar /opt/airflow/jars/ \
#     && mv hadoop-common-3.3.4.jar /opt/airflow/jars/ \
#     && mv hadoop-auth-3.3.4.jar /opt/airflow/jars/




COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

ENV PATH="/home/airflow/.local/bin:${PATH}"