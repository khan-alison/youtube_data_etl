FROM apache/airflow:2.10.2

USER root

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y python3-dev openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

USER airflow

# Upgrade pip and install necessary Python packages
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    opentelemetry-api opentelemetry-instrumentation \
    apache-airflow apache-airflow-providers-google \
    apache-airflow-providers-apache-spark pyspark && \
    pip install --no-cache-dir -r requirements.txt --verbose

# Set the PATH to include the local bin directory where pip installs user packages
ENV PATH="/home/airflow/.local/bin:${PATH}"