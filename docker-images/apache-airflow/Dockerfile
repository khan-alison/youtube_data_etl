FROM apache/airflow:2.7.1-python3.11

USER root

# Install OpenJDK
RUN apt-get update && \
    apt-get install -y python3-dev openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

USER airflow

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir --user \
    opentelemetry-api \
    opentelemetry-instrumentation \
    apache-airflow-providers-google
RUN pip install --no-cache-dir --user -r requirements.txt --verbose

# Set the PATH to include the local bin directory where pip installs user packages
ENV PATH="/home/airflow/.local/bin:${PATH}"