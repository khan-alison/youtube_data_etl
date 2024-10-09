FROM apache/airflow:2.7.1

RUN pip install --upgrade pip
COPY requirements.txt .

USER airflow

# Install Python packages
RUN pip install --no-cache-dir --user \
    opentelemetry-api \
    opentelemetry-instrumentation \
    apache-airflow-providers-google

    RUN pip install --no-cache-dir --user -r requirements.txt --verbose

# Set the PATH to include the local bin directory where pip installs user packages
ENV PATH="/home/airflow/.local/bin:${PATH}"