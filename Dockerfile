FROM apache/airflow:2.10.1

USER root

# Install system-level dependencies if needed
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     <your-system-dependencies> \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir --user \
    opentelemetry-api \
    opentelemetry-instrumentation \
    apache-airflow-providers-google

# Set the PATH to include the local bin directory where pip installs user packages
ENV PATH="/home/airflow/.local/bin:${PATH}"