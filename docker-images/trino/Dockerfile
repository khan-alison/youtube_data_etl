FROM trinodb/trino:461

USER root

# Install required packages
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# Create necessary directories
RUN mkdir -p /usr/lib/trino/plugin/hive

# Set permissions
RUN chown -R trino:trino /usr/lib/trino/plugin/hive

USER trino