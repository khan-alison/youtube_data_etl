FROM apache/hive:4.0.0

USER root

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget netcat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./jars/mysql-connector-j-9.1.0.jar /opt/hive/lib/
COPY ./jars/mysql-connector-j-9.1.0.jar /opt/apache-hive-metastore-3.0.0-bin/lib/
# Create necessary directories
RUN mkdir -p /opt/hive/conf

# Copy configuration files
COPY docker-images/hive-metastore/conf/metastore-site.xml /opt/hive/conf/
COPY docker-images/hive-metastore/scripts/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 9083
ENTRYPOINT ["/entrypoint.sh"]