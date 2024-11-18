FROM apache/hive:4.0.0

USER root

# Install necessary packages
RUN apt-get update && \
    apt-get install -y wget netcat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./jars/mysql-connector-j-9.1.0.jar /opt/hive/lib/
COPY ./jars/hadoop-aws-3.3.4.jar /opt/hive/lib/
COPY ./jars/aws-java-sdk-bundle-1.12.316.jar /opt/hive/lib/
# Create necessary directories
RUN mkdir -p /opt/hive/conf

# Copy configuration files
COPY docker-images/hive-metastore/conf/metastore-site.xml /opt/hive/conf/
COPY docker-images/hive-metastore/scripts/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 9083
ENTRYPOINT ["/entrypoint.sh"]