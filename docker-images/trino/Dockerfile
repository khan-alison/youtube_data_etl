FROM trinodb/trino:latest

USER root

# Create necessary directories
RUN mkdir -p /etc/trino/catalog /opt/trino/plugin/hive/lib

# Copy configuration files
COPY docker-images/trino/etc/catalog/hive.properties /etc/trino/catalog/
COPY docker-images/trino/etc/config.properties /etc/trino/
COPY docker-images/trino/etc/jvm.config /etc/trino/
COPY docker-images/trino/etc/node.properties /etc/trino/
COPY docker-images/trino/etc/log.properties /etc/trino/

# Copy JAR files
COPY jars/aws-java-sdk-bundle-1.12.316.jar /opt/trino/plugin/hive/lib/
COPY jars/hadoop-aws-3.3.4.jar /opt/trino/plugin/hive/lib/
COPY jars/hadoop-common-3.3.4.jar /opt/trino/plugin/hive/lib/
COPY jars/hadoop-auth-3.3.4.jar /opt/trino/plugin/hive/lib/

# Set permissions
RUN chown -R trino:trino /etc/trino /opt/trino/plugin/hive/lib && \
    chmod -R 755 /etc/trino && \
    chmod 644 /etc/trino/catalog/hive.properties \
        /etc/trino/config.properties \
        /etc/trino/jvm.config \
        /etc/trino/node.properties \
        /etc/trino/log.properties \
        /opt/trino/plugin/hive/lib/*.jar

USER trino

EXPOSE 8080