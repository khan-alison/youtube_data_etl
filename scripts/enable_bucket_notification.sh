#!/bin/bash

# Function for logging
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if MinIO is ready
check_minio() {
    /usr/bin/mc alias set myminio http://yt-minio:9000 minio minio123 > /dev/null 2>&1
    return $?
}

# Wait for services to be ready
log "Waiting for services to be fully ready..."
sleep 30

# Wait for MinIO to be ready
until check_minio
do
    log "MinIO is not ready, please wait..."
    sleep 5
done

log "MinIO is ready."

# Configure Kafka notification
log "Configuring Kafka notification..."
/usr/bin/mc admin config set myminio notify_kafka:1 \
    brokers='kafka:9092' \
    topic='minio-events' \
    queue_dir='/tmp/kafka' \
    queue_limit='1000'

if [ $? -ne 0 ]; then
    log "Failed to configure Kafka notification"
    exit 1
fi

log "Waiting before restart..."
sleep 5

# Restart MinIO
log "Restarting MinIO..."
/usr/bin/mc admin service restart myminio

if [ $? -ne 0 ]; then
    log "Failed to restart MinIO"
    exit 1
fi

log "Waiting after restart..."
sleep 10

# Create bucket
log "Creating/checking bucket..."
/usr/bin/mc mb myminio/lakehouse 2>/dev/null || log "Bucket lakehouse already existed."

# Configure event notification
log "Configuring event notification..."
/usr/bin/mc event add myminio/lakehouse arn:minio:sqs::1:kafka \
    --event put \
    --suffix .json

if [ $? -ne 0 ]; then
    log "Failed to add event notification"
    exit 1
fi

log "Checking event configuration:"
/usr/bin/mc event list myminio/lakehouse

# Keep container running
tail -f /dev/null