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

# Function to retry commands
retry_command() {
    local max_attempts=3
    local attempt=1
    local command="$@"
    local delay=5
    
    while [ $attempt -le $max_attempts ]; do
        log "Attempt $attempt of $max_attempts: $command"
        eval $command
        if [ $? -eq 0 ]; then
            return 0
        fi
        log "Command failed, waiting $delay seconds before retry..."
        sleep $delay
        attempt=$((attempt + 1))
    done
    return 1
}

# Main setup function
setup_minio() {
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
    retry_command "/usr/bin/mc admin config set myminio notify_kafka:1 brokers='kafka:9092' topic='minio-events' queue_dir='/tmp/kafka' queue_limit='1000'"
    
    log "Waiting before restart..."
    sleep 5

    # Restart MinIO
    log "Restarting MinIO..."
    retry_command "/usr/bin/mc admin service restart myminio"
    
    log "Waiting after restart..."
    sleep 10

    # Create bucket
    log "Creating/checking bucket..."
    retry_command "/usr/bin/mc mb myminio/lakehouse" 2>/dev/null || log "Bucket lakehouse already existed."

    # Reset notification configuration
    log "Resetting notification configuration..."
    retry_command "/usr/bin/mc admin config reset myminio notify_kafka"
    
    log "Restarting MinIO after reset..."
    retry_command "/usr/bin/mc admin service restart myminio"
    sleep 10

    # Remove all existing notifications
    log "Removing all existing notifications..."
    retry_command "/usr/bin/mc event remove myminio/lakehouse arn:minio:sqs::1:kafka --force" 2>/dev/null || true
    
    log "Verifying notifications are cleared:"
    /usr/bin/mc event list myminio/lakehouse

    # Configure new notification
    log "Adding new event notification..."
    retry_command "/usr/bin/mc event add myminio/lakehouse arn:minio:sqs::1:kafka --event put --suffix '.json'"
    
    # Verify configuration
    log "Final verification - Current event configuration:"
    /usr/bin/mc event list myminio/lakehouse
}

# Main execution loop
while true; do
    if ! setup_minio; then
        log "Setup failed, will retry in 30 seconds..."
        sleep 30
        continue
    fi
    
    log "Setup completed successfully. Container will continue running..."
    break
done

# Keep container running and handle signals properly
trap 'log "Received signal to terminate. Exiting..."; exit 0' SIGTERM SIGINT

# Keep container running
while true; do
    sleep 3600 &
    wait $!
done