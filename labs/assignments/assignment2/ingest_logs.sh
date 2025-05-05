#!/bin/bash

# Check if date parameter is provided
if [ -z "$1" ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

# Parse date
DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Validate date format (basic check)
if ! [[ $DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Invalid date format. Use YYYY-MM-DD."
    exit 1
fi

# Define local and HDFS paths
LOCAL_LOG_DIR="raw_data/${DATE}_logs.csv"
LOCAL_METADATA="raw_data/content_metadata.csv"
HDFS_LOG_DIR="/raw/logs/${YEAR}/${MONTH}/${DAY}"
HDFS_METADATA_DIR="/raw/metadata/${YEAR}/${MONTH}/${DAY}"

# Create HDFS directories
hdfs dfs -mkdir -p $HDFS_LOG_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Copy logs and metadata to HDFS
echo "Ingesting logs for $DATE into $HDFS_LOG_DIR..."
hdfs dfs -put -f $LOCAL_LOG_DIR $HDFS_LOG_DIR/
echo "Ingesting metadata into $HDFS_METADATA_DIR..."
hdfs dfs -put -f $LOCAL_METADATA $HDFS_METADATA_DIR/

echo "Ingestion complete for $DATE."
