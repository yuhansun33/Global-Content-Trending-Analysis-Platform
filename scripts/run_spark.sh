#!/bin/bash
# =============================================================================
# Run Spark Streaming Job (for AWS EC2)
# =============================================================================
# Usage:
#   ./scripts/run_spark.sh           (auto-detect EC2 public IP)
#   ./scripts/run_spark.sh <IP>      (manual override)
# =============================================================================

set -e

# Get EC2 public IP: from argument, or auto-detect via EC2 metadata
EC2_IP="${1:-$(curl -s --max-time 5 http://169.254.169.254/latest/meta-data/public-ipv4)}"

if [ -z "$EC2_IP" ]; then
    echo "Error: Could not detect EC2 public IP"
    echo "Usage: ./scripts/run_spark.sh <EC2-PUBLIC-IP>"
    exit 1
fi

echo "=============================================="
echo "Starting Spark Streaming Aggregator"
echo "=============================================="
echo "EC2 Public IP: $EC2_IP"
echo "=============================================="

# Update KAFKA_EXTERNAL_HOST so Kafka advertises the correct address
export KAFKA_EXTERNAL_HOST="$EC2_IP"

# Restart Kafka to pick up the new advertised listener
echo "Restarting Kafka with KAFKA_EXTERNAL_HOST=$EC2_IP ..."
docker compose up -d kafka
echo "Waiting for Kafka to be ready..."
sleep 15

# Run Spark streaming job
docker exec spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
  /opt/spark/work-dir/workspace/streaming_aggregator.py
