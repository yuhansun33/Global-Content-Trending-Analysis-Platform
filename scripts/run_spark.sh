#!/bin/bash
# =============================================================================
# Run Spark Streaming Job (for AWS EC2)
# =============================================================================
# Usage:
#   ./scripts/run_spark.sh
# =============================================================================

set -e

echo "=============================================="
echo "Starting Spark Streaming Aggregator"
echo "=============================================="


docker exec spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /opt/spark/work-dir/workspace/streaming_aggregator.py
