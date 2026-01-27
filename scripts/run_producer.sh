#!/bin/bash
# =============================================================================
# Run Kafka Producer (for school server / on-premise)
# =============================================================================
# Usage:
#   ./scripts/run_producer.sh <EC2-IP>
#   ./scripts/run_producer.sh 44.212.42.206
# =============================================================================

set -e

# Get EC2 IP from argument or environment variable
EC2_IP="${1:-$KAFKA_BROKER_HOST}"

if [ -z "$EC2_IP" ]; then
    echo "Error: EC2 IP not provided"
    echo ""
    echo "Usage:"
    echo "  ./scripts/run_producer.sh <EC2-PUBLIC-IP>"
    exit 1
fi

export KAFKA_BROKER="${EC2_IP}:29092"

echo "=============================================="
echo "Starting Netflix Event Producer"
echo "=============================================="
echo "Kafka Broker: $KAFKA_BROKER"
echo "=============================================="

uv run python workspace/producer.py
