#!/bin/bash
set -e

# =============================================================================
# Netflix Streaming Pipeline - Startup Script
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# =============================================================================
# Configuration
# =============================================================================
KAFKA_BROKER="kafka:9092"
SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# Load .env file if exists
if [ -f "$PROJECT_DIR/.env" ]; then
    log_info "Loading environment from .env file..."
    set -a
    source "$PROJECT_DIR/.env"
    set +a
fi

# BigQuery settings (from .env or defaults)
export OUTPUT_MODE="${OUTPUT_MODE:-console}"
export GCP_PROJECT="${GCP_PROJECT:-}"
export BQ_DATASET="${BQ_DATASET:-netflix_analytics}"
export BQ_TABLE="${BQ_TABLE:-trending_by_country}"
export GCS_TEMP_BUCKET="${GCS_TEMP_BUCKET:-}"

# Add BigQuery connector (uses Direct Write API, no GCS needed)
if [ "$OUTPUT_MODE" = "bigquery" ]; then
    SPARK_PACKAGES="${SPARK_PACKAGES},com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0"
    log_info "BigQuery mode enabled - Project: $GCP_PROJECT (Direct Write API)"
fi

# =============================================================================
# Step 1: Start Docker Services
# =============================================================================
log_info "Starting Docker services..."
cd "$PROJECT_DIR"
docker compose up -d

# =============================================================================
# Step 2: Wait for Kafka to be ready
# =============================================================================
log_info "Waiting for Kafka to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
        log_info "Kafka is ready!"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    log_warn "Kafka not ready yet, retrying... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log_error "Kafka failed to start within timeout"
    exit 1
fi

# =============================================================================
# Step 3: Fix Spark Ivy cache permissions
# =============================================================================
log_info "Setting up Spark Ivy cache..."
docker exec --user root spark mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars 2>/dev/null || true
docker exec --user root spark chmod -R 777 /home/spark/.ivy2 2>/dev/null || true

# =============================================================================
# Step 4: Start Producer (background)
# =============================================================================
log_info "Starting Kafka Producer..."
cd "$PROJECT_DIR"

# Kill any existing producer process
pkill -f "producer.py" 2>/dev/null || true

# Start producer with uv in background
nohup uv run python workspace/producer.py > logs/producer.log 2>&1 &
PRODUCER_PID=$!
echo $PRODUCER_PID > logs/producer.pid
log_info "Producer started (PID: $PRODUCER_PID)"

# =============================================================================
# Step 5: Start Spark Streaming Job
# =============================================================================
log_info "Starting Spark Streaming Aggregator..."

# Create logs directory
mkdir -p "$PROJECT_DIR/logs"

# Build environment variables for Spark
SPARK_ENV=""
SPARK_ENV="$SPARK_ENV -e KAFKA_BROKER=$KAFKA_BROKER"
SPARK_ENV="$SPARK_ENV -e OUTPUT_MODE=$OUTPUT_MODE"

if [ "$OUTPUT_MODE" = "bigquery" ]; then
    SPARK_ENV="$SPARK_ENV -e GCP_PROJECT=$GCP_PROJECT"
    SPARK_ENV="$SPARK_ENV -e BQ_DATASET=$BQ_DATASET"
    SPARK_ENV="$SPARK_ENV -e BQ_TABLE=$BQ_TABLE"
    SPARK_ENV="$SPARK_ENV -e GCS_TEMP_BUCKET=$GCS_TEMP_BUCKET"
    SPARK_ENV="$SPARK_ENV -e GOOGLE_APPLICATION_CREDENTIALS=/opt/spark/conf/gcp-key.json"
fi

# Run Spark submit
docker exec -d --user root $SPARK_ENV spark \
    /opt/spark/bin/spark-submit \
    --packages "$SPARK_PACKAGES" \
    /opt/spark/work-dir/workspace/streaming_aggregator.py

log_info "Spark Streaming job started!"

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
log_info "Pipeline started successfully!"
echo "=============================================="
echo "  - Kafka UI:     http://localhost:9092"
echo "  - Spark UI:     http://localhost:4040"
echo "  - Producer log: $PROJECT_DIR/logs/producer.log"
echo ""
echo "To view Spark logs:"
echo "  docker logs -f spark"
echo ""
echo "To stop the pipeline:"
echo "  ./scripts/stop_all.sh"
echo "=============================================="
