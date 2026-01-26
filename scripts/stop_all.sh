#!/bin/bash
set -e

# =============================================================================
# Netflix Streaming Pipeline - Shutdown Script
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# =============================================================================
# Step 1: Stop Producer
# =============================================================================
log_info "Stopping Kafka Producer..."

if [ -f "$PROJECT_DIR/logs/producer.pid" ]; then
    PRODUCER_PID=$(cat "$PROJECT_DIR/logs/producer.pid")
    if kill -0 "$PRODUCER_PID" 2>/dev/null; then
        kill "$PRODUCER_PID" 2>/dev/null || true
        log_info "Producer stopped (PID: $PRODUCER_PID)"
    else
        log_warn "Producer process not found"
    fi
    rm -f "$PROJECT_DIR/logs/producer.pid"
else
    # Try to kill by name
    pkill -f "producer.py" 2>/dev/null || log_warn "No producer process found"
fi

# =============================================================================
# Step 2: Stop Docker Containers
# =============================================================================
log_info "Stopping Docker containers..."
cd "$PROJECT_DIR"
docker compose down

# =============================================================================
# Step 3: Clean up (optional)
# =============================================================================
# Uncomment to remove volumes and clean up completely
# log_info "Removing Docker volumes..."
# docker compose down -v

# =============================================================================
# Step 4: Clean up UV background processes
# =============================================================================
log_info "Cleaning up background processes..."
pkill -f "uv run" 2>/dev/null || true

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=============================================="
log_info "Pipeline stopped successfully!"
echo "=============================================="
echo ""
echo "To restart the pipeline:"
echo "  ./scripts/run_all.sh"
echo ""
echo "To clean up Docker volumes:"
echo "  docker compose down -v"
echo "=============================================="
