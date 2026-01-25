# =============================================================================
# Netflix Streaming Analytics - Multi-stage Dockerfile
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Producer Image (Python + Kafka)
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS producer

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy application code
COPY workspace/producer.py ./workspace/
COPY data/ ./data/

# Set environment variables
ENV KAFKA_BROKER=kafka:9092
ENV PYTHONUNBUFFERED=1

# Run producer
CMD ["uv", "run", "python", "workspace/producer.py"]

# -----------------------------------------------------------------------------
# Stage 2: Spark Job Image (for submitting to existing Spark cluster)
# -----------------------------------------------------------------------------
FROM apache/spark:3.5.3-python3 AS spark-job

USER root

# Create directories for Ivy cache
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chmod -R 777 /home/spark/.ivy2

WORKDIR /opt/spark/work-dir

# Copy streaming aggregator
COPY workspace/streaming_aggregator.py ./workspace/

# Copy GCP credentials (mount at runtime for security)
# COPY credentials/gcp-service-account.json /opt/spark/conf/

# Environment variables (override at runtime with -e)
ENV KAFKA_BROKER=kafka:9092
# OUTPUT_MODE should be passed at runtime: -e OUTPUT_MODE=bigquery
ENV SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3

# Default command
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", \
     "/opt/spark/work-dir/workspace/streaming_aggregator.py"]

# -----------------------------------------------------------------------------
# Stage 3: Full Pipeline Image (Producer + Spark in one container for testing)
# -----------------------------------------------------------------------------
FROM apache/spark:3.5.3-python3 AS full-pipeline

USER root

# Install Python dependencies
RUN pip install --no-cache-dir kafka-python

# Create Ivy cache directories
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chmod -R 777 /home/spark/.ivy2

WORKDIR /opt/spark/work-dir

# Copy all workspace files
COPY workspace/ ./workspace/
COPY data/ ./data/

# Environment variables (override at runtime with -e)
ENV KAFKA_BROKER=kafka:9092
ENV PYTHONUNBUFFERED=1
# OUTPUT_MODE should be passed at runtime: -e OUTPUT_MODE=bigquery

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f "spark-submit" || exit 1

# Default: keep container running (use docker exec to run jobs)
CMD ["tail", "-f", "/dev/null"]
