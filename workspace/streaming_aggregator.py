import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    from_json,
    split,
    window,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Configuration from environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "viewing_events")
OUTPUT_MODE = os.getenv("OUTPUT_MODE", "console")  # "console" or "bigquery"

# BigQuery configuration
GCP_PROJECT = os.getenv("GCP_PROJECT", "")
BQ_DATASET = os.getenv("BQ_DATASET", "netflix_analytics")
BQ_TABLE = os.getenv("BQ_TABLE", "trending_by_country")
GCS_TEMP_BUCKET = os.getenv("GCS_TEMP_BUCKET", "")

# Schema matching the producer's event structure
EVENT_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("movie_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("watch_hours", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)


def create_spark_session():
    builder = SparkSession.builder.appName("NetflixTrendingAggregator").config(
        "spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false"
    )

    return builder.getOrCreate()


def write_to_bigquery(batch_df, batch_id):
    """Write micro-batch to BigQuery using Direct Write API."""
    gcp_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    row_count = batch_df.count()
    if row_count > 0:
        writer = (
            batch_df.write.format("bigquery")
            .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
            .option("credentialsFile", gcp_credentials)
            .option("writeMethod", "direct")
        )
        # Use GCS temp bucket if provided (for indirect write method fallback)
        if GCS_TEMP_BUCKET:
            writer = writer.option("temporaryGcsBucket", GCS_TEMP_BUCKET)
        writer.mode("append").save()
        print(f"Batch {batch_id}: Wrote {row_count} rows to BigQuery")


def run_streaming_aggregation():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Reduce shuffle partitions for local testing (default 200 is overkill)
    # Helps handle data skew between US (~41% traffic) and smaller markets
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    # Read from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON and extract fields
    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
    )

    # Explode comma-separated genres into individual rows
    exploded_stream = parsed_stream.withColumn(
        "individual_genre", explode(split(col("genre"), ","))
    )

    # =========================================================================
    # WATERMARKING & WINDOWED AGGREGATION
    # =========================================================================
    #
    # WHY WATERMARKING IS ESSENTIAL FOR A REAL-TIME DASHBOARD:
    #
    # 1. Late-Arriving Data: Events may arrive out of order due to network delays.
    #    Without watermarking, Spark would either keep all windows open forever
    #    (memory exhaustion) or drop late data silently (inaccurate metrics).
    #
    # 2. State Management: Watermarks tell Spark "events older than 10 minutes
    #    past the latest seen timestamp will not arrive." This allows Spark to
    #    safely close windows and clean up state.
    #
    # 3. Business Trade-off: 10-minute watermark balances data accuracy
    #    (capturing delayed mobile/international events) against dashboard freshness.
    # =========================================================================

    trending_by_country = (
        exploded_stream.withWatermark("timestamp", "10 minutes")
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("country"),
            col("individual_genre"),
        )
        .agg(spark_sum("watch_hours").alias("total_watch_hours"))
    )

    # Format output
    output_stream = trending_by_country.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("country"),
        col("individual_genre"),
        col("total_watch_hours"),
    ).withColumn("processed_at", current_timestamp())

    # Choose output sink based on configuration
    if OUTPUT_MODE == "bigquery":
        print(f"Writing to BigQuery: {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
        query = (
            output_stream.writeStream.outputMode("update")
            .foreachBatch(write_to_bigquery)
            .option("checkpointLocation", "/tmp/spark-checkpoints/trending-bq")
            .trigger(processingTime="30 seconds")
            .start()
        )
    else:
        print("Writing to console...")
        query = (
            output_stream.writeStream.outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("checkpointLocation", "/tmp/spark-checkpoints/trending")
            .trigger(processingTime="30 seconds")
            .start()
        )

    print("Streaming aggregation started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming_aggregation()
