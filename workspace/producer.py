import csv
import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pytz
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configuration from environment variables (supports remote AWS Kafka)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "viewing_events")
METADATA_PATH = Path(__file__).parent.parent / "data" / "titles_metadata.csv"

# Event generation configuration
EVENT_INTERVAL = float(os.getenv("EVENT_INTERVAL", "2"))  # 2 seconds between batches
BATCH_MIN = int(os.getenv("BATCH_MIN", "2"))  # Min events per batch
BATCH_MAX = int(os.getenv("BATCH_MAX", "5"))  # Max events per batch

# 2026 Netflix Subscriber Distribution (base weights in millions)
# to simulate global traffic patterns
COUNTRIES = ["US", "UK", "DE", "BR", "MX", "FR", "IN", "CA", "JP", "KR"]
BASE_COUNTRY_WEIGHTS = [81, 18, 16, 16, 14, 13, 12, 9, 9, 8]

# Timezone mapping for each country (used for peak hour simulation)
COUNTRY_TIMEZONES = {
    "US": "America/New_York",
    "UK": "Europe/London",
    "DE": "Europe/Berlin",
    "BR": "America/Sao_Paulo",
    "MX": "America/Mexico_City",
    "FR": "Europe/Paris",
    "IN": "Asia/Kolkata",
    "CA": "America/Toronto",
    "JP": "Asia/Tokyo",
    "KR": "Asia/Seoul",
}

# Peak hour configuration (20:00 - 23:00 local time = highest traffic)
PEAK_HOUR_START = 20
PEAK_HOUR_END = 23
PEAK_HOUR_MULTIPLIER = 3.0  # 3x traffic during peak hours


def load_movie_metadata():
    movies = []
    with open(METADATA_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movies.append({"movie_id": row["movie_id"], "genre": row["genre"]})
    return movies


def get_dynamic_country_weights():
    """
    Calculate dynamic weights based on local time in each country.
    Countries in peak hours (20:00-23:00 local time) get higher weights.

    This simulates real-world streaming behavior where evening hours
    have significantly higher viewership.
    """
    utc_now = datetime.now(timezone.utc)
    dynamic_weights = []

    for i, country in enumerate(COUNTRIES):
        base_weight = BASE_COUNTRY_WEIGHTS[i]
        tz = pytz.timezone(COUNTRY_TIMEZONES[country])
        local_time = utc_now.astimezone(tz)
        local_hour = local_time.hour

        # Apply peak hour multiplier if within 20:00-23:00 local time
        if PEAK_HOUR_START <= local_hour < PEAK_HOUR_END:
            weight = base_weight * PEAK_HOUR_MULTIPLIER
        else:
            weight = base_weight

        dynamic_weights.append(weight)

    return dynamic_weights


def create_topic_if_not_exists():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)
    try:
        admin_client.create_topics([topic])
        print(f"Topic '{TOPIC_NAME}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists.")
    finally:
        admin_client.close()


def generate_viewing_event(movies):
    """
    Generate a viewing event with timezone-aware country weighting.
    Countries in peak hours (20:00-23:00 local time) have 3x higher probability.
    """
    movie = random.choice(movies)
    dynamic_weights = get_dynamic_country_weights()
    return {
        "user_id": f"user_{random.randint(1, 10000):05d}",
        "movie_id": movie["movie_id"],
        "country": random.choices(COUNTRIES, weights=dynamic_weights)[0],
        "genre": movie["genre"],
        "watch_hours": round(random.uniform(0.1, 3.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_peak_countries():
    """Get list of countries currently in peak hours (used for logging only)."""
    utc_now = datetime.now(timezone.utc)
    peak_countries = []

    for country in COUNTRIES:
        tz = pytz.timezone(COUNTRY_TIMEZONES[country])
        local_time = utc_now.astimezone(tz)
        local_hour = local_time.hour
        if PEAK_HOUR_START <= local_hour < PEAK_HOUR_END:
            peak_countries.append((country, local_time.strftime("%H:%M")))

    return peak_countries


def log_peak_hour_status():
    """Log which countries are currently in peak hours."""
    peak_countries = get_peak_countries()

    if peak_countries:
        formatted = [f"{c} ({t})" for c, t in peak_countries]
        print(f"Peak hours (3x weight): {', '.join(formatted)}")
    else:
        print("No countries in peak hours")


def main():
    # Calculate estimated throughput
    avg_batch = (BATCH_MIN + BATCH_MAX) / 2
    events_per_hour = avg_batch / EVENT_INTERVAL * 3600
    events_per_day = events_per_hour * 24

    print("=" * 60)
    print("Netflix Streaming Event Producer")
    print("=" * 60)
    print(f"Kafka Broker: {KAFKA_BROKER}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Interval: {EVENT_INTERVAL}s | Batch: {BATCH_MIN}-{BATCH_MAX} events")
    print(f"Throughput: ~{events_per_hour:,.0f}/hour | ~{events_per_day:,.0f}/day")
    print("=" * 60)

    movies = load_movie_metadata()
    print(f"Loaded {len(movies)} movies from metadata.")

    create_topic_if_not_exists()
    log_peak_hour_status()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"\nSending messages to '{TOPIC_NAME}'... (Ctrl+C to stop)\n")
    try:
        event_count = 0
        batch_count = 0

        while True:
            batch_size = random.randint(BATCH_MIN, BATCH_MAX)

            # Send batch of events
            batch_events = []
            for _ in range(batch_size):
                event = generate_viewing_event(movies)
                producer.send(TOPIC_NAME, value=event)
                batch_events.append(event["country"])
                event_count += 1

            batch_count += 1
            countries_summary = ", ".join(batch_events)
            print(
                f"[Batch {batch_count}] Sent {batch_size} events: [{countries_summary}]"
            )

            # Log status every 50 batches
            if batch_count % 50 == 0:
                print("-" * 50)
                print(f"Stats: {event_count:,} total events | {batch_count} batches")
                log_peak_hour_status()
                print("-" * 50)

            time.sleep(EVENT_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n{'=' * 50}")
        print("Stopping producer.")
        print(f"Total events sent: {event_count:,}")
        print(f"Total batches: {batch_count}")
        print(f"{'=' * 50}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
