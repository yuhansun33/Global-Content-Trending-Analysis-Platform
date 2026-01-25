import csv
import json
import random
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "viewing_events"
METADATA_PATH = Path(__file__).parent.parent / "data" / "titles_metadata.csv"

# 2026 Netflix Subscriber Distribution (weights in millions)
# to simulate global traffic patterns
COUNTRIES = ["US", "UK", "DE", "BR", "MX", "FR", "IN", "CA", "JP", "KR"]
COUNTRY_WEIGHTS = [81, 18, 16, 16, 14, 13, 12, 9, 9, 8]


def load_movie_metadata():
    movies = []
    with open(METADATA_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            movies.append({"movie_id": row["movie_id"], "genre": row["genre"]})
    return movies


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
    movie = random.choice(movies)
    return {
        "user_id": f"user_{random.randint(1, 10000):05d}",
        "movie_id": movie["movie_id"],
        "country": random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS)[0],
        "genre": movie["genre"],
        "watch_hours": round(random.uniform(0.1, 3.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
    }


def main():
    movies = load_movie_metadata()
    print(f"Loaded {len(movies)} movies from metadata.")

    create_topic_if_not_exists()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Sending messages to '{TOPIC_NAME}'... (Ctrl+C to stop)")
    try:
        while True:
            event = generate_viewing_event(movies)
            producer.send(TOPIC_NAME, value=event)
            print(f"Sent: {event}")
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
