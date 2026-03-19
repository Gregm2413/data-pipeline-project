import json
import os
import uuid
from datetime import datetime, timezone
from kafka import KafkaConsumer

# --- Config ---
KAFKA_TOPIC = "olist.events"
KAFKA_BROKER = "localhost:9092"
OUTPUT_DIR = "data/events"
BATCH_SIZE = 100  # events per file

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Consumer ---
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="olist-event-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def write_batch(batch):
    """Write a batch of events to a JSON file."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"{OUTPUT_DIR}/events_{timestamp}_{uuid.uuid4().hex[:8]}.json"
    with open(filename, "w") as f:
        json.dump(batch, f, indent=2)
    print(f"Written {len(batch)} events to {filename}")

def main():
    print(f"Listening on Kafka topic: {KAFKA_TOPIC}")
    batch = []

    for message in consumer:
        event = message.value
        batch.append(event)

        if len(batch) >= BATCH_SIZE:
            write_batch(batch)
            batch = []

if __name__ == "__main__":
    main()