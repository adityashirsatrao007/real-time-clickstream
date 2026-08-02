"""Clickstream event generator.

Simulates user click events and publishes them to Kafka.

Usage:
    python click_generator.py --rate 1000 --seconds 30
"""

import argparse
import json
import random
import time
import uuid

from kafka import KafkaProducer

PAGES = ["/home", "/pricing", "/docs", "/signup", "/login", "/blog", "/api", "/settings"]
LOCALES = ["en", "hi", "ja", "mr", "de"]


def make_event(ts: float) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"u{random.randint(1, 2000):05d}",
        "session_id": str(uuid.uuid4()),
        "page": random.choice(PAGES),
        "locale": random.choice(LOCALES),
        "event_type": random.choice(["pageview", "click", "scroll", "conversion"]),
        "ts": ts,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=1000, help="events/sec")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--topic", default="clicks")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    interval = 1.0 / max(args.rate, 1)
    deadline = time.time() + args.seconds
    sent = 0
    start = time.time()

    print(f"Publishing to {args.topic} at ~{args.rate} events/sec for {args.seconds}s")
    while time.time() < deadline:
        now = time.time()
        producer.send(args.topic, value=make_event(now))
        sent += 1
        time.sleep(interval)

    producer.flush()
    elapsed = time.time() - start
    print(f"Sent {sent} events in {elapsed:.1f}s ({sent / elapsed:.0f} events/sec)")


if __name__ == "__main__":
    main()
