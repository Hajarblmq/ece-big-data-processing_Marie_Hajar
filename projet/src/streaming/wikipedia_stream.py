"""Kafka producer: forward live English Wikipedia changes to Kafka.

The script opens ONE long-lived connection to the public Wikimedia
EventStreams service (Server-Sent Events) and forwards every change made
on English Wikipedia to a Kafka topic, for a fixed duration.

Etiquette towards Wikimedia: a single persistent connection is the usage
the service is designed for (no polling), the requests carry a
descriptive User-Agent as required by the Wikimedia User-Agent policy,
and reconnections wait with an increasing delay.

Run from the `projet` folder while Kafka is up:
    python -m src.streaming.wikipedia_stream --minutes 20
"""
import argparse
import json
import os
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .common import KAFKA_BOOTSTRAP, KAFKA_TOPIC

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
REPO_URL = "https://github.com/Hajarblmq/ece-big-data-processing_Marie_Hajar"
CONTACT = os.environ.get("WIKI_CONTACT", REPO_URL)
USER_AGENT = f"ECE-BigData-Student-Project/1.0 ({CONTACT})"
WIKI = "enwiki"


def ensure_topic(bootstrap: str, topic: str) -> None:
    """Create the topic if it does not exist yet."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    )
    futures[topic].result()
    print(f"Topic {topic!r} created")


def read_events(last_event_id: str | None):
    """Yield (event_id, event) pairs from the EventStreams SSE endpoint."""
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    if last_event_id:
        # Resume exactly where the previous connection stopped.
        headers["Last-Event-ID"] = last_event_id
    with requests.get(
        STREAM_URL, headers=headers, stream=True, timeout=(10, 60)
    ) as response:
        response.raise_for_status()
        event_id = None
        for raw in response.iter_lines(decode_unicode=True):
            if not raw:
                continue
            if raw.startswith("id:"):
                event_id = raw[3:].strip()
            elif raw.startswith("data:"):
                try:
                    yield event_id, json.loads(raw[5:])
                except json.JSONDecodeError:
                    continue


def run(minutes: float) -> None:
    ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "compression.type": "lz4",
        "linger.ms": 100,
    })

    stop_at = time.time() + minutes * 60
    sent = 0
    seen = 0
    last_id = None
    backoff = 1
    print(f"Streaming {WIKI} changes to {KAFKA_TOPIC!r} for {minutes} min")

    while time.time() < stop_at:
        try:
            for event_id, event in read_events(last_id):
                last_id = event_id or last_id
                backoff = 1
                seen += 1
                if event.get("wiki") == WIKI:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=event.get("title", "").encode("utf-8"),
                        value=json.dumps(event).encode("utf-8"),
                    )
                    producer.poll(0)
                    sent += 1
                    if sent % 500 == 0:
                        print(f"  {sent} {WIKI} events sent ({seen} seen)")
                if time.time() >= stop_at:
                    break
        except (requests.RequestException, ValueError) as error:
            print(f"Connection lost ({error}); retrying in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    producer.flush(30)
    print(f"Done: {sent} {WIKI} events sent to Kafka ({seen} events seen)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--minutes", type=float, default=20)
    run(parser.parse_args().minutes)


if __name__ == "__main__":
    main()
