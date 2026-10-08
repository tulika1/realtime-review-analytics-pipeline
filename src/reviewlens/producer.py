"""Sends fake review events to Kafka (topic reviews.v1, key = review_id).

    python -m reviewlens.producer --rate 2
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import time

from confluent_kafka import Producer

from reviewlens.generator import stream_events

TOPIC = os.environ.get("KAFKA_TOPIC", "reviews.v1")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=float(os.environ.get("EVENTS_PER_SEC", "2")))
    p.add_argument("--max-events", type=int, default=0, help="0 = run until stopped")
    args = p.parse_args()

    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "zstd",
    })
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    sent = 0
    for event in stream_events():
        if not running or (args.max_events and sent >= args.max_events):
            break
        producer.produce(TOPIC, key=event["review_id"], value=json.dumps(event))
        producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            print(f"sent {sent} events", flush=True)
        time.sleep(1 / args.rate)
    producer.flush(10)
    print(f"stopped after {sent} events", flush=True)


if __name__ == "__main__":
    main()
