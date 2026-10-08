"""Synthetic review event producer.

Deliberately emits the messy cases a real CDC/event stream produces, so the
pipeline has something to prove: at-least-once duplicates, edits, deletes,
late arrivals, contract violations and PII in free text.
"""
from __future__ import annotations

import random
import uuid
from collections import deque
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

PRODUCTS = {
    "P-100": "wireless earbuds",
    "P-200": "smart watch",
    "P-300": "air fryer",
    "P-400": "standing desk",
}

POSITIVE = [
    "Battery life is excellent and setup took two minutes.",
    "Great build quality, feels premium for the price.",
    "Delivery was fast and the packaging was solid.",
    "Customer support replaced my unit within a day.",
]
NEGATIVE = [
    "Stopped charging after a week, very disappointed.",
    "The app keeps disconnecting and crashes on Android.",
    "Arrived damaged and the refund is taking forever.",
    "Way too loud, and the manual is useless.",
]
PII_SNIPPETS = [
    " Call me on +1 415 555 0134 if you need details.",
    " Email me at jane.doe@example.com for the order number.",
    "",
    "",
    "",
]


def _event(review_id: str, product_id: str, customer_id: str, rating: int, text: str,
           event_time: datetime, op: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "review_id": review_id,
        "product_id": product_id,
        "customer_id": customer_id,
        "rating": rating,
        "review_text": text,
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "op": op,
        "schema_version": "1",
    }


def generate(n_reviews: int = 200, seed: int = 7, now: datetime | None = None) -> list[dict]:
    rng = random.Random(seed)
    now = now or datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
    events: list[dict] = []

    for i in range(n_reviews):
        product_id = rng.choice(list(PRODUCTS))
        positive = rng.random() < 0.6
        rating = rng.choice([4, 5]) if positive else rng.choice([1, 2])
        text = rng.choice(POSITIVE if positive else NEGATIVE) + rng.choice(PII_SNIPPETS)
        t = now - timedelta(minutes=rng.randint(0, 60 * 24 * 3))
        review_id = f"R-{i:05d}"
        ev = _event(review_id, product_id, f"C-{rng.randint(1, 80):04d}", rating, text, t, "create")
        events.append(ev)

        roll = rng.random()
        if roll < 0.08:  # at-least-once delivery: exact redelivery of the same event
            events.append(dict(ev))
        elif roll < 0.14:  # customer edits the review later
            events.append(_event(review_id, product_id, ev["customer_id"], min(5, rating + 1),
                                 text + " Update: it got better after a firmware patch.",
                                 t + timedelta(hours=2), "update"))
        elif roll < 0.17:  # customer deletes the review (GDPR / change of mind)
            events.append(_event(review_id, product_id, ev["customer_id"], rating, text,
                                 t + timedelta(hours=1), "delete"))

    # Contract violations the quarantine must catch.
    bad = _event("R-BAD-1", "P-100", "C-0001", 9, "rating out of range", now, "create")
    missing = _event("R-BAD-2", "P-200", "C-0002", 3, "", now, "create")
    events += [bad, missing]

    rng.shuffle(events)  # arrival order != event order
    return events


def _new_review(rng: random.Random, review_id: str, now: datetime) -> dict:
    product_id = rng.choice(list(PRODUCTS))
    positive = rng.random() < 0.6
    rating = rng.choice([4, 5]) if positive else rng.choice([1, 2])
    text = rng.choice(POSITIVE if positive else NEGATIVE) + rng.choice(PII_SNIPPETS)
    return _event(review_id, product_id, f"C-{rng.randint(1, 80):04d}", rating, text, now, "create")


def stream_events(seed: int | None = None, clock=lambda: datetime.now(UTC)) -> Iterator[dict]:
    """Endless event stream for the Kafka producer, with the same messy cases as generate():
    redelivery, edits, deletes, late arrivals and the occasional contract violation."""
    rng = random.Random(seed)
    prefix = uuid.UUID(int=rng.getrandbits(128)).hex[:6]  # unique per producer run
    live: deque[dict] = deque(maxlen=5000)
    i = 0
    while True:
        now = clock()
        roll = rng.random()
        if roll < 0.05 and live:  # at-least-once redelivery of an earlier event
            yield dict(rng.choice(live))
        elif roll < 0.12 and live:  # edit
            prev = rng.choice(live)
            ev = _event(prev["review_id"], prev["product_id"], prev["customer_id"],
                        min(5, prev["rating"] + 1),
                        prev["review_text"] + " Update: it got better after a firmware patch.",
                        now, "update")
            live.append(ev)
            yield ev
        elif roll < 0.14 and live:  # delete (GDPR / change of mind)
            prev = rng.choice(live)
            yield _event(prev["review_id"], prev["product_id"], prev["customer_id"],
                         prev["rating"], prev["review_text"], now, "delete")
        elif roll < 0.15:  # contract violation
            yield _event(f"R-{prefix}-BAD{i}", "P-100", "C-0001", 9, "rating out of range",
                         now, "create")
        else:
            i += 1
            # 5% of events were queued on a phone offline and arrive hours late
            t = now - timedelta(hours=rng.randint(1, 48)) if rng.random() < 0.05 else now
            ev = _new_review(rng, f"R-{prefix}-{i:06d}", t)
            live.append(ev)
            yield ev
