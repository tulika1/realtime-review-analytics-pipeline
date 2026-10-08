import random

from reviewlens.generator import generate
from reviewlens.transform import redact_pii, to_silver


def ev(event_id, review_id="R-1", t="2026-10-01T10:00:00Z", op="create", rating=4, text="ok"):
    return {"event_id": event_id, "review_id": review_id, "product_id": "P-1",
            "customer_id": "C-1", "rating": rating, "review_text": text,
            "event_time": t, "op": op, "schema_version": "1"}


def test_redelivered_event_is_dropped():
    out = to_silver([ev("e1"), ev("e1")])
    assert len(out.rows) == 1 and out.duplicates_dropped == 1


def test_latest_event_wins_regardless_of_arrival_order():
    late_update = ev("e2", t="2026-10-01T12:00:00Z", op="update", rating=5)
    out = to_silver([late_update, ev("e1")])
    assert out.rows[0]["rating"] == 5


def test_older_late_event_does_not_overwrite_newer_state():
    first = to_silver([ev("e2", t="2026-10-01T12:00:00Z", rating=5)])
    existing = {r["review_id"]: r for r in first.rows}
    second = to_silver([ev("e1", t="2026-10-01T09:00:00Z", rating=1)], existing)
    assert second.rows[0]["rating"] == 5


def test_delete_becomes_tombstone():
    out = to_silver([ev("e1"), ev("e2", t="2026-10-01T11:00:00Z", op="delete")])
    assert out.rows[0]["is_deleted"] is True


def test_contract_violations_are_quarantined_not_dropped():
    out = to_silver([ev("e1", rating=9), ev("e2", review_id="")])
    assert out.rows == [] and len(out.quarantine) == 2


def test_merge_is_idempotent_and_order_independent():
    events = generate(100)
    a = to_silver(events).rows
    shuffled = events[:]
    random.Random(1).shuffle(shuffled)
    b = to_silver(shuffled).rows
    rerun = to_silver(events, {r["review_id"]: r for r in a}).rows
    assert a == b == rerun


def test_pii_is_redacted():
    text = redact_pii("mail jane.doe@example.com or call +1 415 555 0134")
    assert "example.com" not in text and "555" not in text
