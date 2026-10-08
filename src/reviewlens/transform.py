"""Bronze -> silver in plain Python. Reference implementation for the Spark job.

Rules: validate, drop duplicate event_ids, keep the latest event per review,
turn deletes into tombstones, redact PII.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from reviewlens.contracts import parse_ts, validate

EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
PHONE_RE = re.compile(r"\+?\d[\d\s().-]{7,}\d")


def redact_pii(text: str) -> str:
    text = EMAIL_RE.sub("[EMAIL]", text)
    return PHONE_RE.sub("[PHONE]", text)


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


@dataclass
class SilverResult:
    rows: list[dict] = field(default_factory=list)
    quarantine: list[dict] = field(default_factory=list)
    duplicates_dropped: int = 0


def to_silver(events: list[dict], existing: dict[str, dict] | None = None) -> SilverResult:
    """Merge events into the current state. Safe to re-run with the same events."""
    state: dict[str, dict] = dict(existing or {})
    result = SilverResult()
    seen: set[str] = {r["last_event_id"] for r in state.values()}

    valid: list[dict] = []
    for ev in events:
        check = validate(ev)
        if not check.ok:
            result.quarantine.append({**ev, "_errors": list(check.errors)})
            continue
        if ev["event_id"] in seen:
            result.duplicates_dropped += 1
            continue
        seen.add(ev["event_id"])
        valid.append(ev)

    valid.sort(key=lambda e: (parse_ts(e["event_time"]), e["event_id"]))
    for ev in valid:
        current = state.get(ev["review_id"])
        key = (parse_ts(ev["event_time"]), ev["event_id"])
        if current and (parse_ts(current["event_time"]), current["last_event_id"]) >= key:
            continue  # older than what we have
        clean = redact_pii(ev["review_text"])
        state[ev["review_id"]] = {
            "review_id": ev["review_id"],
            "product_id": ev["product_id"],
            "customer_id": ev["customer_id"],
            "rating": ev["rating"],
            "review_text_redacted": clean,
            "text_hash": content_hash(clean),
            "event_time": ev["event_time"],
            "is_deleted": ev["op"] == "delete",
            "last_event_id": ev["event_id"],
        }

    result.rows = sorted(state.values(), key=lambda r: r["review_id"])
    return result
