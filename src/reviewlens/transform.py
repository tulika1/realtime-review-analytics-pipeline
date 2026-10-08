"""Bronze -> silver logic. Pure functions so they are unit-testable and
identical to what the Glue/Spark job does (see glue/silver_job.py).

Silver = current state of each review (SCD1), built with:
  1. contract validation  -> quarantine bad rows
  2. event_id dedup       -> absorbs at-least-once redelivery
  3. latest-wins per review_id by (event_time, event_id) -> deterministic under reordering
  4. deletes become tombstones (kept for downstream propagation, filtered in gold)
  5. deterministic PII redaction BEFORE any text leaves our account boundary
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
    """Idempotent merge of a batch of events into the current silver state.

    Re-running the same batch, or a batch overlapping a previous one, yields
    the same output - the property that makes backfills and retries safe.
    """
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
            continue  # late event older than what we already have: ignore for SCD1
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
