"""Validation against the ReviewEvent v1 contract (contracts/review_event.v1.json)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "review_event.v1.json"
_CONTRACT = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
REQUIRED = _CONTRACT["required"]
OPS = set(_CONTRACT["properties"]["op"]["enum"])
MAX_TEXT = _CONTRACT["properties"]["review_text"]["maxLength"]


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: tuple[str, ...]


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def validate(event: dict) -> ValidationResult:
    errors: list[str] = []
    for field in REQUIRED:
        if event.get(field) in (None, ""):
            errors.append(f"missing:{field}")
    if errors:
        return ValidationResult(False, tuple(errors))

    rating = event["rating"]
    if not isinstance(rating, int) or isinstance(rating, bool) or not 1 <= rating <= 5:
        errors.append("invalid:rating")
    if event["op"] not in OPS:
        errors.append("invalid:op")
    if str(event["schema_version"]) != "1":
        errors.append("invalid:schema_version")
    if not isinstance(event["review_text"], str) or len(event["review_text"]) > MAX_TEXT:
        errors.append("invalid:review_text")
    try:
        ts = parse_ts(event["event_time"])
        if ts.tzinfo is None:
            errors.append("invalid:event_time_no_tz")
    except (TypeError, ValueError):
        errors.append("invalid:event_time")
    return ValidationResult(not errors, tuple(errors))
