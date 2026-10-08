"""Review enrichment: sentiment, topics, actionable flag, summary.

Enrichers: rules (default), ollama (local LLM), bedrock (Claude on AWS).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ValidationError, field_validator

PROMPT_VERSION = "v3"
TOPICS = ["battery", "build_quality", "delivery", "support", "software", "damage",
          "noise", "refund", "documentation", "price", "other"]


class ReviewInsight(BaseModel):
    sentiment: Literal["positive", "neutral", "negative", "mixed"]
    sentiment_score: float  # -1.0 .. 1.0
    topics: list[str]
    is_actionable: bool  # describes a defect or service problem
    summary: str

    @field_validator("sentiment_score")
    @classmethod
    def _clamp(cls, v: float) -> float:
        return max(-1.0, min(1.0, v))

    @field_validator("topics")
    @classmethod
    def _taxonomy(cls, v: list[str]) -> list[str]:
        cleaned = sorted({t for t in v if t in TOPICS})
        return cleaned or ["other"]


@dataclass
class EnrichOutcome:
    review_id: str
    insight: ReviewInsight | None
    error: str | None = None
    cached: bool = False


class Enricher(Protocol):
    model_id: str

    def enrich(self, text: str, rating: int) -> ReviewInsight: ...


_KEYWORDS = {
    "battery": ["battery", "charging", "charge"],
    "build_quality": ["build", "quality", "premium", "cheap"],
    "delivery": ["delivery", "arrived", "packaging", "shipping"],
    "support": ["support", "replaced", "customer service"],
    "software": ["app", "firmware", "crash", "disconnect"],
    "damage": ["damaged", "broken"],
    "noise": ["loud", "noise"],
    "refund": ["refund", "return"],
    "documentation": ["manual", "instructions"],
    "price": ["price", "expensive"],
}
_NEG = ["stopped", "disappointed", "crash", "damaged", "forever", "useless", "loud", "disconnect"]
_POS = ["excellent", "great", "fast", "solid", "premium", "better", "replaced"]


class RuleBasedEnricher:
    model_id = "rules-v1"

    def enrich(self, text: str, rating: int) -> ReviewInsight:
        low = text.lower()
        pos = sum(w in low for w in _POS)
        neg = sum(w in low for w in _NEG)
        score = (pos - neg) / max(1, pos + neg)
        if pos and neg:
            sentiment = "mixed"
        elif score > 0:
            sentiment = "positive"
        elif score < 0:
            sentiment = "negative"
        else:
            sentiment = "neutral"
        topics = [t for t, kws in _KEYWORDS.items() if any(k in low for k in kws)]
        return ReviewInsight(
            sentiment=sentiment,
            sentiment_score=score,
            topics=topics,
            is_actionable=neg > 0,
            summary=text[:80],
        )


class OllamaEnricher:
    """Local LLM via Ollama. Needs `ollama pull llama3.2:3b` on the host and ENRICHER=ollama."""

    def __init__(self, model: str | None = None, url: str | None = None):
        self.model_id = model or os.environ.get("OLLAMA_MODEL", "llama3.2:3b")
        self.url = (url or os.environ.get("OLLAMA_URL", "http://localhost:11434")) + "/api/chat"

    def enrich(self, text: str, rating: int) -> ReviewInsight:
        import urllib.request

        body = json.dumps({
            "model": self.model_id,
            "stream": False,
            "format": _json_schema(),
            "options": {"temperature": 0},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Star rating: {rating}\nReview:\n{text}"},
            ],
        }).encode()
        req = urllib.request.Request(self.url, body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            content = json.loads(resp.read())["message"]["content"]
        return ReviewInsight.model_validate(json.loads(content))


def make_enricher(kind: str | None = None) -> Enricher:
    kind = kind or os.environ.get("ENRICHER", "rules")
    if kind == "ollama":
        return OllamaEnricher()
    if kind == "bedrock":
        return ClaudeBedrockEnricher()
    return RuleBasedEnricher()


SYSTEM_PROMPT = f"""You analyse customer product reviews for a retail analytics platform.
Return the insight for the single review you are given.
- sentiment: overall tone of the text, not the star rating. Use "mixed" when the text has
  both clear praise and clear complaints.
- sentiment_score: -1.0 (very negative) to 1.0 (very positive).
- topics: choose only from {TOPICS}. Use "other" when nothing fits.
- is_actionable: true when the review describes a defect, service failure or bug that a
  product or operations team could act on.
- summary: one neutral sentence, at most 20 words, with no personal data.
Placeholders like [EMAIL] and [PHONE] are redactions; ignore them."""


class ClaudeBedrockEnricher:
    """Claude on Amazon Bedrock via the Anthropic SDK's Mantle client."""

    def __init__(self, region: str | None = None, model_id: str | None = None):
        from anthropic import AnthropicBedrockMantle, BetaRefusalFallbackMiddleware

        self.model_id = model_id or os.environ.get("REVIEWLENS_MODEL", "anthropic.claude-opus-5-5")
        # retry refusals once on a fallback model
        self.client = AnthropicBedrockMantle(
            aws_region=region or os.environ.get("AWS_REGION", "us-east-1"),
            middleware=[BetaRefusalFallbackMiddleware([{"model": "anthropic.claude-opus-4-8"}])],
        )

    def enrich(self, text: str, rating: int) -> ReviewInsight:
        response = self.client.beta.messages.create(
            model=self.model_id,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            output_config={
                "effort": "low",  # simple classification
                "format": {"type": "json_schema", "schema": _json_schema()},
            },
            messages=[{"role": "user", "content": f"Star rating: {rating}\nReview:\n{text}"}],
        )
        if response.stop_reason == "refusal":
            raise RuntimeError("model_refusal")
        if response.stop_reason == "max_tokens":
            raise RuntimeError("truncated_output")
        body = next(b.text for b in response.content if b.type == "text")
        return ReviewInsight.model_validate(json.loads(body))


def _json_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "sentiment": {"type": "string", "enum": ["positive", "neutral", "negative", "mixed"]},
            "sentiment_score": {"type": "number"},
            "topics": {"type": "array", "items": {"type": "string", "enum": TOPICS}},
            "is_actionable": {"type": "boolean"},
            "summary": {"type": "string"},
        },
        "required": ["sentiment", "sentiment_score", "topics", "is_actionable", "summary"],
        "additionalProperties": False,
    }


class EnrichmentCache:
    """JSON file cache (DynamoDB on AWS)."""

    def __init__(self, path: Path):
        self.path = path
        self._data: dict[str, dict] = (
            json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        )

    @staticmethod
    def key(model_id: str, text_hash: str) -> str:
        return f"{PROMPT_VERSION}:{model_id}:{text_hash}"

    def get(self, key: str) -> ReviewInsight | None:
        raw = self._data.get(key)
        return ReviewInsight.model_validate(raw) if raw else None

    def put(self, key: str, insight: ReviewInsight) -> None:
        self._data[key] = insight.model_dump()

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=1), encoding="utf-8")


class CircuitOpen(RuntimeError):
    """Too many enrichment failures in one run."""


def enrich_one(row: dict, enricher: Enricher, max_attempts: int) -> EnrichOutcome:
    error = None
    for _ in range(max_attempts):
        try:
            insight = enricher.enrich(row["review_text_redacted"], row["rating"])
            return EnrichOutcome(row["review_id"], insight)
        except (ValidationError, json.JSONDecodeError, RuntimeError) as exc:
            error = f"{type(exc).__name__}:{exc}"[:200]
            if str(exc) == "model_refusal":
                break  # retrying won't help
    return EnrichOutcome(row["review_id"], None, error=error)


def enrich_rows(rows: list[dict], enricher: Enricher, cache: EnrichmentCache,
                max_attempts: int = 3, max_failure_rate: float = 0.05,
                min_sample: int = 20) -> list[EnrichOutcome]:
    outcomes: list[EnrichOutcome] = []
    failures = 0
    for row in rows:
        if row["is_deleted"]:
            continue
        key = cache.key(enricher.model_id, row["text_hash"])
        hit = cache.get(key)
        if hit:
            outcomes.append(EnrichOutcome(row["review_id"], hit, cached=True))
            continue

        outcome = enrich_one(row, enricher, max_attempts)
        if outcome.insight:
            cache.put(key, outcome.insight)
        else:
            failures += 1
        outcomes.append(outcome)

        attempted = len(outcomes)
        if attempted >= min_sample and failures / attempted > max_failure_rate:
            cache.flush()
            raise CircuitOpen(f"enrichment failure rate {failures}/{attempted} > {max_failure_rate:.0%}")
    cache.flush()
    return outcomes
