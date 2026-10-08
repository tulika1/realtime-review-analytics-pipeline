"""Quality checks for the lite pipeline. Failing a 'block' check stops publishing."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Check:
    name: str
    passed: bool
    value: float
    threshold: str
    severity: str  # "block" or "warn"


def run_checks(n_events: int, quarantine: int, silver: list[dict],
               outcomes: list, rule_baseline: dict[str, str] | None = None) -> list[Check]:
    checks: list[Check] = []

    q_rate = quarantine / max(1, n_events)
    checks.append(Check("contract_quarantine_rate", q_rate <= 0.02, q_rate, "<= 2%", "block"))

    ids = [r["review_id"] for r in silver]
    checks.append(Check("silver_review_id_unique", len(ids) == len(set(ids)),
                        len(ids) - len(set(ids)), "== 0 dupes", "block"))

    live = [r for r in silver if not r["is_deleted"]]
    enriched = [o for o in outcomes if o.insight]
    coverage = len(enriched) / max(1, len(live))
    checks.append(Check("enrichment_coverage", coverage >= 0.95, coverage, ">= 95%", "block"))

    # sentiment should mostly agree with the star rating
    rating = {r["review_id"]: r["rating"] for r in silver}
    agree = sum(
        1 for o in enriched
        if (o.insight.sentiment == "negative" and rating[o.review_id] <= 2)
        or (o.insight.sentiment == "positive" and rating[o.review_id] >= 4)
        or o.insight.sentiment in ("mixed", "neutral")
    )
    agreement = agree / max(1, len(enriched))
    checks.append(Check("sentiment_rating_agreement", agreement >= 0.8, agreement, ">= 80%", "warn"))

    # compare with the rules enricher to spot model/prompt drift
    if rule_baseline:
        same = sum(1 for o in enriched if rule_baseline.get(o.review_id) == o.insight.sentiment)
        overlap = same / max(1, len(enriched))
        checks.append(Check("llm_vs_baseline_overlap", overlap >= 0.6, overlap, ">= 60%", "warn"))
    return checks


def gate(checks: list[Check]) -> bool:
    return all(c.passed for c in checks if c.severity == "block")
