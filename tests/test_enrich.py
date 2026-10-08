import pytest

from reviewlens.enrich import (
    CircuitOpen,
    EnrichmentCache,
    ReviewInsight,
    RuleBasedEnricher,
    enrich_rows,
)


def row(i, text="Battery stopped charging", deleted=False):
    return {"review_id": f"R-{i}", "review_text_redacted": text, "rating": 1,
            "text_hash": f"h{i}", "is_deleted": deleted}


class CountingEnricher(RuleBasedEnricher):
    model_id = "counting"

    def __init__(self):
        self.calls = 0

    def enrich(self, text, rating):
        self.calls += 1
        return super().enrich(text, rating)


class BrokenEnricher:
    model_id = "broken"

    def enrich(self, text, rating):
        raise RuntimeError("truncated_output")


def test_second_run_is_served_from_cache(tmp_path):
    e = CountingEnricher()
    cache = EnrichmentCache(tmp_path / "c.json")
    enrich_rows([row(1), row(2)], e, cache)
    out = enrich_rows([row(1), row(2)], e, EnrichmentCache(tmp_path / "c.json"))
    assert e.calls == 2 and all(o.cached for o in out)


def test_tombstones_are_never_sent_to_the_model(tmp_path):
    e = CountingEnricher()
    enrich_rows([row(1, deleted=True)], e, EnrichmentCache(tmp_path / "c.json"))
    assert e.calls == 0


def test_circuit_breaker_trips_on_systemic_failure(tmp_path):
    with pytest.raises(CircuitOpen):
        enrich_rows([row(i) for i in range(50)], BrokenEnricher(),
                    EnrichmentCache(tmp_path / "c.json"), max_attempts=1)


def test_model_output_is_normalised_to_taxonomy():
    i = ReviewInsight(sentiment="negative", sentiment_score=-3, topics=["battery", "made_up"],
                      is_actionable=True, summary="x")
    assert i.sentiment_score == -1.0 and i.topics == ["battery"]
