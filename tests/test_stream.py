from itertools import islice

from reviewlens.contracts import validate
from reviewlens.generator import stream_events
from reviewlens.transform import to_silver


def test_stream_contains_the_messy_cases_the_pipeline_must_handle():
    events = list(islice(stream_events(seed=1), 2000))
    ops = {e["op"] for e in events}
    assert {"create", "update", "delete"} <= ops
    assert len({e["event_id"] for e in events}) < len(events)  # redeliveries
    invalid = [e for e in events if not validate(e).ok]
    assert 0 < len(invalid) / len(events) < 0.02  # under the quarantine gate


def test_stream_is_consumable_by_silver():
    events = list(islice(stream_events(seed=2), 500))
    out = to_silver(events)
    assert len({r["review_id"] for r in out.rows}) == len(out.rows)
