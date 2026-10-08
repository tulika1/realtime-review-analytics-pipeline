from reviewlens import pipeline


def test_end_to_end_local_run_publishes_gold(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "DATA", tmp_path)
    monkeypatch.setattr(pipeline, "LAKE", tmp_path / "lake")

    first = pipeline.run("local", n_reviews=120)
    assert first["published_to_gold"]
    assert first["quarantined"] == 2
    assert first["duplicates_dropped"] > 0
    assert first["vectors"] == first["silver_rows"] - first["tombstones"]

    # Re-running is idempotent and every enrichment comes from cache (zero model spend).
    second = pipeline.run("local", n_reviews=120)
    assert second["silver_rows"] == first["silver_rows"]
    assert second["cache_hits"] == second["enriched"]
