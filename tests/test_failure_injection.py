"""Safe, reversible failure injection on TEMPORARY copies of the source."""
import pandas as pd
import pytest

from src import ingest, quality, tasks


def test_modified_source_fails_checksum(tmp_path, cfg):
    bad = tmp_path / "bad.csv"
    bad.write_text(ingest.source_path(cfg).read_text(encoding="utf-8-sig").replace("Gloomhaven", "Gloomhaven2", 1),
                   encoding="utf-8")
    with pytest.raises(ingest.SourceError):
        ingest.verify_source(cfg, override=str(bad))


def test_missing_required_column_stops_pipeline(tmp_path, monkeypatch, cfg):
    """Drop 'Rating Average' in a temp copy; the schema check must stop the run
    before anything reaches curated, model or prediction outputs."""
    monkeypatch.setenv("PIPELINE_USE_DB", "0")
    df = ingest.read_source(ingest.source_path(cfg)).drop(columns=["Rating Average"])
    bad = tmp_path / "missing_col.csv"
    df.to_csv(bad, sep=";", index=False)

    from src.common import Run
    run_id = "pytest-failure-injection"
    tasks.ingest_raw(run_id, "full", None, str(bad))
    with pytest.raises((quality.DataQualityError, KeyError)):
        tasks.build_staging_task(run_id)
    assert Run(run_id).data["status"] == "FAILED"
    # restore good state for later tests
    tasks.ingest_raw("pytest-recovery", "full", None, None)
