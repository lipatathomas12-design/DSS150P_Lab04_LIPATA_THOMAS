"""End-to-end (no database): determinism, rerun safety, incremental checkpoint, baseline match."""
import json

import pandas as pd
import pytest

from src import ingest, tasks
from src.common import ROOT, load_config, resolve
from src.run_pipeline import main

pytestmark = pytest.mark.skipif(not (ROOT / "data/raw/bgg_dataset.csv").exists(), reason="source CSV missing")

DATA_COLS = None


def _curated_data():
    from src.constants import COLS
    return pd.read_parquet(resolve(load_config()["paths"]["curated"]))[COLS]


def test_full_run_is_deterministic_and_passes_baseline(monkeypatch):
    monkeypatch.setenv("PIPELINE_USE_DB", "0")
    assert main(["--no-db"]) == 0
    first_data = _curated_data()
    first_metrics = json.loads((ROOT / "outputs/metrics.json").read_text())
    first_cmp = json.loads((ROOT / "outputs/metrics_comparison.json").read_text())
    assert first_cmp["status"] in ("PASS", "REVIEW")

    assert main(["--no-db"]) == 0                    # second run
    pd.testing.assert_frame_equal(first_data, _curated_data())          # same logical records
    second = json.loads((ROOT / "outputs/metrics.json").read_text())
    assert second["accuracy"] == first_metrics["accuracy"]
    assert second["test_ids_sha256"] == first_metrics["test_ids_sha256"]


def test_incremental_checkpoint_has_no_duplicates(monkeypatch):
    cfg = load_config()
    ck = resolve(cfg["paths"]["state"]) / "checkpoint.json"
    ck.unlink(missing_ok=True)
    a = ingest.ingest(cfg, "t1", "incremental", 5000)
    b = ingest.ingest(cfg, "t2", "incremental", 5000)
    assert (a["raw_rows"], b["raw_rows"]) == (5000, 10000)
    while not b["complete"]:
        b = ingest.ingest(cfg, "t3", "incremental", 5000)
    again = ingest.ingest(cfg, "t4", "incremental", 5000)             # rerun after completion
    raw = pd.read_parquet(resolve(cfg["paths"]["raw"]))
    assert again["rows_loaded"] == 0
    assert raw["source_row_number"].is_unique and len(raw) == b["source_rows"]
    ingest.ingest(cfg, "restore", "full")
