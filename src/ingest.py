"""Raw layer: checksum verification, full load, and checkpointed incremental load.

The dataset has no natural update timestamp, so incremental ingestion is simulated
by loading the next N rows by source row number. The checkpoint (state/checkpoint.json)
is a pipeline-generated position marker, not a business timestamp.
"""
import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import yaml

from .common import ROOT, resolve, utcnow, write_parquet
from .constants import COLS


class SourceError(Exception):
    pass


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def source_path(cfg, override=None):
    p = override or os.environ.get("BGG_SOURCE_PATH") or cfg["paths"]["source_csv"]
    return resolve(p)


def verify_source(cfg, override=None, skip_checksum=False):
    path = source_path(cfg, override)
    if not path.exists():
        raise SourceError(f"Source file not found: {path}")
    manifest = yaml.safe_load(resolve(cfg["paths"]["manifest"]).read_text(encoding="utf-8"))
    actual = sha256_file(path)
    expected = manifest["checksum"]["sha256"]
    if actual != expected:
        if skip_checksum:
            from .common import log_event
            log_event("-", "verify_source", "warning",
                      message="checksum mismatch IGNORED (--skip-checksum)", actual=actual)
        else:
            raise SourceError(f"Checksum mismatch for {path.name}: expected {expected}, got {actual}")
    return {"source_file": path.name, "sha256": actual, "checksum_ok": actual == expected}


def read_source(path):
    # everything as text: the raw layer is a faithful copy, typing happens in staging
    return pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig",
                       keep_default_na=False, na_values=[])


def _checkpoint_path(cfg):
    return resolve(cfg["paths"]["state"]) / "checkpoint.json"


def read_checkpoint(cfg):
    p = _checkpoint_path(cfg)
    return json.loads(p.read_text()) if p.exists() else None


def ingest(cfg, run_id, mode="full", batch_size=None, override=None):
    """Returns dict with rows loaded this call and total raw rows."""
    path = source_path(cfg, override)
    sha = sha256_file(path)
    src = read_source(path)
    src.insert(0, "source_row_number", range(1, len(src) + 1))
    raw_path = resolve(cfg["paths"]["raw"])
    ckpt = read_checkpoint(cfg)
    now = utcnow()

    if mode == "full":
        batch, existing = src, None
    elif mode == "incremental":
        batch_size = batch_size or cfg["ingestion"]["incremental_batch_size"]
        last = ckpt["last_source_row"] if ckpt and ckpt["source_sha256"] == sha else 0
        batch = src[(src.source_row_number > last) & (src.source_row_number <= last + batch_size)]
        existing = pd.read_parquet(raw_path) if (last and raw_path.exists()) else None
    else:
        raise ValueError(f"unknown mode: {mode}")

    batch = batch.assign(source_sha256=sha, ingested_at=now, ingest_run_id=run_id)
    combined = batch if existing is None else pd.concat([existing, batch], ignore_index=True)
    # rerun-safe: one row per (file version, source row number)
    combined = (combined.drop_duplicates(["source_sha256", "source_row_number"], keep="first")
                .sort_values("source_row_number").reset_index(drop=True))

    write_parquet(combined, raw_path)          # data first ...
    last_row = int(combined.source_row_number.max()) if len(combined) else 0
    ckpt_path = _checkpoint_path(cfg)          # ... checkpoint second: a crash in between only
    ckpt_path.parent.mkdir(parents=True, exist_ok=True)   # causes a harmless re-read of the batch
    ckpt_path.write_text(json.dumps({
        "source_sha256": sha, "last_source_row": last_row, "updated_at": now.isoformat(),
        "note": "pipeline-generated position marker, not a business timestamp"}, indent=2))
    return {"mode": mode, "rows_loaded": len(batch), "raw_rows": len(combined),
            "source_rows": len(src), "complete": last_row >= len(src)}


if __name__ == "__main__":  # python -m src.ingest --mode incremental --batch-size 5000
    import argparse
    from .common import load_config, new_run_id
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="full", choices=["full", "incremental"])
    ap.add_argument("--batch-size", type=int)
    ap.add_argument("--source")
    a = ap.parse_args()
    print(ingest(load_config(), new_run_id(), a.mode, a.batch_size, a.source))
