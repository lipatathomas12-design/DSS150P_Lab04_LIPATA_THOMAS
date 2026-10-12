"""Recompute the source checksum/row count and print YAML to paste into source_manifest.yml.
    python scripts/update_manifest.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common import load_config
from src.ingest import read_source, sha256_file, source_path

p = source_path(load_config())
df = read_source(p)
print(f"sha256: {sha256_file(p)}\nsize_bytes: {p.stat().st_size}\nexpected_rows: {len(df)}\nexpected_columns: {df.shape[1]}")
