"""Prove rerun safety: row counts and duplicate business keys per PostgreSQL table.
Run before and after a second pipeline run; the numbers must not change.
    python -m src.verify_rerun"""
import json

from .common import ROOT, utcnow
from .load_postgres import connect

QUERIES = {
    "raw.bgg_games":     ("SELECT count(*), count(*) - count(DISTINCT (source_sha256, source_row_number)) FROM raw.bgg_games"),
    "staging.bgg_games": ("SELECT count(*), count(*) - count(DISTINCT id) FROM staging.bgg_games"),
    "curated.bgg_games": ("SELECT count(*), count(*) - count(DISTINCT id) FROM curated.bgg_games"),
    "ml.predictions":    ("SELECT count(*), count(*) - count(DISTINCT game_id) FROM ml.predictions"),
}

if __name__ == "__main__":
    conn = connect()
    out = {"checked_at": utcnow().isoformat(), "tables": {}}
    with conn.cursor() as cur:
        for table, q in QUERIES.items():
            cur.execute(q)
            rows, dups = cur.fetchone()
            out["tables"][table] = {"rows": rows, "duplicate_keys": dups}
    conn.close()
    print(json.dumps(out, indent=2))
    (ROOT / "outputs" / "rerun_check.json").write_text(json.dumps(out, indent=2))
