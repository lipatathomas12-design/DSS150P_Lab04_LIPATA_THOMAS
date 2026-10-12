"""CLI entry point:  python -m src.run_pipeline [--mode full] [--no-db] [--source PATH]"""
import argparse
import sys

from . import tasks
from .common import Run, load_config, new_run_id, use_db


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="full", choices=["full", "incremental"])
    ap.add_argument("--batch-size", type=int)
    ap.add_argument("--source", help="override source CSV (e.g. a temp copy for failure injection)")
    ap.add_argument("--skip-checksum", action="store_true", help="demo only: bypass manifest check")
    ap.add_argument("--no-db", action="store_true")
    ap.add_argument("--run-id")
    a = ap.parse_args(argv)
    if a.no_db:
        import os
        os.environ["PIPELINE_USE_DB"] = "0"

    rid = Run(a.run_id).run_id
    steps = [
        lambda: tasks.verify_source(rid, a.source, a.skip_checksum),
        lambda: tasks.ingest_raw(rid, a.mode, a.batch_size, a.source),
        lambda: tasks.build_staging_task(rid),
        lambda: tasks.build_curated_task(rid),
        lambda: tasks.load_postgres(rid),
        lambda: tasks.build_features_task(rid),
        lambda: tasks.train_model(rid),
        lambda: tasks.compare_metrics_task(rid),
    ]
    try:
        for step in steps:
            step()
    except Exception as exc:
        print(f"PIPELINE FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        try:
            tasks.finalize_run(rid)       # still writes the FAILED run record
        except Exception as inner:
            print(f"Could not write failed run record: {inner}", file=sys.stderr)
        return 1
    print("PIPELINE", tasks.finalize_run(rid))
    return 0


if __name__ == "__main__":
    sys.exit(main())
