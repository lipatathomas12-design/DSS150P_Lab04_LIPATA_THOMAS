import json

import pandas as pd

from . import compare_metrics, features, ingest, quality, train
from .common import Run, load_config, resolve, use_db, write_parquet
from .curated import build_curated
from .staging import build_staging


def _none(v):
    """Airflow templating renders empty values as '' or 'None'."""
    return None if v in (None, "", "None") else v


def verify_source(run_id, source=None, skip_checksum=False):
    source = _none(source)
    cfg, run = load_config(), Run(run_id)
    with run.stage("verify_source") as s:
        s.update(ingest.verify_source(cfg, source, skip_checksum))


def ingest_raw(run_id, mode="full", batch_size=None, source=None):
    source = _none(source)
    batch_size = int(batch_size) if _none(batch_size) else None
    cfg, run = load_config(), Run(run_id)
    with run.stage("ingest_raw") as s:
        info = ingest.ingest(cfg, run.run_id, mode, batch_size, source)
        s.update(rows_in=info["source_rows"], rows_out=info["raw_rows"], **info)
        run.set_count("raw", info["raw_rows"])


def build_staging_task(run_id):
    cfg, run = load_config(), Run(run_id)
    contract = quality.load_contract(cfg)
    with run.stage("staging") as s:
        raw = pd.read_parquet(resolve(cfg["paths"]["raw"]))
        res = quality.run_checks(raw, contract, "raw")          # fail fast on schema problems
        run.add_quality(res); quality.enforce(res)
        stg, rejected = build_staging(raw, run.run_id)
        res = quality.run_checks(stg, contract, "staging")
        run.add_quality(res); quality.enforce(res)
        write_parquet(stg, cfg["paths"]["staging"])
        s.update(rows_in=len(raw), rows_out=len(stg), rejected_null_key=rejected)
        run.set_count("staging", len(stg))


def build_curated_task(run_id):
    cfg, run = load_config(), Run(run_id)
    contract = quality.load_contract(cfg)
    with run.stage("curated") as s:
        stg = pd.read_parquet(resolve(cfg["paths"]["staging"]))
        cur = build_curated(stg, run.run_id)
        res = quality.run_checks(cur, contract, "curated")
        run.add_quality(res); quality.enforce(res)
        write_parquet(cur, cfg["paths"]["curated"])
        s.update(rows_in=len(stg), rows_out=len(cur))
        run.set_count("curated", len(cur))


def load_postgres(run_id):
    cfg, run = load_config(), Run(run_id)
    with run.stage("load_postgres") as s:
        if not use_db(cfg):
            s["skipped"] = "database disabled"
            return
        from . import load_postgres as db
        s.update(db.load_layers(cfg))


def build_features_task(run_id):
    cfg, run = load_config(), Run(run_id)
    contract = quality.load_contract(cfg)
    with run.stage("features") as s:
        cur = pd.read_parquet(resolve(cfg["paths"]["curated"]))
        df = features.build_features(cur, cfg)
        X_tr, X_te, y_tr, y_te = features.split_and_scale(df, cfg)
        res = quality.check_model_ready(pd.concat([y_tr, y_te]), pd.concat([X_tr, X_te]), contract)
        run.add_quality(res); quality.enforce(res)
        write_parquet(features.to_model_ready(X_tr, X_te, y_tr, y_te), cfg["paths"]["model_ready"])
        s.update(rows_in=len(cur), rows_out=len(df))
        run.set_count("model_ready", len(df)); run.set_count("train", len(X_tr)); run.set_count("test", len(X_te))


def train_model(run_id):
    cfg, run = load_config(), Run(run_id)
    with run.stage("train_model") as s:
        metrics, preds = train.train_and_evaluate(cfg, run.run_id)
        s.update(accuracy=metrics["accuracy"], weighted_f1=metrics["weighted_f1"], rows_out=len(preds))
        run.set_count("predictions", len(preds))
        if use_db(cfg):
            from . import load_postgres as db
            db.load_predictions(cfg, run.run_id, metrics["model"], preds)


def compare_metrics_task(run_id):
    cfg, run = load_config(), Run(run_id)
    with run.stage("compare_metrics") as s:
        # refresh run_summary.json so the structural checks see the latest counts
        (resolve(cfg["paths"]["outputs"]) / "run_summary.json").write_text(
            json.dumps(run.data, indent=2, default=str), encoding="utf-8")
        result = compare_metrics.compare(cfg)
        s["comparison_status"] = result["status"]
        if result["status"] == "FAIL":
            raise RuntimeError("Reproduced metrics are materially different from baseline; see outputs/metrics_comparison.json")


def finalize_run(run_id):
    cfg, run = load_config(), Run(run_id)
    data = run.finish()
    if use_db(cfg):
        from . import load_postgres as db
        db.record_run(data)
    return data["status"]