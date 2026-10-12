"""Retrain + evaluate from the model-ready layer (never from copied numbers)."""
import hashlib
import json
import platform

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report, f1_score

from .common import resolve


def train_and_evaluate(cfg, run_id):
    df = pd.read_parquet(resolve(cfg["paths"]["model_ready"]))
    feats = [c for c in df.columns if c not in ("ID", "Rating Category", "split")]
    tr, te = df[df.split == "train"], df[df.split == "test"]

    model = GradientBoostingClassifier(**cfg["model"]["params"])
    model.fit(tr[feats], tr["Rating Category"])
    pred = model.predict(te[feats])
    proba = model.predict_proba(te[feats])

    labels = cfg["target"]["labels"]
    metrics = {
        "pipeline_run_id": run_id,
        "model": cfg["model"]["name"],
        "params": cfg["model"]["params"],
        "accuracy": round(float(accuracy_score(te["Rating Category"], pred)), 4),
        "weighted_f1": round(float(f1_score(te["Rating Category"], pred, average="weighted")), 4),
        "n_train": int(len(tr)), "n_test": int(len(te)), "n_features": len(feats),
        "test_ids_sha256": hashlib.sha256(",".join(map(str, sorted(te["ID"]))).encode()).hexdigest(),
        "classification_report": classification_report(te["Rating Category"], pred,
                                                       target_names=labels, output_dict=True),
        "environment": {"python": platform.python_version(), "scikit-learn": sklearn.__version__,
                        "pandas": pd.__version__, "numpy": np.__version__},
    }
    out = resolve(cfg["paths"]["outputs"])
    out.mkdir(exist_ok=True)
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    predictions = pd.DataFrame({"game_id": te["ID"].astype(int).values,
                                "y_true": te["Rating Category"].values, "y_pred": pred})
    for i, lab in enumerate(labels):
        predictions[f"p_{lab.lower()}"] = proba[:, i].round(6)
    predictions.to_csv(out / "predictions.csv", index=False)
    joblib.dump(model, out / "model.joblib")
    return metrics, predictions
