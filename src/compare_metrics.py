"""Machine-readable comparison of baseline (notebook) vs reproduced metrics."""
import json

from .common import load_config, resolve


def compare(cfg):
    base = json.loads(resolve(cfg["paths"]["baseline"]).read_text(encoding="utf-8"))
    new = json.loads((resolve(cfg["paths"]["outputs"]) / "metrics.json").read_text(encoding="utf-8"))
    run = json.loads((resolve(cfg["paths"]["outputs"]) / "run_summary.json").read_text(encoding="utf-8")) \
        if (resolve(cfg["paths"]["outputs"]) / "run_summary.json").exists() else {"counts": {}}
    rep = cfg["reproducibility"]

    structural = {
        "n_train": (base["n_train"], new["n_train"]),
        "n_test": (base["n_test"], new["n_test"]),
        "n_features": (base["n_features"], new["n_features"]),
    }
    if "curated" in run.get("counts", {}):
        structural["cleaned_rows"] = (base["cleaned_rows"], run["counts"]["curated"])
    struct_ok = all(a == b for a, b in structural.values())

    metrics = {}
    for key, tol in (("accuracy", rep["accuracy_tolerance"]), ("weighted_f1", rep["weighted_f1_tolerance"])):
        delta = round(new[key] - base[key], 4)
        outcome = ("MATCH_WITHIN_TOLERANCE" if abs(delta) <= tol
                   else "BETTER_THAN_BASELINE_REVIEW" if delta > 0 else "WORSE_MATERIALLY_DIFFERENT")
        metrics[key] = {"baseline": base[key], "reproduced": new[key], "delta": delta,
                        "tolerance": tol, "outcome": outcome}

    if not struct_ok or any(m["outcome"] == "WORSE_MATERIALLY_DIFFERENT" for m in metrics.values()):
        status = "FAIL"
    elif any(m["outcome"].startswith("BETTER") for m in metrics.values()):
        status = "REVIEW"   # better is not automatic proof: verify split/preprocessing unchanged
    else:
        status = "PASS"
    result = {"status": status, "primary_metric": rep["primary_metric"], "metrics": metrics,
              "structural_checks": {k: {"baseline": a, "reproduced": b, "match": a == b}
                                    for k, (a, b) in structural.items()},
              "baseline_environment": base.get("source"), "reproduced_environment": new["environment"]}
    (resolve(cfg["paths"]["outputs"]) / "metrics_comparison.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8")
    return result


if __name__ == "__main__":
    r = compare(load_config())
    print(json.dumps(r, indent=2))
    raise SystemExit(1 if r["status"] == "FAIL" else 0)
