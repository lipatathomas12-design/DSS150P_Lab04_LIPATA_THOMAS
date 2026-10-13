import pandas as pd
import yaml

from .common import resolve


class DataQualityError(Exception):
    pass


def load_contract(cfg):
    return yaml.safe_load(resolve(cfg["paths"]["contract"]).read_text(encoding="utf-8"))


def _res(stage, check, passed, detail=""):
    return {"stage": stage, "check": check, "passed": bool(passed), "detail": detail}


def check_schema(df, contract, stage):
    missing = [c["name"] for c in contract["columns"] if c["name"] not in df.columns]
    return _res(stage, "schema", not missing, f"missing columns: {missing}" if missing else "ok")


def check_completeness(df, contract, stage):
    bad = {c["name"]: int(df[c["name"]].isna().sum())
           for c in contract["columns"] if c.get("required") and df[c["name"]].isna().any()}
    return _res(stage, "completeness", not bad, f"nulls in required columns: {bad}" if bad else "ok")


def check_uniqueness(df, contract, stage):
    dups = int(df.duplicated(subset=contract["key"]).sum())
    return _res(stage, "uniqueness", dups == 0, f"{dups} duplicate key rows" if dups else "ok")


def check_validity(df, contract, stage, ranges=True):
    problems = []
    for c in contract["columns"]:
        name, typ = c["name"], c["type"]
        if typ in ("int", "float") and not pd.api.types.is_numeric_dtype(df[name]):
            problems.append(f"{name}: not numeric")
            continue
        if ranges and typ in ("int", "float"):
            if "min" in c and (df[name] < c["min"]).any():
                problems.append(f"{name}: {(df[name] < c['min']).sum()} rows < {c['min']}")
            if "max" in c and (df[name] > c["max"]).any():
                problems.append(f"{name}: {(df[name] > c['max']).sum()} rows > {c['max']}")
    return _res(stage, "validity", not problems, "; ".join(problems) if problems else "ok")


def check_business_rules(df, contract, stage):
    n = int((df["Min Players"] > df["Max Players"]).sum()) if contract["rules"].get("min_players_le_max_players") else 0
    return _res(stage, "business_rule_min_le_max_players", n == 0,
                f"{n} rows with Min Players > Max Players" if n else "ok")


def run_checks(df, contract, stage):
    """stage: raw (schema) | staging | curated (all five)."""
    if stage == "raw":
        return [check_schema(df, contract, stage)]
    results = [check_schema(df, contract, stage)]
    if not results[0]["passed"]:
        return results                      # cannot evaluate the rest without the columns
    results.append(check_uniqueness(df, contract, stage))
    results.append(check_validity(df, contract, stage, ranges=(stage == "curated")))
    if stage == "curated":
        results.append(check_completeness(df, contract, stage))
        results.append(check_business_rules(df, contract, stage))
    return results


def check_model_ready(y, X, contract):
    """Target-category and class-balance checks on the model-ready data."""
    rules, stage = contract["rules"], "model_ready"
    results = []
    bad = sorted(set(y.unique()) - set(rules["target_classes"]))
    results.append(_res(stage, "target_categories", not bad, f"unexpected classes: {bad}" if bad else "ok"))
    share = y.value_counts(normalize=True)
    low = {int(k): round(float(v), 4) for k, v in share.items() if v < rules["min_class_share"]}
    missing = [c for c in rules["target_classes"] if c not in share.index]
    results.append(_res(stage, "business_rule_class_share", not low and not missing,
                        f"classes below {rules['min_class_share']}: {low}, missing: {missing}" if (low or missing) else "ok"))
    nulls = int(X.isna().sum().sum())
    results.append(_res(stage, "completeness", nulls == 0, f"{nulls} null feature values" if nulls else "ok"))
    return results


def enforce(results):
    failed = [r for r in results if not r["passed"]]
    if failed:
        raise DataQualityError("; ".join(f"[{r['stage']}/{r['check']}] {r['detail']}" for r in failed))