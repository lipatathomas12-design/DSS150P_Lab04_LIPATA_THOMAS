import pandas as pd
import pytest

from src import quality
from src.curated import build_curated
from src.staging import build_staging


@pytest.fixture
def contract(cfg):
    return quality.load_contract(cfg)


@pytest.fixture
def curated(raw_df):
    stg, _ = build_staging(raw_df, "t")
    return build_curated(stg, "t")


def test_clean_data_passes_all_checks(curated, contract):
    res = quality.run_checks(curated, contract, "curated")
    assert len(res) == 5 and all(r["passed"] for r in res)


def test_uniqueness_detects_duplicate_ids(curated, contract):
    bad = pd.concat([curated, curated.iloc[[0]]])
    res = {r["check"]: r for r in quality.run_checks(bad, contract, "curated")}
    assert not res["uniqueness"]["passed"]


def test_validity_detects_out_of_range_rating(curated, contract):
    bad = curated.copy(); bad.loc[0, "Rating Average"] = 11.0
    res = {r["check"]: r for r in quality.run_checks(bad, contract, "curated")}
    assert not res["validity"]["passed"]


def test_business_rule_detects_min_gt_max_players(curated, contract):
    bad = curated.copy(); bad.loc[0, "Min Players"] = 9
    res = {r["check"]: r for r in quality.run_checks(bad, contract, "curated")}
    assert not res["business_rule_min_le_max_players"]["passed"]


def test_class_share_rule_detects_collapsed_target(contract):
    y = pd.Series([0] * 199 + [1])           # class 2 missing, class 1 < 1%
    X = pd.DataFrame({"a": range(200)})
    res = quality.check_model_ready(y, X, contract)
    assert any(not r["passed"] for r in res)
    with pytest.raises(quality.DataQualityError):
        quality.enforce(res)
