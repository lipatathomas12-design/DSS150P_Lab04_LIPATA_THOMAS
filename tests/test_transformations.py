import numpy as np

from src.curated import build_curated
from src.staging import build_staging


def test_staging_fixes_decimals_dedupes_and_rejects_null_key(raw_df):
    stg, rejected = build_staging(raw_df, "t")
    assert rejected == 1
    assert stg["ID"].is_unique and len(stg) == 3
    assert stg.loc[stg.ID == 1, "Rating Average"].iloc[0] == 7.5
    assert stg.loc[stg.ID == 2, "Name"].iloc[0] == "B"          # first occurrence kept
    assert np.isnan(stg.loc[stg.ID == 3, "Owned Users"].iloc[0])  # staging does not impute


def test_curated_rules(raw_df):
    stg, _ = build_staging(raw_df, "t")
    cur = build_curated(stg, "t")
    row = cur[cur.ID == 2].iloc[0]
    assert row["Min Players"] == 1 and row["Max Players"] >= row["Min Players"]
    assert row["Play Time"] > 0 and row["Min Age"] > 0 and row["Complexity Average"] > 0
    assert cur.isna().sum().sum() == 0
    assert cur[cur.ID == 3]["Mechanics"].iloc[0] == "Unknown"