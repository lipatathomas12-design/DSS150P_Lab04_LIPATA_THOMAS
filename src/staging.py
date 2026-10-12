"""Staging layer: typed, standardized, de-duplicated. No imputation, no row dropping
other than duplicate IDs (those decisions belong to curated)."""
import numpy as np
import pandas as pd

from .common import utcnow
from .constants import COLS, NUMERIC_COLS, TEXT_COLS


def build_staging(raw, run_id):
    df = raw[["source_row_number"] + COLS].copy()
    df = df.sort_values("source_row_number")

    for col in TEXT_COLS:                                   # trim; blank / 'nan' -> missing
        df[col] = df[col].astype(str).str.strip().replace({"": np.nan, "nan": np.nan})

    for col in ["Rating Average", "Complexity Average"]:    # decimal comma -> point
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False).str.strip()
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # business key = BGG game ID. Rows without a key cannot be identified or loaded
    # (e.g. source row 10778 has a blank ID), so they are rejected here and counted.
    rejected_null_key = int(df["ID"].isna().sum())
    df = df[df["ID"].notna()]
    # duplicate rule: keep the first occurrence in file order
    df = df.drop_duplicates(subset="ID", keep="first").reset_index(drop=True)
    df["staged_at"] = utcnow()
    df["run_id"] = run_id
    return df, rejected_null_key
