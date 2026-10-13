import numpy as np

from .common import utcnow
from .constants import COLS


def build_curated(stg, run_id):
    df = stg[COLS].copy()

    df = df.dropna(subset=["Year Published"])
    df["Owned Users"] = df["Owned Users"].fillna(df["Owned Users"].median())
    for col in ["Name", "Mechanics", "Domains"]:
        df[col] = df[col].fillna("Unknown")

    df.loc[df["Year Published"] < 0, "Year Published"] = np.nan
    df["Year Published"] = df["Year Published"].fillna(df["Year Published"].median()).astype(int)

    df.loc[df["Min Players"] == 0, "Min Players"] = 1
    df.loc[df["Max Players"] == 0, "Max Players"] = np.nan
    df["Max Players"] = df["Max Players"].fillna(df["Min Players"]).astype(int)

    for col in ["Play Time", "Min Age"]:
        df.loc[df[col] == 0, col] = np.nan
        df[col] = df[col].fillna(df[col].median()).astype(int)

    df.loc[df["Complexity Average"] == 0, "Complexity Average"] = np.nan
    df["Complexity Average"] = df["Complexity Average"].fillna(df["Complexity Average"].median()).round(2)

    df = df.dropna(subset=["ID"])
    for col in ["ID", "Min Players", "Users Rated", "BGG Rank", "Owned Users"]:
        df[col] = df[col].astype(int)

    df = df.reset_index(drop=True)
    df["curated_at"] = utcnow()
    df["run_id"] = run_id
    return df