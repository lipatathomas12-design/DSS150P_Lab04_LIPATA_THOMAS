from collections import Counter

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

from .constants import COLS

SCALE_COLS = ["Year Published", "Min Players", "Max Players", "Play Time", "Min Age",
              "Users Rated", "Complexity Average", "Owned Users", "Player Range",
              "Average Players", "Game Age", "Pop_Complexity"]


def _remove_outliers_iqr(df, col):
    q1, q3 = df[col].quantile(0.25), df[col].quantile(0.75)
    iqr = q3 - q1
    return df[(df[col] >= q1 - 1.5 * iqr) & (df[col] <= q3 + 1.5 * iqr)]


def build_features(cur, cfg):
    f = cfg["features"]
    df = cur[COLS].copy().set_index("ID").drop(columns=["Name"])   # ID kept as index for lineage

    for col in f["outlier_columns"]:
        df = _remove_outliers_iqr(df, col)

    df["Mechanics"] = df["Mechanics"].fillna("")
    counts = Counter()
    df["Mechanics"].str.split(", ").apply(counts.update)
    top = [m for m, _ in counts.most_common(f["top_mechanics"]) if m and m != "Unknown"]
    for m in top:
        df["mech_" + m.replace(" ", "_").replace("/", "_")] = df["Mechanics"].str.contains(m, regex=False).astype(int)
    df = df.drop(columns=["Mechanics"])

    df["Domains"] = df["Domains"].fillna("")
    counts = Counter()
    df["Domains"].str.split(", ").apply(counts.update)
    for d in [d for d, _ in counts.most_common() if d and d != "Unknown"]:
        df["domain_" + d.replace(" ", "_")] = df["Domains"].str.contains(d, regex=False).astype(int)
    df = df.drop(columns=["Domains"])

    df["Player Range"] = df["Max Players"] - df["Min Players"]
    df["Average Players"] = (df["Min Players"] + df["Max Players"]) / 2
    df["Game Age"] = f["current_year"] - df["Year Published"]
    df["Play Time Category"] = pd.cut(df["Play Time"], bins=[0, 30, 90, 180, np.inf],
                                      labels=["Short", "Medium", "Long", "Very Long"]
                                      ).map({"Short": 0, "Medium": 1, "Long": 2, "Very Long": 3}).astype(int)
    df["Complexity Category"] = pd.cut(df["Complexity Average"], bins=[0, 2, 3, 4, 5],
                                       labels=["Easy", "Moderate", "Hard", "Expert"]
                                       ).map({"Easy": 0, "Moderate": 1, "Hard": 2, "Expert": 3}).astype(int)
    df["Pop_Complexity"] = df["Users Rated"] * df["Complexity Average"]
    tgt = cfg["target"]
    df["Rating Category"] = pd.cut(df["Rating Average"], bins=tgt["bins"], labels=tgt["labels"],
                                   include_lowest=True
                                   ).map({l: i for i, l in enumerate(tgt["labels"])}).astype(int)
    return df


def split_and_scale(df, cfg):
    X = df.drop(columns=["Rating Average", "Rating Category", "BGG Rank"])
    y = df["Rating Category"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=cfg["split"]["test_size"], random_state=cfg["split"]["random_state"], stratify=y)
    scaler = MinMaxScaler()
    X_train[SCALE_COLS] = scaler.fit_transform(X_train[SCALE_COLS])   # fit on train only
    X_test[SCALE_COLS] = scaler.transform(X_test[SCALE_COLS])
    return X_train, X_test, y_train, y_test


def to_model_ready(X_train, X_test, y_train, y_test):
    train = X_train.assign(**{"Rating Category": y_train, "split": "train"})
    test = X_test.assign(**{"Rating Category": y_test, "split": "test"})
    return pd.concat([train, test]).reset_index()      # ID back as a column; row order preserved