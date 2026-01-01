from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


@dataclass
class FeatureOutput:
    X_train: "pd.DataFrame | object"
    X_test: "pd.DataFrame | object"
    y_train: pd.Series
    y_test: pd.Series
    scaler: StandardScaler
    feature_cols: List[str]


def make_features(
    df: pd.DataFrame,
    id_col: str = "num_acc",
    target_col: str = "grav",
    positive_threshold: int = 3,
    test_size: float = 0.2,
    random_state: int = 42,
) -> FeatureOutput:
    """
    Reproduce your local logic:
      - binary target: 1 if grav >= 3 else 0
      - X = all columns except id, grav, target
      - train/test split (stratify)
      - StandardScaler
      - numeric cleaning: coerce non-numeric to NaN then drop
    """
    df = df.copy()

    if target_col not in df.columns:
        raise ValueError(f"target_col '{target_col}' not found in df columns")

    # binary target
    df["target"] = df[target_col].apply(lambda x: 1 if x >= positive_threshold else 0)

    drop_cols = [c for c in [id_col, target_col, "target"] if c in df.columns]
    X = df.drop(columns=drop_cols)
    y = df["target"]

    # Clean: enforce numeric
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    valid_idx = X.dropna().index
    X = X.loc[valid_idx]
    y = y.loc[valid_idx]

    feature_cols = list(X.columns)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    return FeatureOutput(
        X_train=X_train_scaled,
        X_test=X_test_scaled,
        y_train=y_train,
        y_test=y_test,
        scaler=scaler,
        feature_cols=feature_cols,
    )
