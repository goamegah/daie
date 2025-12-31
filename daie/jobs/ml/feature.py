# daie/jobs/ml/feature.py
from __future__ import annotations
from typing import List, Tuple
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline


def _to_minutes(x) -> float:
    """Convert 'HH:MM' to minutes. Return None if not parsable."""
    if pd.isna(x):
        return None
    s = str(x)
    if ":" in s:
        hh, mm = s.split(":")[:2]
        try:
            return int(hh) * 60 + int(mm)
        except ValueError:
            return None
    return None


def make_features(
    df: pd.DataFrame,
    target_col: str = "grav",
    id_col: str = "num_acc",
    numeric_cols: List[str] | None = None,
    categorical_cols: List[str] | None = None,
) -> Tuple:
    """
    Returns X_train, X_test, y_train, y_test, preprocess_pipeline
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    if target_col.lower() not in df.columns:
        raise ValueError(f"Target column '{target_col}' not found. Available columns: {list(df.columns)[:50]}")

    if "hrmn" in df.columns:
        df["hrmn_minutes"] = df["hrmn"].apply(_to_minutes)

    # Create binary target  
    df["target"] = df[target_col.lower()].apply(lambda x: 1 if x >= 3 else 0)

    drop_cols = ["target", target_col.lower()]
    if id_col and id_col.lower() in df.columns:
        drop_cols.append(id_col.lower())
    if "hrmn" in df.columns:
        drop_cols.append("hrmn")

    X = df.drop(columns=[c for c in drop_cols if c in df.columns])
    y = df["target"]

    if numeric_cols is None or categorical_cols is None:
        numeric_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
        categorical_cols = [c for c in X.columns if not pd.api.types.is_numeric_dtype(X[c])]

    preprocess = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
        ],
        remainder="drop",
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    return X_train, X_test, y_train, y_test, preprocess
