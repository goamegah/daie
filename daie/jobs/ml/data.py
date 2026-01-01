from __future__ import annotations

from typing import List, Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DEFAULT_FEATURE_COLS: List[str] = [
    "lum",
    "atm",
    "col",
    "jour",
    "mois",
    "vma",
    "circ",
    "nbv",
    "surf",
    "infra",
    "situ",
]


def build_accident_level_df(
    sdf: DataFrame,
    feature_cols: Optional[List[str]] = None,
    id_col: str = "Num_Acc",
    target_col: str = "grav",
) -> DataFrame:
    """
    Transform the joined table (accident_v1) into one row per accident:
      - Create hrmn_minutes from hrmn timestamp
      - Aggregate features with first(non-null)
      - Aggregate target with max(grav)
    """
    if feature_cols is None:
        feature_cols = DEFAULT_FEATURE_COLS

    # hrmn timestamp -> minutes in day
    sdf2 = sdf.withColumn("hrmn_minutes", F.hour("hrmn") * 60 + F.minute("hrmn"))

    final_features = feature_cols + ["hrmn_minutes"]

    # Ensure columns exist
    missing = [c for c in final_features + [id_col, target_col] if c not in sdf2.columns]
    if missing:
        raise ValueError(f"Missing columns in source table: {missing}")

    agg_exprs = [F.first(c, ignorenulls=True).alias(c) for c in final_features] + [
        F.max(target_col).alias(target_col)
    ]

    df_acc = sdf2.groupBy(id_col).agg(*agg_exprs).dropna()
    return df_acc


def load_data_from_table(
    spark,
    table_name: str,
    feature_cols: Optional[List[str]] = None,
    id_col: str = "Num_Acc",
    target_col: str = "grav",
) -> pd.DataFrame:
    """
    Load Databricks table into a pandas DataFrame at accident level (1 row per accident).
    """
    sdf = spark.table(table_name)
    df_acc = build_accident_level_df(sdf, feature_cols=feature_cols, id_col=id_col, target_col=target_col)

    pdf = df_acc.toPandas()
    # normalize col names to lower-case for sklearn
    pdf.columns = [c.lower() for c in pdf.columns]
    return pdf.dropna()
