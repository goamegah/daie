from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_TABLE = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"

DEFAULT_FEATURE_COLS = [
    "lum", "atm", "col", "jour", "mois",
    "vma", "circ", "nbv", "surf", "infra", "situ",
]

def load_data_from_table(
    spark,
    table_name: str = SOURCE_TABLE,
    feature_cols: list[str] | None = None,
    id_col: str = "Num_Acc",
    target_col: str = "grav",
) -> DataFrame:
    if feature_cols is None:
        feature_cols = DEFAULT_FEATURE_COLS

    df = spark.table(table_name)

    df = df.withColumn("hrmn_minutes", F.hour("hrmn") * 60 + F.minute("hrmn"))

    cols = [id_col] + feature_cols + ["hrmn_minutes", target_col]
    df = df.select(*cols)

    df = df.dropna()

    return df
