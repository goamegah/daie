# daie/jobs/ml/feature.py

from pyspark.sql import functions as F

from daie.jobs.ml.config import (
    TABLE_SOURCE, CATALOG, SCHEMA_ML, TABLE_TRAIN, TABLE_TEST,
    FEATURE_COLS, ID_COL, TARGET_COL_RAW, TARGET_COL, TEST_SIZE, RANDOM_STATE
)
from daie.jobs.ml.utils import ensure_schema, add_hrmn_minutes, clean_and_cast_numeric, build_binary_target

def main(spark):
    ensure_schema(spark, CATALOG, SCHEMA_ML)

    sdf = spark.table(TABLE_SOURCE)

    # 1) hrmn -> minutes
    sdf2 = add_hrmn_minutes(sdf)

    # 2) garder colonnes utiles
    keep = [ID_COL] + FEATURE_COLS + [TARGET_COL_RAW]
    sdf2 = sdf2.select(*[F.col(c) for c in keep]).dropna()

    # 3) 1 ligne / accident
    agg_exprs = [F.first(c, ignorenulls=True).alias(c) for c in FEATURE_COLS] + [
        F.max(TARGET_COL_RAW).alias(TARGET_COL_RAW)
    ]
    df_acc = sdf2.groupBy(ID_COL).agg(*agg_exprs)

    # 4) target binaire
    df_acc = build_binary_target(df_acc, TARGET_COL_RAW, TARGET_COL)

    # 5) cast numeric + drop rows invalides
    df_acc = clean_and_cast_numeric(df_acc, FEATURE_COLS).dropna(subset=FEATURE_COLS + [TARGET_COL])

    # 6) split
    train_df, test_df = df_acc.randomSplit([1 - TEST_SIZE, TEST_SIZE], seed=RANDOM_STATE)

    # 7) save tables
    train_df.write.mode("overwrite").format("delta").saveAsTable(TABLE_TRAIN)
    test_df.write.mode("overwrite").format("delta").saveAsTable(TABLE_TEST)

    print(f"Saved train: {TABLE_TRAIN} rows: {train_df.count()}")
    print(f"Saved test : {TABLE_TEST} rows: {test_df.count()}")

if __name__ == "__main__":
    main(spark)
