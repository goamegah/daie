from __future__ import annotations

from pyspark.sql import functions as F

from daie.jobs.ml.data import load_data_from_table, DEFAULT_FEATURE_COLS

SOURCE_TABLE = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"

OUT_SCHEMA = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TRAIN_TABLE = f"{OUT_SCHEMA}.accident_train_v1"
TEST_TABLE  = f"{OUT_SCHEMA}.accident_test_v1"

def main(
    spark,
    source_table: str = SOURCE_TABLE,
    train_table: str = TRAIN_TABLE,
    test_table: str = TEST_TABLE,
    test_ratio: float = 0.2,
    seed: int = 42,
):
    df = load_data_from_table(
        spark=spark,
        table_name=source_table,
        feature_cols=DEFAULT_FEATURE_COLS,
        id_col="Num_Acc",
        target_col="grav",
    )

    agg_exprs = [
        F.first(c, ignorenulls=True).alias(c)
        for c in (DEFAULT_FEATURE_COLS + ["hrmn_minutes"])
    ] + [F.max("grav").alias("grav")]

    df_acc = df.groupBy("Num_Acc").agg(*agg_exprs).dropna()

    # Target binaire : grav >= 3 = grave
    df_acc = df_acc.withColumn("target", F.when(F.col("grav") >= 3, 1).otherwise(0))

    train_df, test_df = df_acc.randomSplit([1 - test_ratio, test_ratio], seed=seed)

    train_df.write.mode("overwrite").format("delta").saveAsTable(train_table)
    test_df.write.mode("overwrite").format("delta").saveAsTable(test_table)

    print(" Saved train:", train_table, "rows:", train_df.count())
    print(" Saved test :", test_table,  "rows:", test_df.count())

if __name__ == "__main__":
    main(spark)
