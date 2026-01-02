# daie/jobs/ml/feature.py
from pyspark.sql import functions as F

SOURCE_TABLE = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"
DEST_DB = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TRAIN_TABLE = f"{DEST_DB}.accident_train_v1"
TEST_TABLE  = f"{DEST_DB}.accident_test_v1"

FEATURE_COLS = ["lum", "atm", "col", "jour", "mois", "vma", "circ", "nbv", "surf", "infra", "situ", "hrmn_minutes"]

def _normalize_cols(df):
    for c in df.columns:
        if c.lower() != c:
            df = df.withColumnRenamed(c, c.lower())
    return df

def main(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEST_DB}")

    sdf = spark.table(SOURCE_TABLE)
    sdf = _normalize_cols(sdf)

    # --- hrmn_minutes
    # 1) on convertit hrmn en string pour tester si c’est "HH:MM"
    hrmn_str = F.col("hrmn").cast("string")
    # parse HH:MM
    hhmm_minutes = (
        F.expr("try_cast(split(cast(hrmn as string), ':')[0] as int)") * 60
        + F.expr("try_cast(split(cast(hrmn as string), ':')[1] as int)")
    )
    # parse timestamp/time via to_timestamp
    hrmn_ts = F.to_timestamp(F.col("hrmn"))
    ts_minutes = F.hour(hrmn_ts) * 60 + F.minute(hrmn_ts)

    sdf2 = sdf.withColumn(
        "hrmn_minutes",
        F.when(hrmn_str.contains(":"), hhmm_minutes).otherwise(ts_minutes)
    )

    # --- agrégation 1 ligne par accident
    feature_cols_src = ["lum", "atm", "col", "jour", "mois", "vma", "circ", "nbv", "surf", "infra", "situ", "hrmn_minutes"]

    agg_exprs = [F.first(c, ignorenulls=True).alias(c) for c in feature_cols_src] + [
        F.max("grav").alias("grav")
    ]

    df_acc = sdf2.groupBy("num_acc").agg(*agg_exprs)

    df_acc = df_acc.withColumn("target", F.when(F.col("grav") >= 3, F.lit(1)).otherwise(F.lit(0)))

    keep = ["num_acc", "grav", "target"] + FEATURE_COLS
    df_acc = df_acc.select(*keep)

    df_acc = df_acc.dropna()

    fractions = {0: 0.8, 1: 0.8}
    train = df_acc.stat.sampleBy("target", fractions=fractions, seed=42)
    test = df_acc.join(train.select("num_acc"), on="num_acc", how="left_anti")

    (train.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(TRAIN_TABLE))
    (test.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(TEST_TABLE))

    print("Written:")
    print(" -", TRAIN_TABLE, train.count())
    print(" -", TEST_TABLE, test.count())

if __name__ == "__main__":
    main
