from pyspark.sql import functions as F

SOURCE_TABLE = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"
DEST_DB = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TRAIN_TABLE = f"{DEST_DB}.accident_train_v1"
TEST_TABLE  = f"{DEST_DB}.accident_test_v1"

FEATURE_COLS = ["lum", "atm", "col", "int", "hrmn"]

def main():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEST_DB}")
    df = spark.table(SOURCE_TABLE)

    # --- hrmn safe cast ---
    df = df.withColumn(
        "hrmn",
        F.when(
            F.col("hrmn").contains(":"),
            F.expr("""
              try_cast(split(hrmn, ':')[0] as int) * 60
            + try_cast(split(hrmn, ':')[1] as int)
            """)
        ).otherwise(F.expr("try_cast(hrmn as int)"))
    )

    for c in ["lum", "atm", "col", "int"]:
        df = df.withColumn(c, F.expr(f"try_cast({c} as int)"))

    # target
    df = df.withColumn("target", F.when(F.col("grav") >= 3, F.lit(1)).otherwise(F.lit(0)))

    # garder colonnes utiles
    keep = ["num_acc", "grav", "target"] + FEATURE_COLS
    df = df.select(*keep).dropna(subset=keep).dropDuplicates(["num_acc"])

    # split stratifié
    fractions = {0: 0.8, 1: 0.8}
    train = df.stat.sampleBy("target", fractions=fractions, seed=42)
    test = df.join(train.select("num_acc"), on="num_acc", how="left_anti")

    # write
    (train.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(TRAIN_TABLE))
    (test.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(TEST_TABLE))

    print("Written:")
    print(" -", TRAIN_TABLE, train.count())
    print(" -", TEST_TABLE, test.count())

if __name__ == "__main__":
    main()
