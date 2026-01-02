from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


TABLE = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"

DEST_DB = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TRAIN_TABLE = f"{DEST_DB}.accident_train_v1"
TEST_TABLE  = f"{DEST_DB}.accident_test_v1"

FEATURE_COLS = ["lum","atm","col","jour","mois","vma","circ","nbv","surf","infra","situ","hrmn_minutes"]

def _lower_cols(df):
    for c in df.columns:
        if c.lower() != c:
            df = df.withColumnRenamed(c, c.lower())
    return df

def main():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEST_DB}")

    sdf = spark.table(TABLE)
    sdf = _lower_cols(sdf)

    sdf2 = sdf.withColumn("hrmn_minutes", F.hour("hrmn") * 60 + F.minute("hrmn"))

    agg_exprs = [F.first(c, ignorenulls=True).alias(c) for c in FEATURE_COLS] + [
        F.max("grav").alias("grav")
    ]
    df_acc = sdf2.groupBy("num_acc").agg(*agg_exprs).dropna()

    df_acc = df_acc.withColumn("target", F.when(F.col("grav") >= 3, F.lit(1)).otherwise(F.lit(0)))

    fractions = {0: 0.8, 1: 0.8}
    train = df_acc.stat.sampleBy("target", fractions=fractions, seed=42)
    test = df_acc.join(train.select("num_acc"), on="num_acc", how="left_anti")

    (train.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(TRAIN_TABLE))
    (test.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(TEST_TABLE))

    print("Written:")
    print(" -", TRAIN_TABLE, train.count())
    print(" -", TEST_TABLE, test.count())

if __name__ == "__main__":
    main()
