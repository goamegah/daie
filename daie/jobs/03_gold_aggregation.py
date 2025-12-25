import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from constants import *
from spark_session import get_spark


def normalize_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])

def detect_key(cols):
    cols_set = set(cols)
    for k in KEY_CANDIDATES:
        if k.lower() in cols_set:
            return k.lower()
    return None

def read_silver_delta(spark, name: str):
    path = SILVER_ROOT / name
    if not path.exists():
        raise FileNotFoundError(f"Silver delta folder not found: {path}")
    return spark.read.format("delta").load(str(path))

def main():
    os.makedirs(GOLD_ROOT, exist_ok=True)

    spark = get_spark("job3-gold-local")

    #read silver
    caract = normalize_columns(read_silver_delta(spark, "caract"))
    lieux  = normalize_columns(read_silver_delta(spark, "lieux"))
    usagers = normalize_columns(read_silver_delta(spark, "usagers"))
    vehicules = normalize_columns(read_silver_delta(spark, "vehicules"))

    key1 = detect_key(caract.columns)
    key2 = detect_key(lieux.columns)
    if not key1 or not key2 or key1 != key2:
        raise ValueError(f"Clé join introuvable ou différente. caract:{key1}, lieux:{key2}")

    KEY = key1  # "num_acc"

    accident = caract.join(lieux, on=KEY, how="inner")

    acc_path = GOLD_ROOT / "accident"
    accident.write.format("delta").mode("overwrite").save(str(acc_path))
    print(f"Gold written: {acc_path} (rows={accident.count()})")

    usa_path = GOLD_ROOT / "usagers"
    veh_path = GOLD_ROOT / "vehicules"

    usagers.write.format("delta").mode("overwrite").save(str(usa_path))
    vehicules.write.format("delta").mode("overwrite").save(str(veh_path))

    print(f"Gold written: {usa_path} (rows={usagers.count()})")
    print(f"Gold written: {veh_path} (rows={vehicules.count()})")

    cols = set(accident.columns)
    if "dep" in cols and "grav" in cols:
        agg = accident.groupBy("dep", "grav").agg(F.count("*").alias("nb_accidents"))
        agg_path = GOLD_ROOT / "agg_dep_grav"
        agg.write.format("delta").mode("overwrite").save(str(agg_path))
        print(f"Gold aggregation written: {agg_path} (rows={agg.count()})")
    else:
        print("colonnes dep ou grav absentes dans accident.")

    spark.stop()
    print("JOB 3 OK")

if __name__ == "__main__":
    main()
