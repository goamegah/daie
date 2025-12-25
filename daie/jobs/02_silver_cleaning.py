import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F

SEP = ";"
BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_ROOT = BASE_DIR / "data" / "bronze"
SILVER_ROOT = BASE_DIR / "data" / "silver"
WAREHOUSE_DIR = str(BASE_DIR / "spark_warehouse")


DATASETS = {
    "caract": "caract-2023.csv",
    "lieux": "lieux-2023.csv",
    "usagers": "usagers-2023.csv",
    "vehicules": "vehicules-2023.csv",
}

def get_spark(app_name: str):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    print("Spark:", spark.version)
    print("Scala:", spark.sparkContext._jvm.scala.util.Properties.versionNumberString())
    return spark


def read_csv(spark, path: str):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", SEP)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .load(path)
    )

def normalize_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])

def trim_all(df):
    for c in df.columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df

def normalize_lat_long(df):
    if "lat" in df.columns:
        df = df.withColumn("lat_d", F.regexp_replace(F.col("lat"), ",", ".").cast("double"))
    if "long" in df.columns:
        df = df.withColumn("long_d", F.regexp_replace(F.col("long"), ",", ".").cast("double"))
    return df


def build_features_table(spark):
    caract = normalize_columns(
        spark.read.format("delta").load(str(SILVER_ROOT / "caract"))
    )

    usagers = normalize_columns(
        spark.read.format("delta").load(str(SILVER_ROOT / "usagers"))
    )


    feature_cols = ["num_acc", "lum", "atm", "col", "int", "hrmn"]
    caract = caract.select(*[c for c in feature_cols if c in caract.columns])

    usagers = usagers.select(*[c for c in ["num_acc", "grav"] if c in usagers.columns])

    # Convert hrmn "HH:MM" -> minutes
    caract = (
        caract
        .withColumn("hrmn_str", F.col("hrmn").cast("string"))
        .withColumn("hh", F.split(F.col("hrmn_str"), ":").getItem(0).cast("int"))
        .withColumn("mm", F.split(F.col("hrmn_str"), ":").getItem(1).cast("int"))
        .withColumn(
            "hrmn_min",
            F.when(
                F.col("hh").isNotNull() & F.col("mm").isNotNull(),
                F.col("hh") * F.lit(60) + F.col("mm")
            ).otherwise(F.lit(None).cast("int"))
        )
        .drop("hrmn_str", "hh", "mm")
    )

    # grav per accident
    grav_per_acc = (
        usagers
        .withColumn("grav_i", F.col("grav").cast("int"))
        .groupBy("num_acc")
        .agg(F.max("grav_i").alias("grav_max"))
    )

    df = caract.join(grav_per_acc, on="num_acc", how="inner").dropna()
    return df

if __name__ == "__main__":
    os.makedirs(SILVER_ROOT, exist_ok=True)

    spark = get_spark("job2-silver")

    # bronze -> silver
    for name, filename in DATASETS.items():
        in_path = BRONZE_ROOT / filename
        if not in_path.exists():
            raise FileNotFoundError(f"Bronze file not found: {in_path}")

        df = read_csv(spark, str(in_path))
        df = normalize_columns(df)
        df = trim_all(df)
        df = normalize_lat_long(df)

        out_path = SILVER_ROOT / name
        df.write.format("delta").mode("overwrite").save(str(out_path))
        print(f"Silver written at {out_path} (rows={df.count()})")


        print(f"silver._{name}_2023 created (rows={df.count()})")

    features = build_features_table(spark)

    features_path = SILVER_ROOT / "accidents_features"
    features.write.format("delta").mode("overwrite").save(str(features_path))
    print(f"Features written at {features_path} (rows={features.count()})")

    print(f"silver.accidents_features_2023 created (rows={features.count()})")
    features.show(5, truncate=False)

    spark.stop()
    print("JOB 2 OK")
