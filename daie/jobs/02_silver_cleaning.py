from pyspark.sql import functions as F
from dbx_config import *
from pyspark.sql import SparkSession
import dbutils

spark = SparkSession.builder.getOrCreate()

DATASETS = {
    "caract": "caract-2023.csv",
    "lieux": "lieux-2023.csv",
    "usagers": "usagers-2023.csv",
    "vehicules": "vehicules-2023.csv",
}

BRONZE_ROOT = vol_path(BRONZE_CATALOG, SCHEMA, V_BRONZE, "baac", YEAR)
SILVER_ROOT = vol_path(SILVER_CATALOG, SCHEMA, V_SILVER, "baac", YEAR)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SCHEMA}")
dbutils.fs.mkdirs(SILVER_ROOT)

def read_csv(path: str):
    return (spark.read.format("csv")
            .option("header", "true")
            .option("sep", SEP)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .load(path))

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

#underscore tables
for name, filename in DATASETS.items():
    in_path = f"{BRONZE_ROOT}/{filename}"
    df = normalize_lat_long(trim_all(normalize_columns(read_csv(in_path))))

    out_path = f"{SILVER_ROOT}/{name}"
    table = f"{SILVER_CATALOG}.{SCHEMA}._{name}_{YEAR}"

    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out_path))

    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} USING DELTA LOCATION '{out_path}'")

    print(f"created {table}")

#curated features table (caract + usagers)
caract = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._caract_{YEAR}"))
usagers = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._usagers_{YEAR}"))

feature_cols = ["num_acc", "lum", "atm", "col", "int", "hrmn"]
caract = caract.select(*[c for c in feature_cols if c in caract.columns])
usagers = usagers.select(*[c for c in ["num_acc", "grav"] if c in usagers.columns])

caract = (caract
    .withColumn("hrmn_str", F.col("hrmn").cast("string"))
    .withColumn("hh", F.split(F.col("hrmn_str"), ":").getItem(0).cast("int"))
    .withColumn("mm", F.split(F.col("hrmn_str"), ":").getItem(1).cast("int"))
    .withColumn("hrmn_min",
        F.when(F.col("hh").isNotNull() & F.col("mm").isNotNull(), F.col("hh")*60 + F.col("mm"))
         .otherwise(F.lit(None).cast("int"))
    )
    .drop("hrmn_str", "hh", "mm")
)

grav_per_acc = (usagers
    .withColumn("grav_i", F.col("grav").cast("int"))
    .groupBy("num_acc")
    .agg(F.max("grav_i").alias("grav_max"))
)

features = caract.join(grav_per_acc, "num_acc", "inner").dropna()

features_path = f"{SILVER_ROOT}/accidents_features"
features_table = f"{SILVER_CATALOG}.{SCHEMA}.accidents_features_{YEAR}"

(features.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(features_path))

spark.sql(f"DROP TABLE IF EXISTS {features_table}")
spark.sql(f"CREATE TABLE {features_table} USING DELTA LOCATION '{features_path}'")

print(f"created {features_table}")
