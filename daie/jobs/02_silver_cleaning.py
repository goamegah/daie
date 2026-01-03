# JOB 2 — BRONZE → SILVER

from pyspark.sql import functions as F

CATALOG_BRONZE = "daie_chn_dev_bronze"
CATALOG_SILVER = "daie_chn_dev_silver"
SCHEMA = "accidents"
YEAR = "2023"
SEP = ";"

BRONZE_ROOT = f"/Volumes/{CATALOG_BRONZE}/{SCHEMA}/bronze/baac/{YEAR}"
SILVER_ROOT = f"/Volumes/{CATALOG_SILVER}/{SCHEMA}/silver/baac/{YEAR}"

DATASETS = {
    "caract": "caract-2023.csv",
    "lieux": "lieux-2023.csv",
    "usagers": "usagers-2023.csv",
    "vehicules": "vehicules-2023.csv"
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SILVER}.{SCHEMA}")
dbutils.fs.mkdirs(SILVER_ROOT)

def read_csv(path):
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", SEP)
        .option("quote", '"')
        .option("escape", '"')
        .load(path)
    )

def normalize_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])

def clean_df(df):
    for c in df.columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df

# create underscore tables
for name, filename in DATASETS.items():
    in_path = f"{BRONZE_ROOT}/{filename}"

    df = read_csv(in_path)
    df = normalize_columns(df)
    df = clean_df(df)

    table = f"{CATALOG_SILVER}.{SCHEMA}._{name}_{YEAR}"

    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table))

    print(f"created {table} rows={df.count()}")

# build features table
caract = spark.table(f"{CATALOG_SILVER}.{SCHEMA}._caract_{YEAR}")
usagers = spark.table(f"{CATALOG_SILVER}.{SCHEMA}._usagers_{YEAR}")

caract = normalize_columns(caract)
usagers = normalize_columns(usagers)

caract = caract.select("num_acc", "lum", "atm", "col", "int", "hrmn")
usagers = usagers.select("num_acc", "grav")

caract = (
    caract
    .withColumn("hh", F.split("hrmn", ":").getItem(0).cast("int"))
    .withColumn("mm", F.split("hrmn", ":").getItem(1).cast("int"))
    .withColumn("hrmn_min", F.col("hh") * 60 + F.col("mm"))
    .drop("hh", "mm")
)

grav = usagers.groupBy("num_acc").agg(F.max("grav").alias("grav_max"))

features = caract.join(grav, "num_acc", "inner").dropna()

features_path = f"{SILVER_ROOT}/accidents_features"
features_table = f"{CATALOG_SILVER}.{SCHEMA}.accidents_features_{YEAR}"

(features.write.format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(features_table))

display(features.limit(10))
print("JOB 2 DONE")
