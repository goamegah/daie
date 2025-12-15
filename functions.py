import constants as C
from spark_session import spark
from pyspark.sql.functions import col, regexp_replace, trim

SEP = ";"

def normalize_lat_long(df):
    if "lat" in df.columns:
        df = df.withColumn("lat_d", regexp_replace(trim(col("lat")), ",", ".").cast("double"))
    if "long" in df.columns:
        df = df.withColumn("long_d", regexp_replace(trim(col("long")), ",", ".").cast("double"))
    return df

def read_bronze_csv(name):
    path = f"{C.BRONZE_ROOT}/{name}"
    return (spark.read.format("csv")
        .option("header", "true")
        .option("sep", SEP)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .load(path))

