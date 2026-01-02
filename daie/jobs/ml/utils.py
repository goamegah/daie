# daie/jobs/ml/utils.py

from pyspark.sql import functions as F

def ensure_schema(spark, catalog: str, schema: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

def add_hrmn_minutes(df):
    # hrmn est timestamp chez toi
    return df.withColumn("hrmn_minutes", F.hour("hrmn") * 60 + F.minute("hrmn"))

def build_binary_target(df, grav_col: str, out_col: str = "target"):
    # 1 si grav >= 3 else 0
    return df.withColumn(out_col, F.when(F.col(grav_col) >= 3, F.lit(1)).otherwise(F.lit(0)))

def clean_and_cast_numeric(df, cols):
    # cast en double (valeurs invalides -> null)
    for c in cols:
        df = df.withColumn(c, F.col(c).cast("double"))
    return df
