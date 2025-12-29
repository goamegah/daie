from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import dbutils

spark = SparkSession.builder.getOrCreate()

YEAR = "2023"
SCHEMA = "accidents"

SILVER_CATALOG = "daie_chn_dev_silver"
GOLD_CATALOG   = "daie_chn_dev_gold"

V_SILVER = "silver"
V_GOLD   = "gold"

SILVER_ROOT = f"/Volumes/{SILVER_CATALOG}/{SCHEMA}/{V_SILVER}/baac/{YEAR}"
GOLD_ROOT   = f"/Volumes/{GOLD_CATALOG}/{SCHEMA}/{V_GOLD}/baac/{YEAR}"

KEY = "num_acc"

def normalize_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])

def write_gold_table(df, rel_path: str, table_name: str):
    """
    Ecrit df en Delta dans le volume Gold + crée table UC pointant vers la location.
    rel_path : sous-dossier dans GOLD_ROOT
    table_name : nom complet UC, ex: daie_chn_dev_gold.accidents.accidents_by_dep_2023
    """
    path = f"{GOLD_ROOT}/{rel_path}"
    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path))
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")
    return path


spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{SCHEMA}")
dbutils.fs.mkdirs(GOLD_ROOT)


caract    = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._caract_{YEAR}"))
lieux     = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._lieux_{YEAR}"))
usagers   = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._usagers_{YEAR}"))
vehicules = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._vehicules_{YEAR}"))

accident = caract.join(lieux, on=KEY, how="inner")

accident_table = f"{GOLD_CATALOG}.{SCHEMA}.accident_{YEAR}"
accident_path = write_gold_table(accident, "accident", accident_table)
print(f"Created {accident_table} -> {accident_path} (rows={accident.count()})")

usagers_table = f"{GOLD_CATALOG}.{SCHEMA}.usagers_{YEAR}"
vehicules_table = f"{GOLD_CATALOG}.{SCHEMA}.vehicules_{YEAR}"

write_gold_table(usagers, "usagers", usagers_table)
write_gold_table(vehicules, "vehicules", vehicules_table)
print(f"Created {usagers_table} and {vehicules_table}")

# AGGREGATIONS

# ccidents by department
if "dep" in accident.columns:
    acc_by_dep = (
        accident.groupBy("dep")
        .agg(F.count("*").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_dep_{YEAR}"
    write_gold_table(acc_by_dep, "accidents_by_dep", t)
    print(f"Created {t}")
else:
    print("dep not found -> skip accidents_by_dep")

# by dep n grav
if "dep" in accident.columns and "grav" in accident.columns:
    acc_dep_grav = (
        accident.groupBy("dep", "grav")
        .agg(F.count("*").alias("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_dep_grav_{YEAR}"
    write_gold_table(acc_dep_grav, "accidents_by_dep_grav", t)
    print(f"✅ Created {t}")
else:
    print("dep/grav not found -> skip accidents_by_dep_grav")

#by weather 
if "atm" in accident.columns:
    acc_by_atm = (
        accident.groupBy("atm")
        .agg(F.count("*").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_weather_{YEAR}"
    write_gold_table(acc_by_atm, "accidents_by_weather", t)
    print(f"Created {t}")
else:
    print("atm not found -> skip accidents_by_weather")

if "lum" in accident.columns:
    acc_by_lum = (
        accident.groupBy("lum")
        .agg(F.count("*").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_lum_{YEAR}"
    write_gold_table(acc_by_lum, "accidents_by_lum", t)
    print(f"✅ Created {t}")
else:
    print("lum not found -> skip accidents_by_lum")

# victims per accident + join dep (optional)
if KEY in usagers.columns:
    victims_per_acc = (
        usagers.groupBy(KEY)
        .agg(F.count("*").alias("nb_victimes"))
    )

    if "dep" in accident.columns:
        victims_dep = (
            victims_per_acc
            .join(accident.select(KEY, "dep").dropDuplicates([KEY]), on=KEY, how="left")
            .groupBy("dep")
            .agg(
                F.avg("nb_victimes").alias("avg_victimes_per_accident"),
                F.sum("nb_victimes").alias("total_victimes"),
                F.countDistinct(KEY).alias("nb_accidents")
            )
            .orderBy(F.desc("total_victimes"))
        )
        t = f"{GOLD_CATALOG}.{SCHEMA}.victims_stats_by_dep_{YEAR}"
        write_gold_table(victims_dep, "victims_stats_by_dep", t)
        print(f"created {t}")
    else:
        t = f"{GOLD_CATALOG}.{SCHEMA}.victims_per_accident_{YEAR}"
        write_gold_table(victims_per_acc, "victims_per_accident", t)
        print(f"Created {t}")
else:
    print("num_acc not found in usagers -> skip victims KPIs")

# accidents by vehicle type using vehicules
if KEY in vehicules.columns and "catv" in vehicules.columns:
    acc_by_vehicle = (
        vehicules.groupBy("catv")
        .agg(F.countDistinct(KEY).alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_vehicle_{YEAR}"
    write_gold_table(acc_by_vehicle, "accidents_by_vehicle", t)
    print(f"Created {t}")
else:
    print("catv/num_acc not found -> skip accidents_by_vehicle")

print("JOB 3 OK")
