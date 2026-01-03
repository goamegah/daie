# JOB 3 — SILVER -> GOLD

from pyspark.sql import functions as F

YEAR = "2023"
SCHEMA = "accidents"

SILVER_CATALOG = "daie_chn_dev_silver"
GOLD_CATALOG   = "daie_chn_dev_gold"

KEY = "num_acc"

def normalize_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])

def save_table(df, full_table_name: str):
    """
    Write as managed Delta table in Unity Catalog (no LOCATION needed).
    """
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_table_name))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.{SCHEMA}")

#runderscore tables ---
caract    = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._caract_{YEAR}"))
lieux     = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._lieux_{YEAR}"))
usagers   = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._usagers_{YEAR}"))
vehicules = normalize_columns(spark.table(f"{SILVER_CATALOG}.{SCHEMA}._vehicules_{YEAR}"))

accident = caract.join(lieux, on=KEY, how="inner")

accident_table = f"{GOLD_CATALOG}.{SCHEMA}.accident_{YEAR}"
usagers_table  = f"{GOLD_CATALOG}.{SCHEMA}.usagers_{YEAR}"
vehicules_table= f"{GOLD_CATALOG}.{SCHEMA}.vehicules_{YEAR}"

save_table(accident, accident_table)
save_table(usagers, usagers_table)
save_table(vehicules, vehicules_table)

print(f"Created {accident_table} (rows={accident.count()})")
print(f"Created {usagers_table}")
print(f"Created {vehicules_table}")

# AGGREGATIONS 

#accidents by department
if "dep" in accident.columns:
    acc_by_dep = (
        accident.groupBy("dep")
        .agg(F.count("*").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_dep_{YEAR}"
    save_table(acc_by_dep, t)
    print(f"Created {t}")
else:
    print("dep not found -> skip accidents_by_dep")

#accidents by department + gravity
if "dep" in accident.columns and "grav" in accident.columns:
    acc_dep_grav = (
        accident.groupBy("dep", "grav")
        .agg(F.count("*").alias("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_dep_grav_{YEAR}"
    save_table(acc_dep_grav, t)
    print(f"Created {t}")
else:
    print("dep/grav not found -> skip accidents_by_dep_grav")

#accidents by weather (atm)
if "atm" in accident.columns:
    acc_by_atm = (
        accident.groupBy("atm")
        .agg(F.count("*").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_weather_{YEAR}"
    save_table(acc_by_atm, t)
    print(f"Created {t}")
else:
    print("atm not found -> skip accidents_by_weather")

# 4) Accidents by light conditions (lum)
if "lum" in accident.columns:
    acc_by_lum = (
        accident.groupBy("lum")
        .agg(F.count("*").alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_lum_{YEAR}"
    save_table(acc_by_lum, t)
    print(f"Created {t}")
else:
    print("lum not found -> skip accidents_by_lum")

#victims stats + join dep
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
        save_table(victims_dep, t)
        print(f"Created {t}")
    else:
        t = f"{GOLD_CATALOG}.{SCHEMA}.victims_per_accident_{YEAR}"
        save_table(victims_per_acc, t)
        print(f"Created {t}")
else:
    print("ℹ️ num_acc not found in usagers -> skip victims KPIs")

# accidents by vehicle type (catv) using vehicules
if KEY in vehicules.columns and "catv" in vehicules.columns:
    acc_by_vehicle = (
        vehicules.groupBy("catv")
        .agg(F.countDistinct(KEY).alias("nb_accidents"))
        .orderBy(F.desc("nb_accidents"))
    )
    t = f"{GOLD_CATALOG}.{SCHEMA}.accidents_by_vehicle_{YEAR}"
    save_table(acc_by_vehicle, t)
    print(f"Created {t}")
else:
    print("catv/num_acc not found -> skip accidents_by_vehicle")

print("JOB 3 OK")
