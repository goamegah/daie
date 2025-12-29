from dbx_config import *
import dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

FILES = ["caract-2023.csv", "lieux-2023.csv", "usagers-2023.csv", "vehicules-2023.csv"]

LANDING_ROOT = vol_path(BRONZE_CATALOG, SCHEMA, V_LANDING, "baac", YEAR)
BRONZE_ROOT  = vol_path(BRONZE_CATALOG, SCHEMA, V_BRONZE,  "baac", YEAR)

dbutils.fs.mkdirs(BRONZE_ROOT)

for f in FILES:
    dbutils.fs.cp(f"{LANDING_ROOT}/{f}", f"{BRONZE_ROOT}/{f}", True)

print("Bronze files:", [x.path for x in dbutils.fs.ls(BRONZE_ROOT)])
