# JOB 1 — LANDING → BRONZE

CATALOG = "daie_chn_dev_bronze"
SCHEMA = "accidents"
YEAR = "2023"

LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/landing/baac/{YEAR}"
BRONZE_ROOT  = f"/Volumes/{CATALOG}/{SCHEMA}/bronze/baac/{YEAR}"

FILES = [
    "caract-2023.csv",
    "lieux-2023.csv",
    "usagers-2023.csv",
    "vehicules-2023.csv"
]

dbutils.fs.mkdirs(BRONZE_ROOT)

for f in FILES:
    dbutils.fs.cp(
        f"{LANDING_ROOT}/{f}",
        f"{BRONZE_ROOT}/{f}",
        recurse=True
    )

display(dbutils.fs.ls(BRONZE_ROOT))
print("JOB 1 DONE")
