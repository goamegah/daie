BRONZE_ROOT = "data/bronze/accidents/2023"

SILVER_ROOT = "data/silver/accidents/2023"

GOLD_ROOT   = "data/gold/accidents/2023"

SEP = ";"
BRONZE_ROOT = BASE_DIR / "data" / "bronze"
SILVER_ROOT = BASE_DIR / "data" / "silver"
WAREHOUSE_DIR = str(BASE_DIR / "spark_warehouse")


DATASETS = {
    "caract": "caract-2023.csv",
    "lieux": "lieux-2023.csv",
    "usagers": "usagers-2023.csv",
    "vehicules": "vehicules-2023.csv",
}