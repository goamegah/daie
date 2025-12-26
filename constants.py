from pathlib import Path

SEP = ";"
BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_ROOT = BASE_DIR / "data" / "bronze"
SILVER_ROOT = BASE_DIR / "data" / "silver"
GOLD_ROOT = BASE_DIR / "data" / "gold"
WAREHOUSE_DIR = str(BASE_DIR / "spark_warehouse")
KEY_CANDIDATES = ["num_acc", "Num_Acc", "NUM_ACC"]
DATASETS = {
    "caract": "caract-2023.csv",
    "lieux": "lieux-2023.csv",
    "usagers": "usagers-2023.csv",
    "vehicules": "vehicules-2023.csv",
}

LANDING = "../../data"
FILES = [
    "caract-2023.csv",
    "lieux-2023.csv",
    "usagers-2023.csv",
    "vehicules-2023.csv",
]
