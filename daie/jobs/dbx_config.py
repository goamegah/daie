import os

BRONZE_CATALOG = os.getenv("BRONZE_CATALOG", "daie_chn_dev_bronze")
SILVER_CATALOG = os.getenv("SILVER_CATALOG", "daie_chn_dev_silver")
GOLD_CATALOG   = os.getenv("GOLD_CATALOG",   "daie_chn_dev_gold")

SCHEMA = os.getenv("SCHEMA", "accidents")

V_LANDING = os.getenv("V_LANDING", "landing")
V_BRONZE  = os.getenv("V_BRONZE",  "bronze")
V_SILVER  = os.getenv("V_SILVER",  "silver")
V_GOLD    = os.getenv("V_GOLD",    "gold")

YEAR = os.getenv("YEAR", "2023")
SEP  = os.getenv("SEP",  ";")

def vol_path(catalog: str, schema: str, volume: str, *parts: str) -> str:
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    if parts:
        return base + "/" + "/".join([p.strip("/") for p in parts])
    return base
