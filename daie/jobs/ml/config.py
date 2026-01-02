# daie/jobs/ml/config.py

CATALOG = "daie_chn_dev_gold"
SCHEMA_ML = "dev_ml_accident_severity_prediction"

TABLE_SOURCE = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"

TABLE_TRAIN = f"{CATALOG}.{SCHEMA_ML}.accident_train_v1"
TABLE_TEST  = f"{CATALOG}.{SCHEMA_ML}.accident_test_v1"
TABLE_PRED  = f"{CATALOG}.{SCHEMA_ML}.accident_test_predictions_v1"
TABLE_META  = f"{CATALOG}.{SCHEMA_ML}.ml_metadata_v1"

ID_COL = "Num_Acc"
TARGET_COL_RAW = "grav"
TARGET_COL = "target"

FEATURE_COLS = ["lum","atm","col","jour","mois","vma","circ","nbv","surf","infra","situ","hrmn_minutes"]

TEST_SIZE = 0.2
RANDOM_STATE = 42

# mets ton chemin d’experiment si tu veux
MLFLOW_EXPERIMENT = "/Shared/accident_severity_prediction"
