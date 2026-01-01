from __future__ import annotations

import mlflow
import mlflow.sklearn

from sklearn.metrics import accuracy_score, f1_score

OUT_SCHEMA = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TEST_TABLE = f"{OUT_SCHEMA}.accident_test_v1"
PRED_TABLE = f"{OUT_SCHEMA}.accident_test_predictions_v1"
META_TABLE = f"{OUT_SCHEMA}.ml_metadata_v1"

FEATURE_COLS = ["lum","atm","col","jour","mois","vma","circ","nbv","surf","infra","situ","hrmn_minutes"]

def main(
    spark,
    test_table: str = TEST_TABLE,
    pred_table: str = PRED_TABLE,
    meta_table: str = META_TABLE,
    model_run_id: str | None = None,
):
    if not model_run_id:
        meta = spark.table(meta_table).limit(1).collect()
        if not meta:
            raise ValueError(f"No metadata found in {meta_table}")
        model_run_id = meta[0]["model_run_id"]
        print(" loaded model_run_id:", model_run_id)

    model = mlflow.sklearn.load_model(f"runs:/{model_run_id}/model")
    scaler = mlflow.sklearn.load_model(f"runs:/{model_run_id}/scaler")

    sdf = spark.table(test_table).dropna()
    pdf = sdf.select("Num_Acc", *FEATURE_COLS, "target").toPandas()

    X = pdf[FEATURE_COLS]
    y = pdf["target"]

    Xs = scaler.transform(X)
    preds = model.predict(Xs)

    acc = accuracy_score(y, preds)
    f1 = f1_score(y, preds)

    with mlflow.start_run(run_name="accident_eval_v1") as run:
        mlflow.set_tag("trained_model_run_id", model_run_id)
        mlflow.log_metric("test_accuracy", acc)
        mlflow.log_metric("test_f1", f1)

        print(" test_accuracy:", acc, "test_f1:", f1)

    out = pdf[["Num_Acc"]].copy()
    out["prediction"] = preds

    pred_sdf = spark.createDataFrame(out)
    pred_sdf.write.mode("overwrite").format("delta").saveAsTable(pred_table)

    print(" saved predictions table:", pred_table)

if __name__ == "__main__":
    main(spark)
