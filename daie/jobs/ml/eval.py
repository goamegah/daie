# daie/jobs/ml/eval.py

import mlflow
import mlflow.sklearn
import pandas as pd

from sklearn.metrics import accuracy_score, f1_score

from daie.jobs.ml.config import (
    TABLE_TEST, TABLE_PRED, TABLE_META, FEATURE_COLS, TARGET_COL, MLFLOW_EXPERIMENT, ID_COL
)

def main(spark, model_run_id: str | None = None):
    # récup run_id depuis metadata si non fourni
    if not model_run_id:
        meta = spark.table(TABLE_META).limit(1).collect()
        if not meta:
            raise ValueError(f"No metadata found in {TABLE_META}")
        model_run_id = meta[0]["model_run_id"]
        print("Loaded model_run_id:", model_run_id)

    # recharge pipeline
    pipeline = mlflow.sklearn.load_model(f"runs:/{model_run_id}/model")

    sdf = spark.table(TABLE_TEST).dropna()
    pdf = sdf.select(ID_COL, *FEATURE_COLS, TARGET_COL).toPandas()

    # coerce numeric + drop NaN
    for c in FEATURE_COLS:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
    pdf = pdf.dropna(subset=FEATURE_COLS + [TARGET_COL])

    X = pdf[FEATURE_COLS]
    y = pdf[TARGET_COL]

    preds = pipeline.predict(X)

    acc = accuracy_score(y, preds)
    f1 = f1_score(y, preds)

    mlflow.set_experiment(MLFLOW_EXPERIMENT)
    with mlflow.start_run(run_name="eval_accident_severity") as run:
        mlflow.set_tag("trained_model_run_id", model_run_id)
        mlflow.log_metric("test_accuracy", acc)
        mlflow.log_metric("test_f1", f1)

    print("test_accuracy:", acc, "test_f1:", f1)

    out = pdf[[ID_COL]].copy()
    out["prediction"] = preds

    spark.createDataFrame(out).write.mode("overwrite").format("delta").saveAsTable(TABLE_PRED)
    print("Saved predictions table:", TABLE_PRED)

if __name__ == "__main__":
    main(spark)
