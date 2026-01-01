from __future__ import annotations

import mlflow
import mlflow.sklearn

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, f1_score

from daie.jobs.ml.model import get_model

OUT_SCHEMA = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TRAIN_TABLE = f"{OUT_SCHEMA}.accident_train_v1"
META_TABLE  = f"{OUT_SCHEMA}.ml_metadata_v1"

FEATURE_COLS = ["lum","atm","col","jour","mois","vma","circ","nbv","surf","infra","situ","hrmn_minutes"]

def main(
    spark,
    train_table: str = TRAIN_TABLE,
    meta_table: str = META_TABLE,
    run_name: str = "accident_train_v1",
    n_estimators: int = 200,
):
    sdf = spark.table(train_table).dropna()

    pdf = sdf.select(*FEATURE_COLS, "target").toPandas()
    X = pdf[FEATURE_COLS]
    y = pdf["target"]

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    model = get_model(n_estimators=n_estimators)

    with mlflow.start_run(run_name=run_name) as run:
        model.fit(Xs, y)
        preds = model.predict(Xs)

        acc = accuracy_score(y, preds)
        f1 = f1_score(y, preds)

        mlflow.log_param("model", "RandomForestClassifier")
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("features", ",".join(FEATURE_COLS))
        mlflow.log_metric("train_accuracy", acc)
        mlflow.log_metric("train_f1", f1)

        mlflow.sklearn.log_model(model, "model")
        mlflow.sklearn.log_model(scaler, "scaler")

        # run_id partagé via une table delta
        meta_df = spark.createDataFrame([{
            "model_run_id": run.info.run_id,
            "run_name": run_name,
        }])
        meta_df.write.mode("overwrite").format("delta").saveAsTable(meta_table)

        print(" train_accuracy:", acc, "train_f1:", f1)
        print(" saved metadata:", meta_table, "run_id:", run.info.run_id)

        return run.info.run_id

if __name__ == "__main__":
    main(spark)
