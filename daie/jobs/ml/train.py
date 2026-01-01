from __future__ import annotations

import mlflow
import mlflow.sklearn
from sklearn.metrics import accuracy_score, f1_score

from daie.jobs.ml.data import load_data_from_table, DEFAULT_FEATURE_COLS
from daie.jobs.ml.feature import make_features
from daie.jobs.ml.model import get_model


TABLE_NAME = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"


def main(
    spark=None,
    table_name: str = TABLE_NAME,
    run_name: str = "accident_gravity_prediction_agg",
):
    if spark is None:
        try:
            spark  
        except NameError:
            raise RuntimeError(
                "Spark session 'spark' not found. This entrypoint is meant to run on Databricks."
            )

    df = load_data_from_table(
        spark=spark,
        table_name=table_name,
        feature_cols=DEFAULT_FEATURE_COLS,  
        id_col="Num_Acc",
        target_col="grav",
    )

    fo = make_features(
        df,
        id_col="num_acc",
        target_col="grav",
        positive_threshold=3,
    )

    model = get_model(n_estimators=200)

    with mlflow.start_run(run_name=run_name):
        model.fit(fo.X_train, fo.y_train)
        preds = model.predict(fo.X_test)

        acc = accuracy_score(fo.y_test, preds)
        f1 = f1_score(fo.y_test, preds)

        mlflow.log_param("model", "RandomForestClassifier")
        mlflow.log_param("n_estimators", 200)
        mlflow.log_param("features", ",".join(fo.feature_cols))

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)

        # log model + scaler
        mlflow.sklearn.log_model(model, "model")
        mlflow.sklearn.log_model(fo.scaler, "scaler")

        print(f"Accuracy: {acc:.3f}")
        print(f"F1-score: {f1:.3f}")


if __name__ == "__main__":
    main()
