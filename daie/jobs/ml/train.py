from __future__ import annotations

import mlflow
import mlflow.sklearn

from sklearn.metrics import accuracy_score, f1_score
from sklearn.pipeline import Pipeline

from daie.jobs.ml.data import load_data_from_table
from daie.jobs.ml.feature import make_features
from daie.jobs.ml.model import get_model


TABLE_NAME = "daie_chn_dev_gold.dev_datamart_opendata.accident_v1"


def main():
    try:
        spark  
    except NameError:
        raise RuntimeError(
            "Spark session 'spark' not found.  "
        )

    df = load_data_from_table(spark, TABLE_NAME)

    X_train, X_test, y_train, y_test, preprocess = make_features(
        df,
        target_col="grav",
        id_col="num_acc",
    )

    model = get_model()
    pipeline = Pipeline(steps=[
        ("preprocess", preprocess),
        ("model", model),
    ])

    with mlflow.start_run(run_name="accident_v1_gravity_prediction"):
        pipeline.fit(X_train, y_train)

        preds = pipeline.predict(X_test)
        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)

        mlflow.log_param("model", model.__class__.__name__)
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)

        # Log the full pipeline (preprocess + model)
        mlflow.sklearn.log_model(pipeline, "model")

        print(f"Accuracy: {acc:.3f}")
        print(f"F1-score: {f1:.3f}")


if __name__ == "__main__":
    main()
