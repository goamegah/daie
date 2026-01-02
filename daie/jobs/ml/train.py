# daie/jobs/ml/train.py

import mlflow
import mlflow.sklearn
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier

from daie.jobs.ml.config import (
    TABLE_TRAIN, TABLE_META, FEATURE_COLS, TARGET_COL, RANDOM_STATE, MLFLOW_EXPERIMENT
)

def main(spark):
    sdf = spark.table(TABLE_TRAIN)

    # pandas pour sklearn (comme ton notebook)
    pdf = sdf.select(*(FEATURE_COLS + [TARGET_COL])).toPandas()

    # coerce numeric + drop NaN
    for c in FEATURE_COLS:
        pdf[c] = pd.to_numeric(pdf[c], errors="coerce")
    pdf = pdf.dropna(subset=FEATURE_COLS + [TARGET_COL])

    X_train = pdf[FEATURE_COLS]
    y_train = pdf[TARGET_COL]

    pipeline = Pipeline(steps=[
        ("scaler", StandardScaler()),
        ("model", RandomForestClassifier(
            n_estimators=200,
            random_state=RANDOM_STATE,
            n_jobs=-1,
            class_weight="balanced"
        ))
    ])

    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="train_accident_severity") as run:
        pipeline.fit(X_train, y_train)

        mlflow.log_param("model", "RandomForestClassifier")
        mlflow.log_param("n_estimators", 200)
        mlflow.log_param("features", ",".join(FEATURE_COLS))

        # log pipeline complet
        mlflow.sklearn.log_model(pipeline, "model")

        run_id = run.info.run_id
        print("Train done. run_id:", run_id)

    # écrit metadata pour que eval retrouve le dernier modèle
    meta_pdf = pd.DataFrame([{"model_run_id": run_id}])
    spark.createDataFrame(meta_pdf).write.mode("overwrite").format("delta").saveAsTable(TABLE_META)
    print("Saved metadata:", TABLE_META)

if __name__ == "__main__":
    main(spark)
