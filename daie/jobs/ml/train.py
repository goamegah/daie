# daie/jobs/ml/train.py
import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, f1_score
from daie.jobs.ml.model import get_model



DEST_DB = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TRAIN_TABLE = f"{DEST_DB}.accident_train_v1"

FEATURE_COLS = ["lum", "atm", "col", "jour", "mois", "vma", "circ", "nbv", "surf", "infra", "situ", "hrmn_minutes"]

def main():
    sdf = spark.table(TRAIN_TABLE)
    pdf = sdf.toPandas()

    pdf.columns = [c.lower() for c in pdf.columns]
    pdf = pdf.dropna()

    pdf["target"] = pdf["grav"].apply(lambda x: 1 if x >= 3 else 0)

    X = pdf.drop(columns=["num_acc", "grav", "target"])
    y = pdf["target"]

    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    valid_idx = X.dropna().index
    X = X.loc[valid_idx]
    y = y.loc[valid_idx]

    X_train, X_val, y_train, y_val = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    model = get_model(n_estimators=200)
    mlflow.set_experiment("/Shared/accident_severity_prediction")

    with mlflow.start_run(run_name="accident_gravity_prediction_agg"):
        model.fit(X_train_scaled, y_train)
        preds = model.predict(X_val_scaled)

        acc = accuracy_score(y_val, preds)
        f1 = f1_score(y_val, preds)

        mlflow.log_param("model", "RandomForestClassifier")
        mlflow.log_param("n_estimators", 200)
        mlflow.log_param("class_weight", "balanced")
        mlflow.log_param("features", ",".join(list(X.columns)))

        mlflow.log_metric("accuracy", float(acc))
        mlflow.log_metric("f1_score", float(f1))

        # IMPORTANT: log aussi le scaler + model ensemble
        # => on crée un petit dict packagé
        artifact = {"model": model, "scaler": scaler, "feature_cols": list(X.columns)}
        mlflow.sklearn.log_model(artifact, "model_bundle")

        run_id = mlflow.active_run().info.run_id
        model_uri = f"runs:/{run_id}/model_bundle"
        print(" model_uri:", model_uri)

        try:
            dbutils.jobs.taskValues.set(key="model_uri", value=model_uri)
        except Exception:
            pass

        print(f"Accuracy: {acc:.3f}")
        print(f"F1-score: {f1:.3f}")

if __name__ == "__main__":
    main()
