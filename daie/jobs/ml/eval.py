# daie/jobs/ml/eval.py
import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.metrics import accuracy_score, f1_score

DEST_DB = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TEST_TABLE = f"{DEST_DB}.accident_test_v1"
PRED_TABLE = f"{DEST_DB}.accident_test_pred_v1"

def main():
    # récupérer model_uri depuis le task train dans le workflow
    model_uri = None
    try:
        model_uri = dbutils.jobs.taskValues.get(taskKey="train", key="model_uri")
    except Exception:
        pass

    if not model_uri:
        raise ValueError("model_uri introuvable. Assure-toi que le task train set taskValues model_uri et que son taskKey = 'train'.")

    bundle = mlflow.sklearn.load_model(model_uri)
    model = bundle["model"]
    scaler = bundle["scaler"]
    feature_cols = bundle["feature_cols"]

    sdf = spark.table(TEST_TABLE)
    pdf = sdf.toPandas()

    pdf.columns = [c.lower() for c in pdf.columns]
    pdf = pdf.dropna()

    # target
    pdf["target"] = pdf["grav"].apply(lambda x: 1 if x >= 3 else 0)

    X = pdf.drop(columns=["num_acc", "grav", "target"])
    y = pdf["target"]

    # conversion numeric coerce + dropna aligné
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")

    valid_idx = X.dropna().index
    X = X.loc[valid_idx]
    y = y.loc[valid_idx]
    pdf = pdf.loc[valid_idx]

    X = X[feature_cols]

    X_scaled = scaler.transform(X)
    preds = model.predict(X_scaled)

    acc = accuracy_score(y, preds)
    f1 = f1_score(y, preds)

    mlflow.set_experiment("/Shared/accident_severity_prediction")
    with mlflow.start_run(run_name="accident_gravity_eval_agg"):
        mlflow.log_param("model_uri", model_uri)
        mlflow.log_metric("accuracy", float(acc))
        mlflow.log_metric("f1_score", float(f1))

    pdf_out = pdf.copy()
    pdf_out["prediction"] = preds.astype(int)

    # écrire table delta
    pred_sdf = spark.createDataFrame(pdf_out)
    (pred_sdf.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(PRED_TABLE))

    print(" Written:", PRED_TABLE)
    print(f"Accuracy: {acc:.3f}")
    print(f"F1-score: {f1:.3f}")

if __name__ == "__main__":
    main()
