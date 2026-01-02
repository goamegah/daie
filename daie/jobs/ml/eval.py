# daie/jobs/ml/eval.py
import mlflow
import mlflow.spark

from pyspark.sql import functions as F
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

DEST_DB = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TEST_TABLE = f"{DEST_DB}.accident_test_v1"
PRED_TABLE = f"{DEST_DB}.accident_test_pred_v1"

# MODEL_URI_DEFAULT = "models:/accident_severity_rf/Production"
MODEL_URI_DEFAULT = None  

def main():
    df_test = spark.table(TEST_TABLE)

    # récupérer le model_uri depuis le task précédent (train)
    model_uri = MODEL_URI_DEFAULT
    try:
        model_uri = dbutils.jobs.taskValues.get(taskKey="train", key="model_uri")
    except Exception:
        pass

    if not model_uri:
        raise ValueError("No model_uri found. Provide MODEL_URI_DEFAULT or pass via workflow taskValues.")

    model = mlflow.spark.load_model(model_uri)

    pred = model.transform(df_test)

    pred_out = pred.select(
        "num_acc", "grav", "target",
        *[c for c in df_test.columns if c not in ["num_acc", "grav", "target"]],
        F.col("prediction").cast("int").alias("prediction")
    )

    # métriques
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="target", predictionCol="prediction", metricName="accuracy")
    evaluator_f1  = MulticlassClassificationEvaluator(labelCol="target", predictionCol="prediction", metricName="f1")

    acc = evaluator_acc.evaluate(pred)
    f1  = evaluator_f1.evaluate(pred)

    mlflow.set_experiment("/Shared/accident_severity_prediction")

    with mlflow.start_run(run_name="eval_rf_spark"):
        mlflow.log_param("model_uri", model_uri)
        mlflow.log_metric("accuracy", float(acc))
        mlflow.log_metric("f1_score", float(f1))

    (pred_out.write.mode("overwrite").format("delta").saveAsTable(PRED_TABLE))

    print(" Metrics:")
    print(" - accuracy:", acc)
    print(" - f1_score:", f1)
    print(" Written:", PRED_TABLE)

if __name__ == "__main__":
    main()
