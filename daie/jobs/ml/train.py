import mlflow
import mlflow.spark

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier

DEST_DB = "daie_chn_dev_gold.dev_ml_accident_severity_prediction"
TRAIN_TABLE = f"{DEST_DB}.accident_train_v1"

REGISTER_MODEL_NAME = "accident_severity_rf"

CATEGORICAL_COLS = ["lum", "atm", "col", "int"]
NUMERIC_COLS = ["hrmn"]
LABEL_COL = "target"

def main():
    df_train = spark.table(TRAIN_TABLE)

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]

    feature_inputs = [f"{c}_idx" for c in CATEGORICAL_COLS] + NUMERIC_COLS

    assembler = VectorAssembler(inputCols=feature_inputs, outputCol="features_raw", handleInvalid="keep")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

    rf = RandomForestClassifier(
        labelCol=LABEL_COL,
        featuresCol="features",
        numTrees=200,
        maxDepth=10,
        seed=42
    )

    pipeline = Pipeline(stages=indexers + [assembler, scaler, rf])

    mlflow.set_experiment("/Shared/accident_severity_prediction") 

    with mlflow.start_run(run_name="train_rf_spark"):
        model = pipeline.fit(df_train)

        # log modèle spark
        mlflow.spark.log_model(
            spark_model=model,
            artifact_path="model",
            registered_model_name=REGISTER_MODEL_NAME  
        )

        # log params
        mlflow.log_param("algo", "RandomForestClassifier (Spark)")
        mlflow.log_param("numTrees", 200)
        mlflow.log_param("maxDepth", 10)

        run_id = mlflow.active_run().info.run_id
        model_uri = f"runs:/{run_id}/model"
        print("model_uri =", model_uri)

        try:
            dbutils.jobs.taskValues.set(key="model_uri", value=model_uri)
        except Exception:
            pass

if __name__ == "__main__":
    main()
