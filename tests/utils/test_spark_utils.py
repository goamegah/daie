# tests/utils/test_spark_utils.py

import pytest
from unittest.mock import MagicMock

import mds.utils.spark_utils as SU
from pyspark.sql import SparkSession

# Fixture pour réinitialiser le singleton avant chaque test
@pytest.fixture(autouse=True)
def reset_spark_manager():
    # Réinitialise la SparkSession et DBUtils internes
    SU._spark_manager._spark = None
    SU._spark_manager._dbutils = None
    yield
    # Détruit la session Spark si elle existe
    if SU._spark_manager._spark is not None:
        SU._spark_manager._spark.stop()
        SU._spark_manager._spark = None

def test_get_spark_session_returns_singleton():
    # 1ère récupération
    spark1: SparkSession = SU.get_spark_session()
    # 2ème récupération – même instance !
    spark2: SparkSession = SU.get_spark_session()
    assert spark1 is spark2
    # On peut créer un DataFrame local sans erreur
    df = spark1.createDataFrame([(1, "a")], ["id", "val"])
    assert df.count() == 1

def test_scope_exists_true(monkeypatch):
    """
    Teste scope_exists() → True si secrets.get() ne lève pas une exception.
    """
    fake_dbutils = MagicMock()
    fake_dbutils.secrets.get.return_value = "secret"
    monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

    assert SU.scope_exists("my_scope", "my_key") is True
    fake_dbutils.secrets.get.assert_called_once_with(scope="my_scope", key="my_key")

def test_scope_exists_false(monkeypatch):
    """
    Teste scope_exists() → False si secrets.get() lève une exception.
    """
    fake_dbutils = MagicMock()
    fake_dbutils.secrets.get.side_effect = Exception("not found")
    monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

    assert SU.scope_exists("my_scope", "my_key") is False


if __name__ == "__main__":
    # debug section
    print("Running tests...")
    test_get_spark_session_returns_singleton()

