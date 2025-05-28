# tests/utils/test_spark_utils.py
from unittest.mock import MagicMock
import pytest
from pyspark.sql import SparkSession
import ade.utils.spark_utils as SU
from ade.utils.spark_utils import reset_spark_manager

# Fixture to reset the singleton before each test
@pytest.fixture(autouse=True)
def reset_spark():
    reset_spark_manager()
    yield
    reset_spark_manager()

def test_get_spark_session_returns_singleton():
    # 1st retrieval
    spark1: SparkSession = SU.get_spark_session()
    # 2nd retrieval – same instance!
    spark2: SparkSession = SU.get_spark_session()
    assert spark1 is spark2
    # Can create a local DataFrame without error
    df = spark1.createDataFrame([(1, "a")], ["id", "val"])
    assert df.count() == 1

def test_scope_exists_true(monkeypatch):
    """
    Tests scope_exists() → True if secrets.get() does not raise an exception.
    """
    fake_dbutils = MagicMock()
    fake_dbutils.secrets.get.return_value = "secret"
    monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

    assert SU.scope_exists("my_scope", "my_key") is True
    fake_dbutils.secrets.get.assert_called_once_with(scope="my_scope", key="my_key")

def test_scope_exists_false(monkeypatch):
    """
    Tests scope_exists() → False if secrets.get() raises an exception.
    """
    fake_dbutils = MagicMock()
    fake_dbutils.secrets.get.side_effect = Exception("not found")
    monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

    assert SU.scope_exists("my_scope", "my_key") is False


if __name__ == "__main__":
    # debug section
    print("Running tests...")
    test_get_spark_session_returns_singleton()
