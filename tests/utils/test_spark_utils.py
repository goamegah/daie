# tests/utils/test_spark_utils.py
from daie.utils.spark_utils import spark
from daie.utils import spark_utils
from daie.utils.tests import common as tst
from unittest.mock import patch, MagicMock
from pyspark.sql import Row
from pyspark.sql.functions import col

def test_validate_table_identifier():
    # Mock arguments
    table_identifier_unity = "daie_chn_dev_bronze.godwin_raw_opendata.accident_v1"
    result = spark_utils.validate_table_identifier(table_identifier_unity)
    assert result == table_identifier_unity

def test_write_delta_table_with_path():
    # Given: a mock dataframe and a Delta table path identifier
    dataframe_mock = MagicMock()
    path = "abfss://daie-platform@daiechndev.dfs.core.windows.net/raw/opendata/accident_v1"
    table_identifier_path = f"{spark_utils.DELTA}{path}`"
    partitions = ["_year"]
    options = {"mergeSchema": "true"}

    # When: patching the dependency and calling the function under test
    with patch('daie.utils.spark_utils.write_delta_table_by_path') as mock_write_delta_table_by_path:
        # Then: the correct dependency should be called with expected arguments
        spark_utils.write_delta_table(dataframe_mock, table_identifier_path, partitions, options)
        mock_write_delta_table_by_path.assert_called_once_with(dataframe_mock, path, partitions, options)

def test_write_delta_table_with_unity_table_name():
    # Given: a mock dataframe and a Unity Catalog table identifier
    dataframe_mock = MagicMock()
    table_identifier_unity = "daie_chn_dev_silver.godwin_curated_opendata.accident_v1"
    partitions = ["_year"]
    options = {"mergeSchema": "true"}

    # When: patching the dependency and calling the function under test
    with patch('daie.utils.spark_utils.write_delta_table_by_name') as mock_write_delta_table_by_name:
        spark_utils.write_delta_table(dataframe_mock, table_identifier_unity, partitions, options)

        # Then: the correct dependency should be called with expected arguments
        mock_write_delta_table_by_name.assert_called_once_with(dataframe_mock, table_identifier_unity, partitions, options, None, None)

def test_write_delta_stream_with_path():
    # Given: a mock dataframe and a Delta table path identifier
    new_df_mock = MagicMock()
    path = "abfss://daie-platform@daiechndev.dfs.core.windows.net/raw/opendata/accident_v1"
    table_identifier_path = f"{spark_utils.DELTA}{path}`"
    partitions = ["_year"]

    # When: patching the dependency and calling the function under test
    with patch('daie.utils.spark_utils.write_delta_stream_by_path') as mock_write_delta_stream_by_path:
        spark_utils.write_delta_stream(new_df_mock, table_identifier_path, partitions)

        # Then: the correct dependency should be called with expected arguments
        mock_write_delta_stream_by_path.assert_called_once_with(new_df_mock, path, partitions)


def test_write_delta_stream_with_unity_table_name():
    # Given: a mock dataframe and a Unity Catalog table identifier
    new_df_mock = MagicMock()
    table_identifier_unity = "daie_chn_dev_bronze.godwin_raw_opendata.accident_v1"
    partitions = ["partition_column"]
    mock_checkpoint_path = "/Volumes/daie_chn_dev_bronze/godwin_raw_opendata/accident_v1"

    # When: patching the dependencies and calling the function under test
    with patch('daie.utils.spark_utils.write_delta_stream_by_name') as mock_write_delta_stream_by_name, \
         patch('daie.utils.spark_utils.get_volume_location', return_value=mock_checkpoint_path):
        spark_utils.write_delta_stream(new_df_mock, table_identifier_unity, partitions)

        # Then: the correct dependency should be called with expected arguments
        mock_write_delta_stream_by_name.assert_called_once_with(
            new_df_mock, table_identifier_unity, mock_checkpoint_path, partitions
        )

def test_read_delta_table_with_condition_table_exists():
    # Given: a table identifier, a filter condition, and a DataFrame with matching and non-matching rows
    table_identifier = "daie_chn_dev_bronze.godwin_raw_opendata.accident_v1"
    condition = (col("Principal") == "dbw-G-daie-chn-dev-DS")
    data = [
        Row(Principal="dbw-G-daie-chn-dev-DE", ActionType="SELECT", ObjectType="SCHEMA"),
        Row(Principal="dbw-G-daie-chn-dev-DE", ActionType="MANAGE", ObjectType="SCHEMA"),
        Row(Principal="dbw-G-daie-chn-dev-DS", ActionType="SELECT", ObjectType="SCHEMA")
    ]
    dataframe = spark.createDataFrame(data)
    expected_data = [
        Row(Principal="dbw-G-daie-chn-dev-DS", ActionType="SELECT", ObjectType="SCHEMA")
    ]
    expected_dataframe = spark.createDataFrame(expected_data)

    # When: patching dependencies to simulate table exists and calling the function under test
    with patch('daie.utils.spark_utils.read_delta_table', return_value=dataframe) as mock_read_delta_table, \
         patch('daie.utils.spark_utils.check_if_table_exists', return_value=True) as mock_check_if_table_exists:
        result_df = spark_utils.read_delta_table_with_condition(table_identifier, condition)

        # Then: the correct dependencies should be called and the filtered DataFrame should match expectation
        mock_check_if_table_exists.assert_called_once_with(table_identifier)
        mock_read_delta_table.assert_called_once_with(table_identifier)
        tst.assert_dataframe_equals(result_df, expected_dataframe)


def test_read_delta_table_with_condition_table_not_exists():
    # Given: a table identifier and a filter condition, but the table does not exist
    table_identifier = "daie_chn_dev_bronze.godwin_raw_opendata.accident_v1"
    condition = (col("Principal") == "db-G-dspBFI-chn-dev-DS")

    # When: patching dependencies to simulate table does not exist and calling the function under test
    with patch('daie.utils.spark_utils.read_delta_table') as mock_read_delta_table, \
         patch('daie.utils.spark_utils.check_if_table_exists', return_value=False) as mock_check_if_table_exists:
        result_df = spark_utils.read_delta_table_with_condition(table_identifier, condition)

        # Then: check_if_table_exists should be called, read_delta_table should not be called, and result should be None
        mock_check_if_table_exists.assert_called_once_with(table_identifier)
        mock_read_delta_table.assert_not_called()
        assert result_df is None

if __name__ == "__main__":
    #debug section
    test_read_delta_table_with_condition_table_exists()
    test_read_delta_table_with_condition_table_not_exists()
    test_write_delta_stream_with_path()
    test_write_delta_stream_with_unity_table_name()
    test_write_delta_table_with_unity_table_name()
    test_write_delta_table_with_path()
    test_validate_table_identifier()
