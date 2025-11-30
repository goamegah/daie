# tests/utils/test_spark_utils.py
"""
Unit tests for spark_utils module.
Uses given/when/then approach with mocked Databricks dependencies.
"""
from unittest.mock import MagicMock
import daie.utils.spark_utils as SU


class TestCheckIfScopeExists:
    """Tests for check_if_scope_exists function."""

    def test_scope_exists_returns_true_when_secret_found(self, monkeypatch):
        """
        GIVEN a valid scope and key
        WHEN check_if_scope_exists is called
        THEN it returns True
        """
        # GIVEN
        fake_dbutils = MagicMock()
        fake_dbutils.secrets.get.return_value = "secret_value"
        monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

        # WHEN
        result = SU.check_if_scope_exists("my_scope", "my_key")

        # THEN
        assert result is True
        fake_dbutils.secrets.get.assert_called_once_with(scope="my_scope", key="my_key")

    def test_scope_exists_returns_false_when_secret_not_found(self, monkeypatch):
        """
        GIVEN an invalid scope or key
        WHEN check_if_scope_exists is called
        THEN it returns False
        """
        # GIVEN
        fake_dbutils = MagicMock()
        fake_dbutils.secrets.get.side_effect = Exception("Secret not found")
        monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

        # WHEN
        result = SU.check_if_scope_exists("invalid_scope", "invalid_key")

        # THEN
        assert result is False


class TestDoesPathExistInDatabricks:
    """Tests for does_path_exist_in_databricks function."""

    def test_path_exists_returns_true(self, monkeypatch):
        """
        GIVEN a valid path
        WHEN does_path_exist_in_databricks is called
        THEN it returns True
        """
        # GIVEN
        fake_dbutils = MagicMock()
        fake_dbutils.fs.ls.return_value = ["file1", "file2"]
        monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

        # WHEN
        result = SU.does_path_exist_in_databricks("/valid/path")

        # THEN
        assert result is True

    def test_path_exists_returns_false(self, monkeypatch):
        """
        GIVEN an invalid path
        WHEN does_path_exist_in_databricks is called
        THEN it returns False
        """
        # GIVEN
        fake_dbutils = MagicMock()
        fake_dbutils.fs.ls.side_effect = Exception("Path not found")
        monkeypatch.setattr(SU, "get_dbutils", lambda: fake_dbutils)

        # WHEN
        result = SU.does_path_exist_in_databricks("/invalid/path")

        # THEN
        assert result is False


class TestGetApplicationId:
    """Tests for get_application_id function."""

    def test_get_application_id_returns_value(self):
        """
        GIVEN a valid environment
        WHEN get_application_id is called
        THEN it returns the application_id from config
        """
        # WHEN
        result = SU.get_application_id("dev")

        # THEN
        assert result is not None


class TestGetSecretKey:
    """Tests for get_secret_key function."""

    def test_get_secret_key_returns_value(self):
        """
        GIVEN a valid environment
        WHEN get_secret_key is called
        THEN it returns the secret_key from config
        """
        # WHEN
        result = SU.get_secret_key("dev")

        # THEN
        assert result is not None


class TestValidateTableIdentifier:
    """Tests for validate_table_identifier function."""

    def test_validate_path_adds_delta_prefix(self):
        """
        GIVEN a table identifier that is a path without delta prefix
        WHEN validate_table_identifier is called
        THEN it adds the delta prefix
        """
        # GIVEN
        path = "abfss://container@storage.dfs.core.windows.net/path/to/table"

        # WHEN
        result = SU.validate_table_identifier(path)

        # THEN
        assert result.startswith("`delta`.`")
        assert result.endswith("`")

    def test_validate_table_name_unchanged(self):
        """
        GIVEN a table identifier that is a catalog.schema.table name
        WHEN validate_table_identifier is called
        THEN it returns unchanged
        """
        # GIVEN
        table_name = "catalog.schema.table"

        # WHEN
        result = SU.validate_table_identifier(table_name)

        # THEN
        assert result == table_name


class TestCheckIfTableIdentifierIsPath:
    """Tests for check_if_table_identifier_is_path function."""

    def test_returns_true_for_abfss_path(self):
        """
        GIVEN a path starting with abfss://
        WHEN check_if_table_identifier_is_path is called
        THEN it returns True
        """
        assert SU.check_if_table_identifier_is_path("abfss://container@storage/path") is True

    def test_returns_false_for_table_name(self):
        """
        GIVEN a catalog.schema.table name
        WHEN check_if_table_identifier_is_path is called
        THEN it returns False
        """
        assert SU.check_if_table_identifier_is_path("catalog.schema.table") is False


class TestExtractStoragePath:
    """Tests for extract_storage_path function."""

    def test_extracts_path_from_delta_identifier(self):
        """
        GIVEN a delta table identifier with path
        WHEN extract_storage_path is called
        THEN it returns the storage path
        """
        # GIVEN
        delta_identifier = "`delta`.`abfss://container@storage.dfs.core.windows.net/path/to/table`"

        # WHEN
        result = SU.extract_storage_path(delta_identifier)

        # THEN
        assert result == "abfss://container@storage.dfs.core.windows.net/path/to/table"
