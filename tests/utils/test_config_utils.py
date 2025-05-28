# tests/utils/test_config_utils.py

from pathlib import Path
import json
import pytest
from ade.utils import config_utils as cu

@pytest.fixture
def databricks_config_path(tmp_path: Path):
    """
    Simulates a .databricks_instances.json config file for testing.
    """
    data = {
        "dev": {
            "secret_key": "dev-key",
            "scope_name": "dev-scope",
            "databricks_instance_id": "dev-id"
        }
    }
    path = tmp_path / ".databricks_instances.json"
    path.write_text(json.dumps(data))
    return path

@pytest.fixture
def azure_config_path(tmp_path: Path):
    """
    Simulates an Azure configuration file for testing.
    """
    data = {
        "tenant_id": "my-tenant-id",
        "storage_account": "my-storage-account"
    }
    path = tmp_path / "azure_storage.json"
    path.write_text(json.dumps(data))
    return path

def test_load_json(databricks_config_path): # pylint: disable=redefined-outer-name
    """
    Test that load_json correctly loads the JSON config file.
    """
    data = cu.load_json(databricks_config_path)
    assert data["dev"]["secret_key"] == "dev-key"

def test_validate_env_valid():
    """
    Test that validate_env does not raise an error for a valid environment.
    """
    cu.validate_env("dev")  # should not raise an error

def test_validate_env_invalid():
    """
    Test that validate_env raises a ValueError for an invalid environment.
    """
    with pytest.raises(ValueError):
        cu.validate_env("staging")

def test_get_databricks_secret_key_with_mock(monkeypatch, databricks_config_path): # pylint: disable=redefined-outer-name
    """
    Test retrieval of the Databricks secret key from the config file.
    """
    monkeypatch.setattr(cu, "DATABRICKS_INSTANCES_FILE", databricks_config_path)
    result = cu.get_databricks_secret_key_from_config_file("dev")
    assert result == "dev-key"

def test_get_databricks_scope_name_with_mock(monkeypatch, databricks_config_path): # pylint: disable=redefined-outer-name
    """
    Test retrieval of the Databricks scope name from the config file.
    """
    monkeypatch.setattr(cu, "DATABRICKS_INSTANCES_FILE", databricks_config_path)
    result = cu.get_databricks_scope_name_from_config_file("dev")
    assert result == "dev-scope"

def test_get_databricks_instance_id_with_mock(monkeypatch, databricks_config_path): # pylint: disable=redefined-outer-name
    """
    Test retrieval of the Databricks instance ID from the config file.
    """
    monkeypatch.setattr(cu, "DATABRICKS_INSTANCES_FILE", databricks_config_path)
    result = cu.get_databricks_instance_id_from_config_file("dev")
    assert result == "dev-id"

def test_get_azure_tenant_id_with_mock(monkeypatch, azure_config_path): # pylint: disable=redefined-outer-name
    """
    Test retrieval of the Azure tenant ID from the config file.
    """
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", azure_config_path)
    assert cu.get_azure_tenant_id_from_config_file() == "my-tenant-id"

def test_get_azure_tenant_id_missing_field_with_mock(monkeypatch, tmp_path):
    """
    Test that an empty tenant_id field returns an empty string.
    """
    config_path = tmp_path / "azure.json"
    config_path.write_text(json.dumps({}))
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", config_path)
    assert cu.get_azure_tenant_id_from_config_file() == ""

def test_get_azure_tenant_id_missing_file_with_mock(monkeypatch):
    """
    Test that a missing Azure config file returns an empty string for tenant_id.
    """
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", Path("nonexistent.json"))
    assert cu.get_azure_tenant_id_from_config_file() == ""

def test_get_azure_storage_account_with_mock(monkeypatch, azure_config_path): # pylint: disable=redefined-outer-name
    """
    Test retrieval of the Azure storage account from the config file.
    """
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", azure_config_path)
    assert cu.get_azure_storage_account_from_config_file() == "my-storage-account"
