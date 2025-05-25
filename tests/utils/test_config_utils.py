# tests/utils/test_config_utils.py

import pytest
import json
from pathlib import Path
from mds.utils import config_utils as cu


@pytest.fixture
def databricks_config(tmp_path: Path):
    # Simule un fichier de conf .databricks_instances.json
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
def azure_config(tmp_path: Path):
    # Simule un fichier de conf Azure
    data = {
        "tenant_id": "my-tenant-id",
        "storage_account": "my-storage-account"
    }
    path = tmp_path / "azure_storage.json"
    path.write_text(json.dumps(data))
    return path


def test_load_json(databricks_config):
    data = cu.load_json(databricks_config)
    assert data["dev"]["secret_key"] == "dev-key"


def test_validate_env_valid():
    cu.validate_env("dev")  # ne doit pas lever d'erreur


def test_validate_env_invalid():
    with pytest.raises(ValueError):
        cu.validate_env("staging")


def test_get_databricks_secret_key(monkeypatch, databricks_config):
    monkeypatch.setattr(cu, "DATABRICKS_INSTANCES_FILE", databricks_config)
    result = cu.get_databricks_secret_key_from_config_file("dev")
    assert result == "dev-key"


def test_get_databricks_scope_name(monkeypatch, databricks_config):
    monkeypatch.setattr(cu, "DATABRICKS_INSTANCES_FILE", databricks_config)
    result = cu.get_databricks_scope_name_from_config_file("dev")
    assert result == "dev-scope"


def test_get_databricks_instance_id(monkeypatch, databricks_config):
    monkeypatch.setattr(cu, "DATABRICKS_INSTANCES_FILE", databricks_config)
    result = cu.get_databricks_instance_id_from_config_file("dev")
    assert result == "dev-id"


def test_get_azure_tenant_id(monkeypatch, azure_config):
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", azure_config)
    assert cu.get_azure_tenant_id_from_config_file() == "my-tenant-id"


def test_get_azure_tenant_id_missing_field(monkeypatch, tmp_path):
    config_path = tmp_path / "azure.json"
    config_path.write_text(json.dumps({}))
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", config_path)
    assert cu.get_azure_tenant_id_from_config_file() == ""


def test_get_azure_tenant_id_missing_file(monkeypatch):
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", Path("nonexistent.json"))
    assert cu.get_azure_tenant_id_from_config_file() == ""


def test_get_azure_storage_account(monkeypatch, azure_config):
    monkeypatch.setattr(cu, "AZURE_STORAGE_FILE", azure_config)
    assert cu.get_azure_storage_account_from_config_file() == "my-storage-account"
