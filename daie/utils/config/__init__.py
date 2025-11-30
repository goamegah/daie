"""Environment configuration module."""

from typing import Dict, Any

# Environment configuration - update with your actual values
ENV_CONFIG: Dict[str, Dict[str, Any]] = {
    "dev": {
        "tenant_id": "your-dev-tenant-id",
        "application_id": "your-dev-app-id",
        "secret_key": "dev-secret-key",
        "storage_account_name": "devstorageaccount",
    },
    "test": {
        "tenant_id": "your-test-tenant-id",
        "application_id": "your-test-app-id",
        "secret_key": "test-secret-key",
        "storage_account_name": "teststorageaccount",
    },
    "prod": {
        "tenant_id": "your-prod-tenant-id",
        "application_id": "your-prod-app-id",
        "secret_key": "prod-secret-key",
        "storage_account_name": "prodstorageaccount",
    },
}


def get_env_config() -> Dict[str, Dict[str, Any]]:
    """Return the environment configuration."""
    return ENV_CONFIG
