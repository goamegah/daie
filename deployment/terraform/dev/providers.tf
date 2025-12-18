# ==============================================================================
# Providers Configuration
# ==============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.45"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

# Provider Azure - pas besoin de spécifier subscription_id si vous utilisez Azure CLI
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

provider "azuread" {}

# Provider Databricks au niveau du compte (pour le metastore)
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
  
  # Authentification via Azure CLI
  azure_use_msi = false
}

# Provider Databricks au niveau du workspace
provider "databricks" {
  alias = "workspace"
  host  = azurerm_databricks_workspace.this.workspace_url
  
  # Authentification via Azure CLI
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
  azure_use_msi               = false
}