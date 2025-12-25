# ==============================================================================
# Locals - Conventions de nommage
# ==============================================================================
locals {
  # PrÃ©fixe : <project>-chn-<env>
  name_prefix = "${var.project}-chn-${var.environment}"
  
  # Noms des ressources
  resource_group_name   = "rg-${local.name_prefix}"
  key_vault_name        = "kv-${local.name_prefix}-2025"
  storage_account_name  = "sta${var.project}chn${var.environment}"
  databricks_workspace  = "dbw-${local.name_prefix}"
  access_connector_name = "ac-${local.name_prefix}"
  metastore_name        = "metastore-${var.project}-${var.location}"
  
  # Unity Catalog : <project>_chn_<env>_<layer>
  catalog_bronze = "${var.project}_chn_${var.environment}_bronze"
  catalog_silver = "${var.project}_chn_${var.environment}_silver"
  catalog_gold   = "${var.project}_chn_${var.environment}_gold"
}

# ==============================================================================
# Data Sources
# ==============================================================================
data "azurerm_client_config" "current" {}
data "azuread_client_config" "current" {}

# ==============================================================================
# Resource Group
# ==============================================================================
resource "azurerm_resource_group" "this" {
  name     = local.resource_group_name
  location = var.location
  tags     = var.tags
}

# ==============================================================================
# Storage Account (pour Unity Catalog)
# ==============================================================================
resource "azurerm_storage_account" "this" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  tags = var.tags
}

# Containers pour Unity Catalog
resource "azurerm_storage_container" "metastore" {
  name                  = "metastore"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

# ==============================================================================
# Databricks Access Connector
# ==============================================================================
resource "azurerm_databricks_access_connector" "this" {
  name                = local.access_connector_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ==============================================================================
# NOTE : Role Assignments pour Access Connector et SP
# ==============================================================================
# Ces role assignments nÃ©cessitent le rÃ´le "User Access Administrator"
# Ils seront crÃ©Ã©s MANUELLEMENT via Azure Portal ou CLI :
#
# 1. Storage Blob Data Contributor pour Access Connector :
# az role assignment create \
#   --assignee <ACCESS_CONNECTOR_PRINCIPAL_ID> \
#   --role "Storage Blob Data Contributor" \
#   --scope /subscriptions/455fde4d-a77d-4a42-bbff-d13341a34203/resourceGroups/rg-daie-chn-dev/providers/Microsoft.Storage/storageAccounts/stadaiechndev
#
# 2. Contributor sur Databricks pour le SP GitHub :
# az role assignment create \
#   --assignee 76be8de1-371e-4234-bc73-af19d46a0c44 \
#   --role "Contributor" \
#   --scope /subscriptions/455fde4d-a77d-4a42-bbff-d13341a34203/resourceGroups/rg-daie-chn-dev/providers/Microsoft.Databricks/workspaces/dbw-daie-chn-dev

# ==============================================================================
# Databricks Workspace
# ==============================================================================
resource "azurerm_databricks_workspace" "this" {
  name                = local.databricks_workspace
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = var.databricks_sku

  tags = var.tags
}