# ==============================================================================
# Outputs - Informations générales
# ==============================================================================
output "resource_group_name" {
  description = "Nom du Resource Group"
  value       = azurerm_resource_group.this.name
}

output "location" {
  description = "Région Azure"
  value       = azurerm_resource_group.this.location
}

output "key_vault_name" {
  description = "Nom du Key Vault"
  value       = azurerm_key_vault.this.name
}

output "key_vault_uri" {
  description = "URI du Key Vault"
  value       = azurerm_key_vault.this.vault_uri
}

output "storage_account_name" {
  description = "Nom du Storage Account"
  value       = azurerm_storage_account.this.name
}

output "storage_account_id" {
  description = "ID du Storage Account"
  value       = azurerm_storage_account.this.id
}

# ==============================================================================
# Outputs Databricks
# ==============================================================================
output "databricks_workspace_name" {
  description = "Nom du workspace Databricks"
  value       = azurerm_databricks_workspace.this.name
}

output "databricks_workspace_url" {
  description = "URL du workspace Databricks"
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "databricks_workspace_id" {
  description = "ID du workspace Databricks"
  value       = azurerm_databricks_workspace.this.workspace_id
}

# ==============================================================================
# Outputs Unity Catalog
# ==============================================================================
# NOTE: These outputs are commented out until Unity Catalog metastore is assigned
# to the workspace by an account admin.
#
# output "unity_catalog_metastore" {
#   description = "Informations Metastore"
#   value = {
#     id           = databricks_metastore.this.id
#     name         = databricks_metastore.this.name
#     storage_root = databricks_metastore.this.storage_root
#   }
# }
#
# output "unity_catalog_catalogs" {
#   description = "Liste des Catalogs Unity Catalog"
#   value = {
#     bronze = {
#       id   = databricks_catalog.bronze.id
#       name = databricks_catalog.bronze.name
#     }
#     silver = {
#       id   = databricks_catalog.silver.id
#       name = databricks_catalog.silver.name
#     }
#     gold = {
#       id   = databricks_catalog.gold.id
#       name = databricks_catalog.gold.name
#     }
#   }
# }
#
# output "unity_catalog_external_locations" {
#   description = "Liste des External Locations Unity Catalog"
#   value = {
#     bronze = {
#       id   = databricks_external_location.bronze.id
#       name = databricks_external_location.bronze.name
#       url  = databricks_external_location.bronze.url
#     }
#     silver = {
#       id   = databricks_external_location.silver.id
#       name = databricks_external_location.silver.name
#       url  = databricks_external_location.silver.url
#     }
#     gold = {
#       id   = databricks_external_location.gold.id
#       name = databricks_external_location.gold.name
#       url  = databricks_external_location.gold.url
#     }
#   }
# }
#
# output "unity_catalog_storage_credential" {
#   description = "Storage Credential Unity Catalog"
#   value = {
#     id   = databricks_storage_credential.this.id
#     name = databricks_storage_credential.this.name
#   }
# }
