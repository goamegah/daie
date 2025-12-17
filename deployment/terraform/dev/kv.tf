# ==============================================================================
# Key Vault
# ==============================================================================
resource "azurerm_key_vault" "this" {
  name                       = local.key_vault_name
  location                   = azurerm_resource_group.this.location
  resource_group_name        = azurerm_resource_group.this.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  tags = var.tags
}

# Access policy pour l'utilisateur/SP qui exécute Terraform (Azure CLI)
resource "azurerm_key_vault_access_policy" "terraform" {
  key_vault_id = azurerm_key_vault.this.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = [
    "Get", "List", "Set", "Delete", "Purge", "Recover"
  ]
}

# ==============================================================================
# NOTE : Access policy pour GitHub Actions SP
# ==============================================================================
# Cette policy sera créée MANUELLEMENT via Azure Portal ou CLI
# car elle nécessite des permissions User Access Administrator
# que votre SP n'a pas actuellement.
#
# Pour la créer manuellement :
# az keyvault set-policy \
#   --name kv-daie-chn-dev-2025 \
#   --object-id 76be8de1-371e-4234-bc73-af19d46a0c44 \
#   --secret-permissions get list

# ==============================================================================
# Secrets de configuration
# ==============================================================================
resource "azurerm_key_vault_secret" "databricks_host" {
  name         = "databricks-host"
  value        = "https://${azurerm_databricks_workspace.this.workspace_url}"
  key_vault_id = azurerm_key_vault.this.id

  depends_on = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "storage_account_name" {
  name         = "storage-account-name"
  value        = azurerm_storage_account.this.name
  key_vault_id = azurerm_key_vault.this.id

  depends_on = [azurerm_key_vault_access_policy.terraform]
}

# NOTE: These secrets are commented out until Unity Catalog metastore is assigned
# to the workspace by an account admin.
#
# resource "azurerm_key_vault_secret" "bronze_catalog" {
#   name         = "bronze-catalog"
#   value        = databricks_catalog.bronze.name
#   key_vault_id = azurerm_key_vault.this.id
#
#   depends_on = [azurerm_key_vault_access_policy.terraform]
# }
#
# resource "azurerm_key_vault_secret" "silver_catalog" {
#   name         = "silver-catalog"
#   value        = databricks_catalog.silver.name
#   key_vault_id = azurerm_key_vault.this.id
#
#   depends_on = [azurerm_key_vault_access_policy.terraform]
# }
#
# resource "azurerm_key_vault_secret" "gold_catalog" {
#   name         = "gold-catalog"
#   value        = databricks_catalog.gold.name
#   key_vault_id = azurerm_key_vault.this.id
#
#   depends_on = [azurerm_key_vault_access_policy.terraform]
# }