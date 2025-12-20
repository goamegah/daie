# ==============================================================================
# Key Vault
# ==============================================================================
resource "azurerm_key_vault" "this" {
  name                       = local.key_vault_name
  location                   = azurerm_resource_group.this.location
  resource_group_name        = azurerm_resource_group.this.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  # soft_delete_retention_days = 1
  # purge_protection_enabled   = false

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
# Secrets - Configuration Databricks
# ==============================================================================
resource "azurerm_key_vault_secret" "databricks_host" {
  name         = "dbh-daie-chn-${var.environment}"
  value        = "https://${azurerm_databricks_workspace.this.workspace_url}"
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_key_vault_access_policy.terraform]
}

# ==============================================================================
# Secrets - Azure Infrastructure
# ==============================================================================
resource "azurerm_key_vault_secret" "subscription_id" {
  name         = "sid-daie-chn-${var.environment}"
  value        = data.azurerm_client_config.current.subscription_id
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "tenant_id" {
  name         = "tid-daie-chn-${var.environment}"
  value        = data.azurerm_client_config.current.tenant_id
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "resource_group_name" {
  name         = "rg-daie-chn-${var.environment}"
  value        = azurerm_resource_group.this.name
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "storage_account_name" {
  name         = "sta-daie-chn-${var.environment}"
  value        = azurerm_storage_account.this.name
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_key_vault_access_policy.terraform]
}

# ==============================================================================
# Secrets - Service Principal
# ==============================================================================
resource "azurerm_key_vault_secret" "sp_client_id" {
  name         = "cid-daie-chn-${var.environment}"
  value        = var.sp_client_id
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_key_vault_access_policy.terraform]
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

# az keyvault secret set --vault-name "kv-daie-chn-dev-2025" \
#   --name "cst-daie-chn-dev" \
#   --value "<votre_secret>"



