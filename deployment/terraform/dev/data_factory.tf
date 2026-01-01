# ==============================================================================
# Azure Data Factory
# ==============================================================================
resource "azurerm_data_factory" "this" {
  name                = "adf-${local.name_prefix}"
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ==============================================================================
# Role Assignments - Data Factory Permissions
# ==============================================================================

# Data Factory needs Storage Blob Data Contributor on storage account
resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.this.identity[0].principal_id

  depends_on = [
    azurerm_data_factory.this,
    azurerm_storage_account.this
  ]
}

# Data Factory needs Contributor on Databricks workspace for job orchestration
resource "azurerm_role_assignment" "adf_databricks" {
  scope                = azurerm_databricks_workspace.this.id
  role_definition_name = "Contributor"
  principal_id         = azurerm_data_factory.this.identity[0].principal_id

  depends_on = [
    azurerm_data_factory.this,
    azurerm_databricks_workspace.this
  ]
}

# ==============================================================================
# Linked Service - Azure Data Lake Storage Gen2 (Unity Catalog)
# ==============================================================================
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "unity_catalog" {
  name              = "ls_adls_unity_catalog"
  data_factory_id   = azurerm_data_factory.this.id
  
  # Utiliser Managed Identity pour l'authentification
  use_managed_identity = true
  url                  = "https://${azurerm_storage_account.this.name}.dfs.core.windows.net"

  depends_on = [azurerm_role_assignment.adf_storage]
}

# ==============================================================================
# Linked Service - Key Vault (pour les secrets)
# ==============================================================================
resource "azurerm_data_factory_linked_service_key_vault" "this" {
  name            = "ls_key_vault"
  data_factory_id = azurerm_data_factory.this.id
  key_vault_id    = azurerm_key_vault.this.id

  depends_on = [azurerm_data_factory.this]
}

# ==============================================================================
# Access Policy - Data Factory vers Key Vault
# ==============================================================================
resource "azurerm_key_vault_access_policy" "adf" {
  key_vault_id = azurerm_key_vault.this.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_data_factory.this.identity[0].principal_id

  secret_permissions = [
    "Get", "List"
  ]

  depends_on = [azurerm_data_factory.this]
}

# ==============================================================================
# NOTE : Linked Service Databricks
# ==============================================================================
# Le Linked Service Databricks sera créé manuellement dans l'interface Data Factory
# car il nécessite la configuration d'un cluster ou job cluster.
#
# Pour créer manuellement :
# 1. Aller dans Data Factory Studio > Manage > Linked Services
# 2. New > Azure Databricks
# 3. Configurer :
#    - Name: ls_databricks_workspace
#    - Workspace URL: https://<workspace>.azuredatabricks.net
#    - Authentication: Service Principal
#    - Client ID: Depuis Key Vault (cid-daie-chn-dev)
#    - Client Secret: Depuis Key Vault (cst-daie-chn-dev)
#    - Tenant ID: Depuis Key Vault (tid-daie-chn-dev)
#    - Cluster: Job cluster ou existing cluster

# ==============================================================================
# NOTE : Pipelines
# ==============================================================================
# Les Pipelines seront créés manuellement dans l'interface Data Factory
# car c'est plus simple et visuel pour :
# - Créer des activités
# - Configurer les paramètres
# - Gérer les dépendances
# - Débugger et tester
#
# Exemples de pipelines à créer :
# 1. pl_orchestrate_databricks_job : Exécute un job Databricks
# 2. pl_elt_full_pipeline : Pipeline complet (ingestion + transformation)
