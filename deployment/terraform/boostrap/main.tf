# ==============================================================================
# BOOTSTRAP - À exécuter UNE SEULE FOIS pour obtenir le Databricks Account ID
# ==============================================================================
# Ce fichier crée un workspace minimal temporaire juste pour initialiser
# votre compte Databricks et récupérer l'Account ID
# 
# Après avoir récupéré l'Account ID :
# 1. Notez-le dans vos variables
# 2. Détruisez ce bootstrap : terraform destroy
# 3. Utilisez les vrais fichiers Terraform pour le déploiement complet
# ==============================================================================

terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.80"
    }
  }
}

provider "azurerm" {
  features {}
}

# Data source pour la config actuelle
data "azurerm_client_config" "current" {}

# Resource Group temporaire
resource "azurerm_resource_group" "bootstrap" {
  name     = "rg-databricks-bootstrap"
  location = "westeurope"
  
  tags = {
    Purpose   = "Bootstrap Databricks Account ID"
    Temporary = "true"
  }
}

# Workspace Databricks minimal (Premium pour Unity Catalog)
resource "azurerm_databricks_workspace" "bootstrap" {
  name                = "dbw-bootstrap-temp"
  resource_group_name = azurerm_resource_group.bootstrap.name
  location            = azurerm_resource_group.bootstrap.location
  sku                 = "premium"
  
  tags = {
    Purpose   = "Bootstrap Databricks Account ID"
    Temporary = "true"
  }
}

# ==============================================================================
# OUTPUTS - Votre Account ID sera affiché ici
# ==============================================================================
output "databricks_account_id" {
  description = "IMPORTANT : Copiez cet Account ID dans votre terraform.tfvars principal"
  value       = azurerm_databricks_workspace.bootstrap.workspace_id
}

output "workspace_url" {
  description = "URL du workspace temporaire"
  value       = "https://${azurerm_databricks_workspace.bootstrap.workspace_url}"
}

output "instructions" {
  description = "Prochaines étapes"
  value = <<-EOT
  
  ========================================================================
  ✅ BOOTSTRAP TERMINÉ !
  ========================================================================
  
  Votre Databricks Account ID est affiché ci-dessus.
  
  PROCHAINES ÉTAPES :
  
  1. Copiez le 'databricks_account_id' affiché ci-dessus
  
  2. Allez dans le répertoire principal et éditez terraform.tfvars :
     databricks_account_id = "VOTRE_ACCOUNT_ID"
  
  3. Revenez ici et détruisez ce bootstrap :
     terraform destroy
  
  4. Retournez dans le répertoire principal et déployez :
     terraform init
     terraform apply
  
  ========================================================================
  
  EOT
}