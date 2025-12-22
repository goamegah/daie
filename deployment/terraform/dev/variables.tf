# ==============================================================================
# Variables Générales
# ==============================================================================
variable "environment" {
  description = "Nom de l'environnement (dev, test, prod)"
  type        = string
  
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "L'environnement doit être : dev, test ou prod"
  }
}

variable "project" {
  description = "Nom du projet"
  type        = string
  default     = "daie"
  
  validation {
    condition     = length(var.project) >= 2 && length(var.project) <= 8
    error_message = "Le nom du projet doit faire entre 2 et 8 caractères"
  }
}

variable "location" {
  description = "Région Azure"
  type        = string
  default     = "westeurope"
}

variable "tags" {
  description = "Tags communs à toutes les ressources"
  type        = map(string)
  default     = {}
}

# ==============================================================================
# Variables Service Principal (pour les permissions futures - pas pour Terraform)
# ==============================================================================
variable "sp_client_id" {
  description = "Client ID du Service Principal (pour Key Vault et permissions Databricks)"
  type        = string
  sensitive   = true
}

variable "sp_object_id" {
  description = "Object ID du Service Principal (pour Key Vault et permissions Databricks)"
  type        = string
  sensitive   = true
}

# ==============================================================================
# Variables Databricks
# ==============================================================================
variable "databricks_sku" {
  description = "SKU du workspace Databricks"
  type        = string
  default     = "premium"
  
  validation {
    condition     = contains(["standard", "premium"], var.databricks_sku)
    error_message = "Le SKU doit être : standard ou premium (premium requis pour Unity Catalog)"
  }
}

variable "databricks_account_id" {
  description = "ID du compte Databricks (requis pour Unity Catalog)"
  type        = string
  sensitive   = true
}

# ==============================================================================
# Variables Storage
# ==============================================================================
variable "storage_account_tier" {
  description = "Tier du Storage Account"
  type        = string
  default     = "Standard"
  
  validation {
    condition     = contains(["Standard", "Premium"], var.storage_account_tier)
    error_message = "Le tier doit être : Standard ou Premium"
  }
}

variable "storage_replication_type" {
  description = "Type de réplication du Storage Account"
  type        = string
  default     = "LRS"
  
  validation {
    condition     = contains(["LRS", "GRS", "RAGRS", "ZRS"], var.storage_replication_type)
    error_message = "Le type de réplication doit être : LRS, GRS, RAGRS ou ZRS"
  }
}

# ==============================================================================
# Variables Clusters
# ==============================================================================
variable "databricks_runtime_version" {
  description = "Version du runtime Databricks"
  type        = string
  default     = "15.4.x-scala2.12"
}

variable "node_type_id" {
  description = "Type de nœud pour les clusters"
  type        = string
  default     = "Standard_D4ds_v5"
}

variable "auto_install_daie_package" {
  description = "Installer automatiquement le package daie sur les clusters"
  type        = bool
  default     = true
}

variable "daie_package_version" {
  description = "Version du package daie à installer"
  type        = string
  default     = "0.0.1"
}

variable "developer_name" {
  description = "Nom du développeur pour le tag et le chemin du package"
  type        = string
  default     = "dev"
}