# ==============================================================================
# Backend Configuration - Stockage de l'état Terraform
# ==============================================================================
# Pour le développement local, on peut utiliser le backend local
# Pour la production, utilisez Azure Storage Backend

terraform {
  # Backend local (développement)
  backend "local" {
    path = "terraform.tfstate"
  }
  
  # Backend Azure (à activer pour la production)
  # backend "azurerm" {
  #   resource_group_name  = "rg-terraform-state"
  #   storage_account_name = "statfstate"
  #   container_name       = "tfstate"
  #   key                  = "dev/databricks.tfstate"
  # }
}

# ==============================================================================
# NOTE : Pour créer le backend Azure Storage (une seule fois)
# ==============================================================================
# az group create --name rg-terraform-state --location westeurope
# 
# az storage account create \
#   --name statfstate \
#   --resource-group rg-terraform-state \
#   --location westeurope \
#   --sku Standard_LRS \
#   --encryption-services blob
# 
# az storage container create \
#   --name tfstate \
#   --account-name statfstate