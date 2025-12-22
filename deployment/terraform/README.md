# Infrastructure Terraform

## 📋 Responsabilités

### ✅ Géré par Terraform
- Resource Group
- Storage Account & Containers
- Databricks Workspace
- Databricks Access Connector
- Key Vault
- Unity Catalog :
  - Storage Credentials
  - External Locations
  - Catalogs (Bronze, Silver, Gold)
  - Schemas & Volumes
  - Service Principal & Grants

### ❌ NON géré par Terraform
- **Clusters Databricks** → Gérés par le pipeline CD
  - Création/suppression à la demande
  - Configuration dynamique par développeur
  - Voir `deployment/devops/manage_cluster.py`

## 🚀 Déploiement

### Prérequis
```bash
# Installer Terraform
# Configurer Azure CLI
az login

# Configurer les variables d'environnement
export ARM_CLIENT_ID="..."
export ARM_CLIENT_SECRET="..."
export ARM_TENANT_ID="..."
export ARM_SUBSCRIPTION_ID="..."
```

### Déployer l'infrastructure

```bash
cd deployment/terraform/dev

# Initialiser Terraform
terraform init

# Planifier les changements
terraform plan -var-file="vars/dev.tfvars"

# Appliquer les changements
terraform apply -var-file="vars/dev.tfvars"
```

## 📁 Structure

```
deployment/terraform/dev/
├── backend.tf           # Configuration du backend Terraform
├── main.tf             # Ressources Azure principales
├── providers.tf        # Configuration des providers
├── variables.tf        # Déclaration des variables
├── outputs.tf          # Outputs Terraform
├── kv.tf              # Key Vault
├── unity_catalog.tf   # Unity Catalog (Catalogs, Volumes, Grants)
├── null.tf            # Null resources (scripts manuels)
└── vars/
    └── dev.tfvars     # Valeurs des variables pour dev
```

## 🔧 Variables

### Obligatoires (dans dev.tfvars)
- `environment` - Environnement (dev/test/prod)
- `project` - Nom du projet
- `sp_client_id` - Client ID du Service Principal
- `sp_object_id` - Object ID du Service Principal
- `databricks_account_id` - ID du compte Databricks

### Optionnelles (avec valeurs par défaut)
- `location` - Région Azure (défaut: westeurope)
- `databricks_sku` - SKU Databricks (défaut: premium)
- `storage_account_tier` - Tier du Storage (défaut: Standard)
- `storage_replication_type` - Type de réplication (défaut: LRS)

## 🎯 Workflow

### 1. Infrastructure initiale (une fois)
```bash
cd deployment/terraform/dev
terraform apply -var-file="vars/dev.tfvars"
```

### 2. Déploiement du code (quotidien)
```bash
# Via GitHub Actions
# Actions > CD - Déploiement Databricks
# - Déployer le package
# - Créer/mettre à jour le cluster
# - Installer sur les clusters
```

### 3. Mise à jour de l'infrastructure (rare)
```bash
# Modifier les fichiers .tf ou dev.tfvars
terraform plan -var-file="vars/dev.tfvars"
terraform apply -var-file="vars/dev.tfvars"
```

## 📝 Notes importantes

### Unity Catalog
- Le metastore doit être créé et assigné manuellement par un admin compte Databricks
- Une fois assigné, décommenter les ressources dans `unity_catalog.tf`

### Service Principal
- Créé manuellement dans Entra ID
- Ajouté dans Databricks via Terraform
- Permissions configurées automatiquement via Grants

### Clusters
- **Ne PAS créer de clusters dans Terraform**
- Utiliser le pipeline CD : `python deployment/devops/manage_cluster.py`
- Avantages : flexibilité, isolation par développeur, économies

## 🔍 Vérification

```bash
# Lister les ressources
terraform state list

# Voir les outputs
terraform output

# Vérifier un volume
terraform state show databricks_volume.packages
```

## 🧹 Nettoyage

```bash
# Supprimer toute l'infrastructure (ATTENTION!)
terraform destroy -var-file="vars/dev.tfvars"
```

## 📚 Documentation

- [Terraform Databricks Provider](https://registry.terraform.io/providers/databricks/databricks/latest/docs)
- [Azure Provider](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
