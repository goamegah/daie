# ==============================================================================
# Unity Catalog Configuration
# ==============================================================================

# ==============================================================================
# Metastore - Using existing metastore (account-level access not available)
# ==============================================================================
# NOTE: Metastore creation and assignment requires account-level admin access.
# If you don't have account admin access, you have two options:
#
# Option A: Request your Databricks account admin to:
#   1. Create a metastore for your region (westeurope)
#   2. Assign it to your workspace
#   3. Grant you permissions on the metastore
#
# Option B: If a metastore is already assigned to your workspace, 
#   use the data source below to reference it.
#
# Once the metastore is assigned to your workspace, uncomment the data source:
#
# data "databricks_metastore" "this" {
#   provider = databricks.workspace
#   name     = local.metastore_name  # or use the actual metastore name
# }

# ==============================================================================
# COMMENTED OUT - Requires account-level admin access
# ==============================================================================
# resource "databricks_metastore" "this" {
#   provider      = databricks.account
#   name          = local.metastore_name
#   storage_root  = "abfss://metastore@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
#   force_destroy = true
#   region        = var.location
#
#   depends_on = [
#     azurerm_storage_container.metastore
#   ]
# }
#
# resource "databricks_metastore_assignment" "this" {
#   provider     = databricks.account
#   metastore_id = databricks_metastore.this.id
#   workspace_id = azurerm_databricks_workspace.this.workspace_id
#
#   depends_on = [azurerm_databricks_workspace.this]
# }
#
# resource "databricks_metastore_data_access" "this" {
#   provider     = databricks.account
#   metastore_id = databricks_metastore.this.id
#   name         = "metastore-access-${local.name_prefix}"
#   
#   azure_managed_identity {
#     access_connector_id = azurerm_databricks_access_connector.this.id
#   }
#
#   is_default = true
#
#   depends_on = [
#     azurerm_databricks_access_connector.this
#   ]
# }

# ==============================================================================
# Service Principal dans Databricks (optionnel)
# ==============================================================================
# Décommenter si vous utilisez un SP
# resource "databricks_service_principal" "sp" {
#   provider       = databricks.workspace
#   application_id = var.sp_client_id
#   display_name   = "SP GitHub Actions"
#   
#   depends_on = [databricks_metastore_assignment.this]
# }

# ==============================================================================
# Storage Credential
# ==============================================================================
# NOTE: These resources require Unity Catalog metastore to be assigned to workspace.
# Once your admin has assigned a metastore, uncomment these resources.
#
resource "databricks_storage_credential" "this" {
	provider = databricks.workspace
	name     = "sc-${local.name_prefix}"
  
	azure_managed_identity {
		access_connector_id = azurerm_databricks_access_connector.this.id
	}
  
	comment = "Storage credential pour ${var.environment}"
}

# ==============================================================================
# External Locations
# ==============================================================================
# NOTE: These resources require Unity Catalog metastore to be assigned to workspace.
# Once your admin has assigned a metastore, uncomment these resources.
#
resource "databricks_external_location" "bronze" {
	provider        = databricks.workspace
	name            = "ext-${local.name_prefix}-bronze"
	url             = "abfss://bronze@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
	credential_name = databricks_storage_credential.this.name
	comment         = "External location Bronze"
  force_destroy   = true # <-

	depends_on = [databricks_storage_credential.this]
}
#
resource "databricks_external_location" "silver" {
	provider        = databricks.workspace
	name            = "ext-${local.name_prefix}-silver"
	url             = "abfss://silver@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
	credential_name = databricks_storage_credential.this.name
	comment         = "External location Silver"
  force_destroy   = true # <-

	depends_on = [databricks_storage_credential.this]
}
#
resource "databricks_external_location" "gold" {
	provider        = databricks.workspace
	name            = "ext-${local.name_prefix}-gold"
	url             = "abfss://gold@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
	credential_name = databricks_storage_credential.this.name
	comment         = "External location Gold"
  force_destroy   = true # <-

	depends_on = [databricks_storage_credential.this]
}

# ==============================================================================
# Catalogs
# ==============================================================================
# NOTE: These resources require Unity Catalog metastore to be assigned to workspace.
# Once your admin has assigned a metastore, uncomment these resources.
#
resource "databricks_catalog" "bronze" {
	provider     = databricks.workspace
	name         = local.catalog_bronze
	comment      = "Catalog Bronze - Données brutes"
	storage_root = "abfss://bronze@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  force_destroy   = true # <-

	properties = {
		purpose = "bronze"
	}
	depends_on = [
		databricks_external_location.bronze
	]
}
#
resource "databricks_catalog" "silver" {
	provider     = databricks.workspace
	name         = local.catalog_silver
	comment      = "Catalog Silver - Données curées"
	storage_root = "abfss://silver@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  force_destroy   = true # <-
	properties = {
		purpose = "silver"
	}
	depends_on = [
		databricks_external_location.silver
	]
}
#
resource "databricks_catalog" "gold" {
	provider     = databricks.workspace
	name         = local.catalog_gold
	comment      = "Catalog Gold - Données agrégées"
	storage_root = "abfss://gold@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  force_destroy   = true # <-
	properties = {
		purpose = "gold"
	}
	depends_on = [
		databricks_external_location.gold
	]
}

# ==============================================================================
# Schema artifacts
# ==============================================================================
resource "databricks_schema" "artifacts" {
	provider     = databricks.workspace
	catalog_name = databricks_catalog.bronze.name
	name         = "artifacts"
	comment      = "Schema pour les artifacts"
    force_destroy   = true # <-
}

# ==============================================================================
# Volume packages
# ==============================================================================
resource "databricks_volume" "packages" {
	provider     = databricks.workspace
	name         = "packages"
	catalog_name = databricks_catalog.bronze.name
	schema_name  = databricks_schema.artifacts.name
	volume_type  = "MANAGED"
	comment      = "Volume pour les packages wheel"
}

# ==============================================================================
# Volumes supplémentaires pour artifacts
# ==============================================================================
resource "databricks_volume" "metadata" {
	provider     = databricks.workspace
	name         = "metadata"
	catalog_name = databricks_catalog.bronze.name
	schema_name  = databricks_schema.artifacts.name
	volume_type  = "MANAGED"
	comment      = "Volume pour les métadonnées des sources"
}

resource "databricks_volume" "config" {
	provider     = databricks.workspace
	name         = "config"
	catalog_name = databricks_catalog.bronze.name
	schema_name  = databricks_schema.artifacts.name
	volume_type  = "MANAGED"
	comment      = "Volume pour les fichiers de configuration"
}

resource "databricks_volume" "schema" {
	provider     = databricks.workspace
	name         = "schema"
	catalog_name = databricks_catalog.bronze.name
	schema_name  = databricks_schema.artifacts.name
	volume_type  = "MANAGED"
	comment      = "Volume pour les schémas de données"
}

# ==============================================================================
# Volume init_scripts
# ==============================================================================
resource "databricks_volume" "init_scripts" {
	provider     = databricks.workspace
	name         = "init_scripts"
	catalog_name = databricks_catalog.bronze.name
	schema_name  = databricks_schema.artifacts.name
	volume_type  = "MANAGED"
	comment      = "Volume pour les scripts d'initialisation des clusters"
}

# ==============================================================================
# Service Principal dans Databricks
# ==============================================================================
resource "databricks_service_principal" "github_actions" {
  provider                   = databricks.workspace
  application_id             = var.sp_client_id
  display_name               = "SP GitHub Actions - ${var.environment}"
  active                     = true
  allow_cluster_create       = true
  allow_instance_pool_create = false
}

# ==============================================================================
# NOTE: Service Principal Usage Permissions
# ==============================================================================
# Pour permettre aux utilisateurs d'utiliser le SP dans les jobs, exécutez:
# python deployment/devops/grant_sp_usage.py dev
#
# Ou manuellement dans Databricks UI:
# Settings > Identity and Access > Service Principals > SP GitHub Actions - dev
# > Permissions > Add > Group: users > Permission: CAN_USE

# ==============================================================================
# Grants - Permissions automatisées
# ==============================================================================

# Grant sur le catalog Bronze
resource "databricks_grants" "catalog_bronze" {
  provider = databricks.workspace
  catalog  = databricks_catalog.bronze.name
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_catalog.bronze
  ]
}

# Grant sur le catalog Silver
resource "databricks_grants" "catalog_silver" {
  provider = databricks.workspace
  catalog  = databricks_catalog.silver.name
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_catalog.silver
  ]
}

# Grant sur le catalog Gold
resource "databricks_grants" "catalog_gold" {
  provider = databricks.workspace
  catalog  = databricks_catalog.gold.name
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_catalog.gold
  ]
}

# Grant sur le schema artifacts (pour les packages)
resource "databricks_grants" "schema_artifacts" {
  provider = databricks.workspace
  schema   = "${databricks_catalog.bronze.name}.${databricks_schema.artifacts.name}"
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME", "READ_VOLUME", "WRITE_VOLUME"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_schema.artifacts
  ]
}

# Grant sur le volume packages (crucial pour le CD)
resource "databricks_grants" "volume_packages" {
  provider = databricks.workspace
  volume   = "${databricks_catalog.bronze.name}.${databricks_schema.artifacts.name}.${databricks_volume.packages.name}"
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_volume.packages
  ]
}

# Grants sur les volumes metadata, config, schema
resource "databricks_grants" "volume_metadata" {
  provider = databricks.workspace
  volume   = "${databricks_catalog.bronze.name}.${databricks_schema.artifacts.name}.${databricks_volume.metadata.name}"
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_volume.metadata
  ]
}

resource "databricks_grants" "volume_config" {
  provider = databricks.workspace
  volume   = "${databricks_catalog.bronze.name}.${databricks_schema.artifacts.name}.${databricks_volume.config.name}"
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_volume.config
  ]
}

resource "databricks_grants" "volume_schema" {
  provider = databricks.workspace
  volume   = "${databricks_catalog.bronze.name}.${databricks_schema.artifacts.name}.${databricks_volume.schema.name}"
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_volume.schema
  ]
}

# Grant sur le volume init_scripts
resource "databricks_grants" "volume_init_scripts" {
  provider = databricks.workspace
  volume   = "${databricks_catalog.bronze.name}.${databricks_schema.artifacts.name}.${databricks_volume.init_scripts.name}"
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_volume.init_scripts
  ]
}

# Grant sur les External Locations
resource "databricks_grants" "external_location_bronze" {
  provider          = databricks.workspace
  external_location = databricks_external_location.bronze.name
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_external_location.bronze,
    null_resource.grant_storage_access # to ensure access connector setup is done
  ]
}

resource "databricks_grants" "external_location_silver" {
  provider          = databricks.workspace
  external_location = databricks_external_location.silver.name
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_external_location.silver,
    null_resource.grant_storage_access # to ensure access connector setup is done
  ]
}

resource "databricks_grants" "external_location_gold" {
  provider          = databricks.workspace
  external_location = databricks_external_location.gold.name
  
  grant {
    principal  = databricks_service_principal.github_actions.application_id
    privileges = ["CREATE_EXTERNAL_TABLE", "READ_FILES", "WRITE_FILES"]
  }
  
  depends_on = [
    databricks_service_principal.github_actions,
    databricks_external_location.gold,
    null_resource.grant_storage_access # to ensure access connector setup is done
  ]
}
