#!/usr/bin/env python3
"""
Script de déploiement d'artifacts vers Databricks Unity Catalog Volumes.

Usage simple avec Service Principal (comme dans le CD):
    python deploy_artifacts.py metadata dev
    python deploy_artifacts.py metadata my_custom_env
"""

import sys
import os
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def create_volume_if_not_exists(workspace_client: WorkspaceClient, catalog: str, schema: str, volume: str) -> None:
    """
    Vérifie que le volume Unity Catalog existe.
    Note: Le volume doit être créé manuellement ou via Terraform avant le premier déploiement.
    
    Args:
        workspace_client: Client Databricks SDK
        catalog: Nom du catalog
        schema: Nom du schema
        volume: Nom du volume
    """
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
    print(f"📦 Vérification du volume: {volume_path}")
    
    try:
        # Tenter de lister le contenu pour vérifier l'existence
        list(workspace_client.files.list_directory_contents(volume_path))
        print("✅ Volume existe\n")
    except Exception as e:
        if "does not exist" in str(e).lower():
            print(f"❌ Volume n'existe pas!")
            print(f"   Créez-le dans Databricks avec:")
            print(f"   CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
            print(f"   CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume};")
            print()
            raise FileNotFoundError(f"Le volume {catalog}.{schema}.{volume} doit être créé d'abord")
        else:
            # Autre erreur, peut-être que le volume est vide
            print(f"✅ Volume existe (vide)\n")


def upload_directory_to_volume(workspace_client: WorkspaceClient, local_dir: str, volume_path: str) -> int:
    """
    Upload un répertoire local vers un volume Databricks (récursif).
    
    Args:
        workspace_client: Client Databricks SDK
        local_dir: Chemin local du répertoire source
        volume_path: Chemin du volume de destination
    
    Returns:
        Nombre de fichiers uploadés
    """
    local_path = Path(local_dir)
    
    if not local_path.exists():
        raise FileNotFoundError(f"Le répertoire source '{local_dir}' n'existe pas")
    
    # Créer le répertoire de destination
    print(f"📁 Création du dossier: {volume_path}")
    try:
        workspace_client.files.create_directory(volume_path)
        print("✅ Créé\n")
    except DatabricksError as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
            print("⚠️  Existe déjà\n")
        else:
            raise
    
    files_uploaded = 0
    
    # Upload récursif
    for file_path in local_path.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(local_path)
            dest_path = f"{volume_path}/{relative_path}"
            
            # Créer les sous-répertoires si nécessaire
            dest_dir = "/".join(str(dest_path).split("/")[:-1])
            try:
                workspace_client.files.create_directory(dest_dir)
            except DatabricksError:
                pass  # Ignore si existe déjà
            
            print(f"📤 {relative_path}")
            with open(file_path, 'rb') as f:
                workspace_client.files.upload(dest_path, f, overwrite=True)
            files_uploaded += 1
    
    return files_uploaded


def deploy_artifacts(artifact_type: str, env: str, developer_name: str = None) -> None:
    """
    Déploie des artifacts vers un volume Unity Catalog.
    
    Args:
        artifact_type: Type d'artifact (metadata, config, etc.)
        env: Environnement cible (dev, test, prod, ou custom)
        developer_name: Nom du développeur (optionnel, pour env custom)
    """
    # Configuration des artifacts
    artifact_config = {
        "metadata": {
            "source_dir": "lakehouse/metadata",
            "description": "Métadonnées des sources",
            "use_developer_folder": True  # Déployer dans un dossier par développeur
        },
        "config": {
            "source_dir": "config",
            "description": "Fichiers de configuration",
            "use_developer_folder": True
        },
        "schema": {
            "source_dir": "lakehouse/schema",
            "description": "Schémas de données",
            "use_developer_folder": True
        },
        "init_scripts": {
            "source_dir": "deployment/databricks/init_scripts",
            "description": "Scripts d'initialisation des clusters",
            "use_developer_folder": True  # Init scripts par développeur
        }
    }
    
    if artifact_type not in artifact_config:
        raise ValueError(f"Type d'artifact inconnu: {artifact_type}. Valeurs possibles: {list(artifact_config.keys())}")
    
    config = artifact_config[artifact_type]
    source_dir = config["source_dir"]
    use_developer_folder = config["use_developer_folder"]
    
    # Nettoyer les variables conflictuelles
    for key in ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET']:
        if key in os.environ:
            del os.environ[key]
    
    # Récupérer les credentials depuis les variables d'environnement
    host = os.getenv('DATABRICKS_HOST')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    
    if not all([host, client_id, client_secret, tenant_id]):
        raise ValueError("Variables d'environnement manquantes: DATABRICKS_HOST, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID")
    
    # Construire le chemin du volume
    catalog = f"daie_chn_{env}_bronze"
    
    # Utiliser developer_name comme dossier uniquement si configuré
    if use_developer_folder:
        folder_name = developer_name if developer_name else 'dev'
        volume_path = f"/Volumes/{catalog}/artifacts/{artifact_type}/{folder_name}"
    else:
        # Init scripts partagés - pas de dossier développeur
        volume_path = f"/Volumes/{catalog}/artifacts/{artifact_type}"
    
    print(f"\n╔═══════════════════════════════════════╗")
    print(f"║   📦 DÉPLOIEMENT D'ARTIFACTS          ║")
    print(f"╚═══════════════════════════════════════╝")
    print(f"Type: {artifact_type} ({config['description']})")
    print(f"Environnement: {env}")
    if use_developer_folder and developer_name:
        print(f"Développeur: {developer_name}")
    print(f"Source: {source_dir}")
    print(f"Destination: {volume_path}")
    print()
    
    # Connexion à Databricks
    print("🔐 Connexion à Databricks...")
    w = WorkspaceClient(
        host=host,
        azure_client_id=client_id,
        azure_client_secret=client_secret,
        azure_tenant_id=tenant_id
    )
    user = w.current_user.me()
    print(f"✅ Connecté en tant que: {user.user_name}\n")
    
    # Vérifier que le volume existe
    create_volume_if_not_exists(w, catalog, "artifacts", artifact_type)
    
    # Upload des fichiers
    files_count = upload_directory_to_volume(w, source_dir, volume_path)
    
    print(f"\n╔═══════════════════════════════════════╗")
    print(f"║   ✅ DÉPLOIEMENT RÉUSSI               ║")
    print(f"╚═══════════════════════════════════════╝")
    print(f"📊 {files_count} fichier(s) déployé(s)")
    print(f"📍 Location: {volume_path}\n")


def main():
    if len(sys.argv) < 3:
        print("Usage: python deploy_artifacts.py <artifact_type> <env> [developer_name]")
        print()
        print("Arguments:")
        print("  artifact_type    Type d'artifact: metadata, config, schema")
        print("  env              Environnement: dev, test, prod, ou custom")
        print("  developer_name   Nom du développeur (optionnel, pour env custom)")
        print()
        print("Exemples:")
        print("  python deploy_artifacts.py metadata dev")
        print("  python deploy_artifacts.py metadata my_custom_env john")
        print("  python deploy_artifacts.py config prod")
        sys.exit(1)
    
    artifact_type = sys.argv[1]
    env = sys.argv[2]
    developer_name = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        deploy_artifacts(artifact_type, env, developer_name)
    except Exception as e:
        print(f"\n❌ Erreur: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()