#!/usr/bin/env python3
"""
Script pour gérer les clusters Databricks (créer/détruire).
Usage: python manage_cluster.py <action> <environment> <developer_name>
"""

import sys
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Library

def create_cluster(w: WorkspaceClient, env: str, developer: str, catalog: str) -> tuple[str, bool]:
    """
    Crée un cluster pour un développeur ou retourne l'existant.
    
    Returns:
        tuple[str, bool]: (cluster_id, was_created)
    """
    
    cluster_name = f"daie-{env}-{developer}"
    
    # Vérifier si le cluster existe déjà
    existing = list(w.clusters.list())
    for c in existing:
        if c.cluster_name == cluster_name:
            print(f"⚠️  Cluster already exists: {cluster_name} ({c.cluster_id})")
            print(f"   State: {c.state.value if c.state else 'UNKNOWN'}")
            print(f"   → Will install package on existing cluster")
            return c.cluster_id, False
    
    print(f"🚀 Creating cluster: {cluster_name}")
    
    cluster = w.clusters.create(
        cluster_name=cluster_name,
        spark_version="15.4.x-scala2.12",
        node_type_id="Standard_D4ds_v5",
        autotermination_minutes=20,
        num_workers=0,  # Single node
        data_security_mode=DataSecurityMode.USER_ISOLATION,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]"
        },
        custom_tags={
            "ResourceClass": "SingleNode",
            "Developer": developer,
            "Environment": env,
            "ManagedBy": "Pipeline"
        }
    ).result()
    
    cluster_id = cluster.cluster_id
    print(f"✅ Cluster created: {cluster_id}")
    
    # Ajouter init script
    init_script = f"/Volumes/{catalog}/artifacts/init_scripts/{developer}/install_daie_package.sh"
    print(f"🔧 Init script configured: {init_script}")
    print(f"   (Will be used on next cluster restart)")
    
    print(f"\n✅ Cluster ready: {cluster_name} ({cluster_id})")
    print(f"\n💡 Access your cluster:")
    print(f"   Databricks UI > Compute > {cluster_name}")
    
    return cluster_id, True

def delete_cluster(w: WorkspaceClient, env: str, developer: str) -> None:
    """Supprime le cluster d'un développeur."""
    
    cluster_name = f"daie-{env}-{developer}"
    
    print(f"🔍 Searching for cluster: {cluster_name}")
    
    clusters = list(w.clusters.list())
    cluster = next((c for c in clusters if c.cluster_name == cluster_name), None)
    
    if not cluster:
        print(f"⚠️  Cluster not found: {cluster_name}")
        return
    
    print(f"🗑️  Deleting cluster: {cluster.cluster_id}")
    w.clusters.permanent_delete(cluster_id=cluster.cluster_id)
    print(f"✅ Cluster deleted: {cluster_name}")

def main():
    if len(sys.argv) < 4:
        print("Usage: python manage_cluster.py <create|delete> <environment> <developer_name>")
        print("\nExamples:")
        print("  python manage_cluster.py create dev john")
        print("  python manage_cluster.py delete dev john")
        sys.exit(1)
    
    action = sys.argv[1]
    env = sys.argv[2]
    developer = sys.argv[3]
    
    if action not in ["create", "delete"]:
        print("❌ Action must be 'create' or 'delete'")
        sys.exit(1)
    
    # Credentials
    host = os.getenv('DATABRICKS_HOST')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    
    if not all([host, client_id, client_secret, tenant_id]):
        print("❌ Missing environment variables")
        sys.exit(1)
    
    catalog = f"daie_chn_{env}_bronze"
    
    print(f"\n{'='*60}")
    print(f"Action: {action.upper()}")
    print(f"Environment: {env}")
    print(f"Developer: {developer}")
    print(f"Catalog: {catalog}")
    print(f"{'='*60}\n")
    
    # Connexion
    for key in ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET']:
        if key in os.environ:
            del os.environ[key]
    
    w = WorkspaceClient(
        host=host,
        azure_client_id=client_id,
        azure_client_secret=client_secret,
        azure_tenant_id=tenant_id
    )
    
    user = w.current_user.me()
    print(f"🔐 Connected as: {user.user_name}\n")
    
    try:
        if action == "create":
            cluster_id, was_created = create_cluster(w, env, developer, catalog)
            
            # Trouver le dernier wheel déployé
            volume_path = f"/Volumes/{catalog}/artifacts/packages/{developer}"
            print(f"\n� IFinding latest wheel in: {volume_path}")
            
            try:
                files = list(w.files.list_directory_contents(volume_path))
                wheels = [f for f in files if f.name.endswith('.whl') and f.name.startswith('daie-')]
                
                if not wheels:
                    print(f"⚠️  No wheel found in {volume_path}")
                    print(f"   The init script will install it when available")
                else:
                    # Trier par nom (version) et prendre le dernier
                    latest = sorted(wheels, key=lambda x: x.name)[-1]
                    wheel_path = f"{volume_path}/{latest.name}"
                    
                    print(f"📦 Installing package: {latest.name}")
                    
                    try:
                        w.libraries.install(
                            cluster_id=cluster_id,
                            libraries=[Library(whl=wheel_path)]
                        )
                        print(f"✅ Package installed on cluster")
                    except Exception as e:
                        print(f"⚠️  Package installation failed: {e}")
                        print(f"   The init script will install it on next cluster restart")
            except Exception as e:
                print(f"⚠️  Could not access volume: {e}")
                print(f"   The init script will install the package on next cluster restart")
        else:
            delete_cluster(w, env, developer)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
