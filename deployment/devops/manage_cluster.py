#!/usr/bin/env python3
"""
Script pour gérer les clusters Databricks (créer/détruire).
Usage: python manage_cluster.py <action> <environment> <developer_name>
"""

import sys
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, Library

def create_cluster(w: WorkspaceClient, env: str, developer: str, catalog: str) -> str:
    """Crée un cluster pour un développeur."""
    
    cluster_name = f"daie-{env}-{developer}"
    
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
    
    # Installer le package
    wheel_path = f"/Volumes/{catalog}/artifacts/packages/{developer}/daie-0.0.1-py3-none-any.whl"
    print(f"📦 Installing package: {wheel_path}")
    
    w.libraries.install(
        cluster_id=cluster_id,
        libraries=[Library(whl=wheel_path)]
    )
    
    # Ajouter init script
    init_script = f"/Volumes/{catalog}/artifacts/init_scripts/{developer}/install_daie_package.sh"
    print(f"🔧 Init script: {init_script}")
    
    print(f"\n✅ Cluster ready: {cluster_name} ({cluster_id})")
    return cluster_id

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
            create_cluster(w, env, developer, catalog)
        else:
            delete_cluster(w, env, developer)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
