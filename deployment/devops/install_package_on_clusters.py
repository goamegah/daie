#!/usr/bin/env python3
"""
Script pour installer automatiquement le package daie sur les clusters Databricks.
Usage: python install_package_on_clusters.py <environment> [cluster_name]
"""

import os
import sys
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Library, LibraryInstallStatus

def get_latest_wheel(w: WorkspaceClient, catalog: str, developer: str = "dev") -> str:
    """Trouve le dernier wheel dans le volume."""
    volume_path = f"/Volumes/{catalog}/artifacts/packages/{developer}"
    
    try:
        files = list(w.files.list_directory_contents(volume_path))
        wheels = [f for f in files if f.name.endswith('.whl') and f.name.startswith('daie-')]
        
        if not wheels:
            raise FileNotFoundError(f"No wheel files found in {volume_path}")
        
        # Trier par nom (version) et prendre le dernier
        latest = sorted(wheels, key=lambda x: x.name)[-1]
        wheel_path = f"{volume_path}/{latest.name}"
        
        print(f"✅ Found latest wheel: {latest.name}")
        return wheel_path
        
    except Exception as e:
        print(f"❌ Error finding wheel: {e}")
        raise

def install_library_on_cluster(w: WorkspaceClient, cluster_id: str, cluster_name: str, wheel_path: str):
    """Installe une library sur un cluster."""
    try:
        print(f"\n📦 Installing {wheel_path} on cluster {cluster_name} ({cluster_id})...")
        
        library = Library(whl=wheel_path)
        w.libraries.install(cluster_id=cluster_id, libraries=[library])
        
        print(f"✅ Library installation initiated")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

def main():
    if len(sys.argv) < 2:
        print("Usage: python install_package_on_clusters.py <environment> [developer_name]")
        sys.exit(1)
    
    environment = sys.argv[1]
    developer_name = sys.argv[2] if len(sys.argv) > 2 else "dev"
    
    host = os.getenv('DATABRICKS_HOST')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    
    if not all([host, client_id, client_secret, tenant_id]):
        print("❌ Missing required environment variables")
        sys.exit(1)
    
    catalog = f"daie_chn_{environment}_bronze"
    
    print(f"🔧 Config: Env={environment}, Developer={developer_name}, Catalog={catalog}\n")
    
    print("🔐 Connecting...")
    w = WorkspaceClient(host=host, azure_client_id=client_id, azure_client_secret=client_secret, azure_tenant_id=tenant_id)
    user = w.current_user.me()
    print(f"✅ Connected as: {user.user_name}\n")
    
    wheel_path = get_latest_wheel(w, catalog, developer_name)
    
    print("\n📋 Listing clusters...")
    clusters = list(w.clusters.list())
    
    # Filtrer par tag developer
    if developer_name:
        clusters = [c for c in clusters if c.custom_tags and c.custom_tags.get('Developer') == developer_name]
    
    if not clusters:
        print(f"⚠️  No clusters found for developer: {developer_name}")
        print(f"\n💡 Tip: Create a cluster first:")
        print(f"   python deployment/devops/manage_cluster.py create {environment} {developer_name}")
        print(f"\n   Or via GitHub Actions:")
        print(f"   Actions > CD - Déploiement Databricks")
        print(f"   Gérer le cluster: create")
        return 0  # Exit success - pas d'erreur, juste pas de clusters
    
    print(f"Found {len(clusters)} cluster(s) for {developer_name}:")
    for cluster in clusters:
        state = cluster.state.value if cluster.state else "UNKNOWN"
        print(f"  - {cluster.cluster_name} ({cluster.cluster_id}) - {state}")
    
    print("\n" + "="*60)
    installed_count = 0
    for cluster in clusters:
        try:
            install_library_on_cluster(w, cluster.cluster_id, cluster.cluster_name, wheel_path)
            installed_count += 1
        except Exception as e:
            print(f"⚠️  Failed on {cluster.cluster_name}: {e}")
            continue
    
    print("\n" + "="*60)
    print("✅ INSTALLATION COMPLETE")
    print(f"📦 Package: {wheel_path}")
    print(f"🎯 Installed on {installed_count}/{len(clusters)} cluster(s)")
    
    return 0 if installed_count > 0 else 1

if __name__ == "__main__":
    sys.exit(main())