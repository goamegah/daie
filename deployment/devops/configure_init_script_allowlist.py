#!/usr/bin/env python3
"""
Script pour configurer l'allowlist des init scripts dans Databricks.
Usage: python configure_init_script_allowlist.py <environment>
"""

import sys
import os
from databricks.sdk import WorkspaceClient

def configure_allowlist(w: WorkspaceClient, env: str) -> None:
    """Configure l'allowlist pour les init scripts."""
    
    catalog = f"daie_chn_{env}_bronze"
    
    # Pattern pour autoriser tous les init scripts dans le volume
    allowlist_pattern = f"/Volumes/{catalog}/artifacts/init_scripts/*"
    
    print(f"🔧 Configuring init script allowlist...")
    print(f"   Pattern: {allowlist_pattern}")
    
    try:
        # Récupérer la configuration actuelle
        current_config = w.workspace_conf.get_status()
        
        # Mettre à jour l'allowlist
        w.workspace_conf.set_status({
            "enableInitScriptAllowlist": "true",
            "initScriptAllowlist": allowlist_pattern
        })
        
        print(f"✅ Allowlist configured successfully")
        print(f"\n💡 All init scripts in {allowlist_pattern} are now allowed")
        
    except Exception as e:
        print(f"❌ Error configuring allowlist: {e}")
        print(f"\n⚠️  You may need admin permissions to configure the allowlist")
        print(f"\n📝 Manual configuration:")
        print(f"   1. Go to Databricks Admin Console")
        print(f"   2. Workspace Settings > Advanced")
        print(f"   3. Enable 'Init script allowlist'")
        print(f"   4. Add pattern: {allowlist_pattern}")
        raise

def main():
    if len(sys.argv) < 2:
        print("Usage: python configure_init_script_allowlist.py <environment>")
        print("\nExamples:")
        print("  python configure_init_script_allowlist.py dev")
        print("  python configure_init_script_allowlist.py test")
        sys.exit(1)
    
    env = sys.argv[1]
    
    # Credentials
    host = os.getenv('DATABRICKS_HOST')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    
    if not all([host, client_id, client_secret, tenant_id]):
        print("❌ Missing environment variables")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Environment: {env}")
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
        configure_allowlist(w, env)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
