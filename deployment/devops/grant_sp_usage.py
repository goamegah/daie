#!/usr/bin/env python3
"""
Script pour donner la permission CAN_USE sur le Service Principal aux utilisateurs.
Usage: python grant_sp_usage.py <environment>
"""

import sys
import os
from databricks.sdk import WorkspaceClient

def grant_sp_usage(env: str):
    """Donne la permission CAN_USE sur le SP au groupe 'users'."""
    
    # Credentials
    host = os.getenv('DATABRICKS_HOST')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    
    if not all([host, client_id, client_secret, tenant_id]):
        print("❌ Missing environment variables")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Granting SP Usage Permission")
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
    
    # Trouver le Service Principal
    sp_name = f"SP GitHub Actions - {env}"
    print(f"🔍 Searching for Service Principal: {sp_name}")
    
    sps = list(w.service_principals.list(filter=f"displayName eq '{sp_name}'"))
    
    if not sps:
        print(f"❌ Service Principal not found: {sp_name}")
        sys.exit(1)
    
    sp = sps[0]
    print(f"✅ Found: {sp.display_name} (ID: {sp.id})\n")
    
    # Donner la permission CAN_USE au groupe 'users'
    print(f"🔧 Granting CAN_USE permission to group 'users'...")
    
    try:
        # Note: L'API Databricks pour les permissions sur les SP n'est pas directement
        # disponible dans le SDK Python. Il faut utiliser l'API REST.
        import requests
        
        # Get token
        token = w.config.authenticate()
        
        # API endpoint
        url = f"{host}/api/2.0/preview/permissions/servicePrincipals/{sp.id}"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Payload
        payload = {
            "access_control_list": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_USE"
                }
            ]
        }
        
        response = requests.put(url, headers=headers, json=payload)
        
        if response.status_code in [200, 201]:
            print(f"✅ Permission granted successfully!")
            print(f"\n💡 Users can now use this Service Principal in jobs")
        else:
            print(f"⚠️  API response: {response.status_code}")
            print(f"   {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"\n💡 Manual alternative:")
        print(f"   1. Go to Databricks UI > Settings > Identity and Access")
        print(f"   2. Click on Service Principals > {sp_name}")
        print(f"   3. Click Permissions > Add")
        print(f"   4. Select Group: users, Permission: CAN_USE")
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python grant_sp_usage.py <environment>")
        print("\nExample:")
        print("  python grant_sp_usage.py dev")
        sys.exit(1)
    
    env = sys.argv[1]
    
    try:
        grant_sp_usage(env)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
