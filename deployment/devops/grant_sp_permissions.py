#!/usr/bin/env python3
"""
Script pour s'accorder les permissions CAN_USE sur le Service Principal.
Usage: python grant_sp_permissions.py <environment>
"""

import os
import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

def grant_sp_permissions(w: WorkspaceClient, sp_application_id: str) -> None:
    """
    Accorde la permission CAN_USE au user actuel sur le Service Principal.
    
    Args:
        w: WorkspaceClient
        sp_application_id: Application ID du Service Principal
    """
    
    print(f"🔍 Finding Service Principal...")
    
    # Trouver le Service Principal par son application_id
    service_principals = list(w.service_principals.list(filter=f'applicationId eq "{sp_application_id}"'))
    
    if not service_principals:
        print(f"❌ Service Principal not found: {sp_application_id}")
        sys.exit(1)
    
    sp = service_principals[0]
    sp_id = sp.id
    sp_display_name = sp.display_name
    
    print(f"✅ Found: {sp_display_name} (ID: {sp_id})\n")
    
    # Récupérer l'utilisateur actuel
    current_user = w.current_user.me()
    user_name = current_user.user_name
    
    print(f"👤 Current user: {user_name}")
    print(f"📋 Service Principal: {sp_display_name}")
    print(f"\n{'='*60}")
    print(f"Granting CAN_USE permission...")
    print(f"{'='*60}\n")
    
    try:
        # Utiliser l'API permissions avec object_type service-principals
        object_id = f"/service-principals/{sp_id}"
        
        # Récupérer les permissions actuelles
        try:
            current_permissions = w.permissions.get(request_object_type="service-principals", request_object_id=sp_id)
            access_control_list = list(current_permissions.access_control_list) if current_permissions.access_control_list else []
        except:
            # Si pas de permissions existantes, créer une liste vide
            access_control_list = []
        
        # Vérifier si l'utilisateur a déjà la permission
        user_already_has_permission = any(
            acl.user_name == user_name for acl in access_control_list
        )
        
        if user_already_has_permission:
            print(f"✅ User {user_name} already has CAN_USE permission")
            print(f"\n{'='*60}")
            print(f"✅ SUCCESS (already configured)")
            print(f"{'='*60}\n")
            return
        
        # Ajouter la permission CAN_USE pour l'utilisateur actuel
        access_control_list.append(
            AccessControlRequest(
                user_name=user_name,
                permission_level=PermissionLevel.CAN_USE
            )
        )
        
        # Appliquer les permissions
        w.permissions.set(
            request_object_type="service-principals",
            request_object_id=sp_id,
            access_control_list=access_control_list
        )
        
        print(f"✅ Permission CAN_USE granted to {user_name}")
        print(f"\n{'='*60}")
        print(f"✅ SUCCESS")
        print(f"{'='*60}\n")
        print(f"💡 You can now:")
        print(f"   1. Configure jobs to 'Run as' the Service Principal")
        print(f"   2. Edit job > Advanced > Run as > Service Principal > {sp_display_name}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python grant_sp_permissions.py <environment>")
        print("\nExamples:")
        print("  python grant_sp_permissions.py dev")
        print("  python grant_sp_permissions.py prod")
        print("\nDescription:")
        print("  Grants CAN_USE permission on the Service Principal to the current user.")
        print("  This allows you to use the SP as 'Run as' identity in jobs.")
        sys.exit(1)
    
    environment = sys.argv[1]
    
    # Credentials
    host = os.getenv('DATABRICKS_HOST')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    
    if not all([host, client_id, client_secret, tenant_id]):
        print("❌ Missing required environment variables")
        print("   Required: DATABRICKS_HOST, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Grant Service Principal Permissions")
    print(f"Environment: {environment}")
    print(f"{'='*60}\n")
    
    # Connexion
    for key in ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET']:
        if key in os.environ:
            del os.environ[key]
    
    print("🔐 Connecting...")
    w = WorkspaceClient(
        host=host,
        azure_client_id=client_id,
        azure_client_secret=client_secret,
        azure_tenant_id=tenant_id
    )
    
    user = w.current_user.me()
    print(f"✅ Connected as: {user.user_name}\n")
    
    # Le SP application_id est le même que le client_id
    sp_application_id = client_id
    
    try:
        grant_sp_permissions(w, sp_application_id)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
