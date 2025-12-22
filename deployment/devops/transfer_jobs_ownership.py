#!/usr/bin/env python3
"""
Script pour transférer l'ownership des jobs Databricks au Service Principal.
Usage: python transfer_jobs_ownership.py <environment>
"""

import sys
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest
from databricks.sdk.service.jobs import JobPermissionLevel

def transfer_jobs_ownership(w: WorkspaceClient, env: str, sp_application_id: str) -> None:
    """
    Transfère l'ownership de tous les jobs au Service Principal.
    
    Args:
        w: WorkspaceClient
        env: Environnement (dev, test, prod)
        sp_application_id: Application ID du Service Principal
    """
    
    print(f"🔍 Listing all jobs...")
    jobs = list(w.jobs.list())
    
    if not jobs:
        print(f"⚠️  No jobs found in environment: {env}")
        print(f"\n💡 Tip: Create workflows in Databricks UI (Workflows menu)")
        return
    
    print(f"   Found {len(jobs)} job(s)\n")
    
    print(f"{'='*60}")
    print(f"Transferring ownership to SP: {sp_application_id}")
    print(f"{'='*60}\n")
    
    success_count = 0
    error_count = 0
    
    for job in jobs:
        job_id = job.job_id
        job_name = job.settings.name if job.settings else f"Job {job_id}"
        
        print(f"📋 {job_name} (ID: {job_id})")
        
        try:
            # Définir les permissions: SP = OWNER, admins = CAN_MANAGE
            w.jobs.set_permissions(
                job_id=str(job_id),
                access_control_list=[
                    AccessControlRequest(
                        service_principal_name=sp_application_id,
                        permission_level=JobPermissionLevel.IS_OWNER
                    ),
                    AccessControlRequest(
                        group_name="admins",
                        permission_level=JobPermissionLevel.CAN_MANAGE
                    )
                ]
            )
            print(f"   ✅ Ownership transferred\n")
            success_count += 1
            
        except Exception as e:
            print(f"   ❌ Error: {e}\n")
            error_count += 1
    
    print(f"{'='*60}")
    print(f"✅ TRANSFER COMPLETE")
    print(f"   Success: {success_count}/{len(jobs)}")
    if error_count > 0:
        print(f"   Errors: {error_count}")
    print(f"{'='*60}\n")
    
    if success_count > 0:
        print(f"💡 Jobs can now run as Service Principal")
        print(f"   Update job settings: Run as > Service Principal > {sp_application_id}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python transfer_jobs_ownership.py <environment>")
        print("\nExamples:")
        print("  python transfer_jobs_ownership.py dev")
        print("  python transfer_jobs_ownership.py prod")
        print("\nDescription:")
        print("  Transfers ownership of ALL jobs/workflows to the Service Principal.")
        print("  The SP is shared across all developers in the environment.")
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
    print(f"Transfer Jobs Ownership")
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
        transfer_jobs_ownership(w, environment, sp_application_id)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
