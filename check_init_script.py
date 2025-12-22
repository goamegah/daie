import os
from databricks.sdk import WorkspaceClient

host = os.getenv('DATABRICKS_HOST')
client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
tenant_id = os.getenv('AZURE_TENANT_ID')

# Nettoyer les variables conflictuelles
for key in ['DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET']:
    if key in os.environ:
        del os.environ[key]

w = WorkspaceClient(
    host=host,
    azure_client_id=client_id,
    azure_client_secret=client_secret,
    azure_tenant_id=tenant_id
)

init_script_path = "/Volumes/daie_chn_dev_bronze/artifacts/init_scripts/dev/install_daie_package.sh"

print(f"Checking if init script exists: {init_script_path}")
try:
    files = list(w.files.list_directory_contents("/Volumes/daie_chn_dev_bronze/artifacts/init_scripts/dev"))
    print(f"✅ Files found:")
    for f in files:
        print(f"  - {f.name}")
except Exception as e:
    print(f"❌ Error: {e}")
