resource "null_resource" "grant_storage_access" {
  depends_on = [
    azurerm_databricks_access_connector.this,
    azurerm_role_assignment.access_connector_storage,
    azurerm_role_assignment.sp_storage
  ]
  
  # This ensures role assignments are created before grants
  provisioner "local-exec" {
    command = "echo 'Storage access granted for Access Connector and Service Principal'"
  }
  
  triggers = {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }
}
