resource "null_resource" "grant_storage_access" {
  provisioner "local-exec" {
    command = "../scripts/grant-access-connector.sh"
  }
  
  depends_on = [azurerm_databricks_access_connector.this]
  
  triggers = {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }
}
