
def create_unity_catalog_volume(
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    sub_folder: str = None,
    volume_identifier: str = None,
) -> str:
    """
    Create a Unity Catalog volume in Databricks.
    Args:
        catalog_name (str): The name of the catalog.
        schema_name (str): The name of the schema.
        volume_name (str): The name of the volume.
        volume_identifier (str): The volume identifier. eg. catalog_name.schema_name.volume_name
    Returns:
        str: The path to the volume.
    """
    if volume_identifier:
        catalog_name, schema_name, volume_name = volume_identifier.split(".")
    else:
        spark.sql(f"create schema if not exists {catalog_name}.{schema_name}")
        spark.sql(f"create volume if not exists {catalog_name}.{schema_name}.{volume_name}")
        if sub_folder:
            spark.sql(f"create volume if not exists {catalog_name}.{schema_name}.{volume_name}/{sub_folder}")
            
    return f"/Volumes/{catalog_name}/{schema_name}/{volume_name}" if not sub_folder else f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{sub_folder}"