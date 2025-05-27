""" Unity catalog utils """

# mds/utils/unity_catalog.py
from typing import Optional
from src.utils.spark_utils import get_spark_session

__all__ = ['create_unity_catalog_volume']

def create_unity_catalog_volume(
    catalog: str,
    schema: str,
    volume: str,
    subfolder: Optional[str] = None,
    identifier: Optional[str] = None,
) -> str:
    """
    Creates a Unity Catalog Databricks volume using the SparkSession singleton.

    Args:
        catalog:  Catalog name.
        schema:   Schema name.
        volume:   Volume name.
        subfolder: Subfolder to create (optional).
        identifier: 'catalog.schema.volume' (takes precedence if provided).

    Returns:
        str: Path to the volume (with subfolder if specified).
    """
    spark = get_spark_session()

    if identifier:
        catalog, schema, volume = identifier.split('.')

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

    base = f"/Volumes/{catalog}/{schema}/{volume}"
    return f"{base}/{subfolder}" if subfolder else base
