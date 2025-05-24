""" Unity catalog utils"""

# mds/utils/unity_catalog.py
from typing import Optional
from mds.utils.spark_utils import get_spark_session

__all__ = ['create_unity_catalog_volume']

def create_unity_catalog_volume(
    catalog: str,
    schema: str,
    volume: str,
    subfolder: Optional[str] = None,
    identifier: Optional[str] = None,
) -> str:
    """
    Crée un volume Unity Catalog Databricks en réutilisant la SparkSession singleton.
    
    Args:
        catalog:  Nom du catalogue.
        schema:   Nom du schéma.
        volume:   Nom du volume.
        subfolder: Sous-dossier à créer (facultatif).
        identifier: 'catalog.schema.volume' (prioritaire si fourni).

    Returns:
        str: Chemin du volume (avec sous-dossier si précisé).
    """
    spark = get_spark_session()

    if identifier:
        catalog, schema, volume = identifier.split('.')

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

    base = f"/Volumes/{catalog}/{schema}/{volume}"
    return f"{base}/{subfolder}" if subfolder else base
