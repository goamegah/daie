import yaml
import daie.utils.spark_utils as su

CATALOG = "daie_chn_{dbw_env}_{layer}"

def resolve_catalog(layer):
    dbw_env = su.get_main_env()
    return CATALOG.format(dbw_env=dbw_env, layer=layer)

def resolve_base_dir(env, content):
    storage_account = su.get_storage_account(env)
    transient_base_dir = su.BASE_DIR.format(
        container="transient",
        storage_account=storage_account
    )
    content = content.replace(
        "{{base_dir}}/transient", 
        transient_base_dir
    ).replace(
        "{{env}}", 
        env
    )
    return content

def get_artifact_base_path(env, artifact_type="metadata"):
    """
    Docstring for get_artifact_base_path
    
    :param env: dev, test, prod, or custom env
    :param artifact_type: metadata, package, etc.
    """
    # TODO: 
    volume_base_path = "/Volumes/{catalog}/artifacts/{artifact_type}/{folder_env}"
    if not env in ["dev", "test", "prod"]:
        volume_custom_env_path = volume_base_path.format(
            catalog=resolve_catalog(layer="bronze"),
            artifact_type=artifact_type,
            folder_env=env
        )
        if su.does_path_exist_in_databricks(volume_custom_env_path):
            return volume_custom_env_path
    volume_env_path = volume_base_path.format(
        catalog=resolve_catalog(layer="bronze"),
        artifact_type=artifact_type,
        folder_env=env
    )
    if su.does_path_exist_in_databricks(volume_env_path):
        return volume_env_path
    raise FileNotFoundError(f"The folder for artifacts '{artifact_type}' for env: '{env}' under path '{volume_env_path}' was not found in Databricks.")

def read_artifacts_file(env, artifact_path):
    f = open(artifact_path, encoding="utf-8")
    c = f.read()
    y = resolve_base_dir(env, c)
    f.close()
    metadata = yaml.safe_load(y)
    return metadata

def get_source_metadata(env, source, entity):
    base_path = get_artifact_base_path(env)
    artifact_path = f"{base_path}/source/{source}/{entity}.yml"
    return read_artifacts_file(env, artifact_path)

def build_schema_name(
    env: str,
    stage: str,
    base_name: str
):
    return f"{env}_{stage}_{base_name}"

def get_or_create_volume_location_from_metadata(
    env: str,
    metadata: dict,
    sub_folder: str,
    stage: str="raw",
):
    catalog = resolve_catalog(layer="bronze")
    source = metadata["source"]
    entity = metadata["entity"]
    schema = build_schema_name(
        env=env,
        stage=stage,
        base_name=source
    )
    return su.get_or_create_volume_location(
        catalog=catalog,
        schema=schema,
        entity=entity,
        sub_folder=sub_folder
    )

    