import sys
import daie.utils.elt.common as ec
import daie.utils.spark_utils as su
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


JOB = "daie.jobs.elt.dm.datamart_table_load"
DATAMART_KEY = "datamart"


def start(
    env: str,
    metadata: dict
) -> dict:
    inputs: dict = {}
    for entity in metadata["inputs"]["entities"]:
        entity_name: str = entity["entity"]
        source_metadata: dict = ec.get_source_metadata(env=env, entity=entity_name)
        curated_df: DataFrame = su.read_delta_table_if_exists(
            table_identifier=ec.get_curated_table_identifier_from_metadata(env=env, metadata=source_metadata)
        )
        if curated_df is not None:
            inputs[entity_name] = curated_df
    return inputs

def run(
    inputs: dict,
    metadata: dict
) -> dict:
    joined_df: DataFrame = None
    for entity in metadata["inputs"]["entities"]:
        curated_df: DataFrame = inputs.get(entity["entity"])
        if joined_df:
            joined_df = joined_df.join(
                curated_df, 
                on=[F.col(key["join_entity_key"]) == F.col(key["main_entity_key"]) for key in entity.get("join", {}).get("keys", [])], 
                how=entity.get("join", {}).get("type", "inner")
            )
        else:
            joined_df = curated_df
    outputs: dict = {
        DATAMART_KEY: joined_df
    }
    return outputs

def end(
    env: str,
    outputs: dict,
    metadata: dict
) -> None:
    datamart_df: DataFrame = outputs[DATAMART_KEY]
    su.write_delta_table(
        df=datamart_df,
        table_identifier=ec.get_datamart_table_identifier_from_metadata(env=env, metadata=metadata),
        mode="overwrite"
    )

def main(
    env: str,
    datamart: str,
    entity: str,
    **_
) -> None:
    metadata: dict = ec.get_datamart_metadata(env=env, datamart=datamart, entity=entity)
    inputs: dict = start(env, metadata)
    outputs: dict = run(inputs, metadata)
    end(
        env=env,
        outputs=outputs,
        metadata=metadata
    )

if __name__ == "__main__":
    from  daie.utils.args_helper import parse_args
    args_dict = parse_args(sys.argv[1:])
    main(**args_dict)