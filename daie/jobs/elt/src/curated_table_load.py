import sys
import daie.utils.elt.common as ec
import daie.utils.spark_utils as su
from pyspark.sql import DataFrame


JOB = "daie.jobs.elt.curated_table_load"
RAW_KEY = "raw"
CURATED_KEY = "curated"

#TODO: customize per entity
def transform_dataframe_columns(
    raw_df: DataFrame,
    metadata: dict
) -> DataFrame:
    if not raw_df:
        return None
    # we can do something like: 
    # if metadata["entity"] == "accident": ...
    # elif metadata["entity"] == "vehicule": ...
    transformed_df = raw_df
    if metadata["entity"] == "vehicule_v1":
        # example transformation: nothing for now
        transformed_df = raw_df
    return transformed_df

def start(
    env: str,
    metadata: dict
) -> dict[str, DataFrame]:
    raw_df = su.read_delta_table_if_exists(
        table_identifier=ec.get_raw_table_identifier_from_metadata(env=env, metadata=metadata)
    )

    if raw_df is None:
        return
    
    inputs = {
        RAW_KEY: raw_df
    }
    return inputs

def run(
    inputs: dict,
    metadata: dict
) -> dict[str, DataFrame]:
    raw_df = inputs[RAW_KEY]
    curated_df = transform_dataframe_columns(
        raw_df=raw_df,
        metadata=metadata
    )

    outputs = {
        CURATED_KEY: curated_df
    }
    return outputs

def end(
    env: str,
    outputs: dict[str, DataFrame],
    metadata: dict
) -> None:
    curated_df = outputs[CURATED_KEY]
    su.write_delta_table(
        dataframe=curated_df,
        table_identifier=ec.get_curated_table_identifier_from_metadata(env=env, metadata=metadata)
    )
    
def main(
    env: str,
    source: str,
    entity: str,
    **_
) -> None:
    metadata = ec.get_source_metadata(
        env=env,
        source=source,
        entity=entity
    )

    # read data as spark dataframe
    inputs = start(
        env=env,
        metadata=metadata
    )
    if inputs:
        outputs = run(inputs=inputs, metadata=metadata)
        end(env=env, outputs=outputs, metadata=metadata)

if __name__ == "__main__":
    from  daie.utils.args_helper import parse_args
    args_dict = parse_args(sys.argv[1:])
    main(**args_dict)