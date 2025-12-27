import sys
import daie.utils.elt.common as ec
import daie.utils.spark_utils as su
from pyspark.sql import DataFrame


JOB = "daie.jobs.elt.raw_table_load"
RAW_KEY = "raw"
NEW_KEY = "new"


def get_options(
    metadata: dict
) -> dict:
    options = metadata.get("options", {})
    return options

def load_from_raw(
    metadata: dict,
    file_location: str,
    options: dict
):
    format = metadata.get("format", "")
    raw_df = su.spark.read.options(**options).format(format).load(file_location)
    return raw_df

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
):
    options = get_options(metadata=metadata)
    raw_df = load_from_raw(
        metadata=metadata,
        file_location=ec.get_or_create_volume_location_from_metadata(env=env, metadata=metadata, sub_folder="raw_files", stage="raw"),
        options=options
    )
    inputs = {
        RAW_KEY: raw_df
    }
    return inputs

def run(
    inputs: dict,
    metadata: dict
):
    raw_df = inputs[RAW_KEY]
    transformed_df = transform_dataframe_columns(
        df=raw_df,
        metadata=metadata
    )

    outputs = {
        NEW_KEY: transformed_df
    }
    return outputs

def end(
    env: str,
    metadata: dict,
    outputs: dict[str, DataFrame]
):
    new_df = outputs[NEW_KEY]
    su.write_delta_table(
        dataframe=new_df,
        table_identifier=ec.get_raw_table_identifier_from_metadata(env=env, metadata=metadata)
    )

def main(
    env: str,
    source: str,
    entity: str,
    **_
):
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

    # transform logic
    outputs = run(inputs=inputs, metadata=metadata)

    # write data back to table bronze layer
    end(
        env=env,
        metadata=metadata,
        outputs=outputs
    )

if __name__ == "__main__":
    from  daie.utils.args_helper import parse_args
    args_dict = parse_args(sys.argv[1:])
    main(**args_dict)