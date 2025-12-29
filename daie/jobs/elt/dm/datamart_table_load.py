# import sys
# import daie.utils.elt.common as ec
# import daie.utils.spark_utils as su
# from pyspark.sql import DataFrame


# JOB = "daie.jobs.elt.dm.datamart_table_load"


# def start(
#     env: str,
#     metadata: dict
# ) -> dict:
#     inputs: dict = {}
#     for entity in metadata["inputs"]["entities"]:
#         entity_name: str = entity["entity"]
#         inputs[entity_name] = dm_df

# def main(
#     env: str,
#     datamart: str,
#     entity: str,
#     **_
# ) -> None:
#     metadata: dict = ec.get_datamart_metadata(env=env, datamart=datamart, entity=entity)
#     inputs: dict = start(env, metadata)


# if __name__ == "__main__":
#     from  daie.utils.args_helper import parse_args
#     args_dict = parse_args(sys.argv[1:])
#     main(**args_dict)