import sys
from daie.utils import spark_utils as su

def grant_access_to_catalog() -> None:
    dbw_env = su.get_main_env()
    su.spark.sql(f"GRANT USAGE, READ_METADATA, SELECT ON CATALOG hive_metastore TO `G-DE-daie-chn-{dbw_env}`")
def main(**_) -> None:
    grant_access_to_catalog()

if __name__ == "__main__":
    from  daie.utils.args_helper import parse_args
    args_dict = parse_args(sys.argv[1:])
    main(**args_dict)