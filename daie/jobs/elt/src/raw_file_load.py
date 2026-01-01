# daie/jobs/elt/raw_file_load.py
import sys
import daie.utils.elt.common as ec
import daie.utils.spark_utils as su


JOB = "daie.jobs.elt.src.raw_file_load"


def move_files(src, dst) -> None:
    dbutils = su.get_dbutils()
    dbutils.fs.mkdirs(dst)
    for file in dbutils.fs.ls(src):
        src_path = file.path
        file_name = src_path.split("/")[-1]
        dst_path = f"{dst}/{file_name}"
        print(f"Moving file from '{src_path}' to '{dst_path}'")
        dbutils.fs.mv(src_path, dst_path, recurse=True)

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
    transient_dir = metadata["transient_dir"]
    raw_base_path = ec.get_or_create_volume_location_from_metadata(env=env, metadata=metadata, sub_folder="raw_files", stage="raw")
    move_files(src=transient_dir, dst=raw_base_path)

if __name__ == "__main__":
    from  daie.utils.args_helper import parse_args
    args_dict = parse_args(sys.argv[1:])
    main(**args_dict)