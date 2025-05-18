import sys
from importlib import import_module
from mds.utils.args_helper import parse_args

job_param = "job"
log_param = "log"
is_unity_param = "is_unity"

def main() -> None:
    args_dict = parse_args(sys.argv[1:])
    job = args_dict.pop(job_param)
    _ = args_dict.get(is_unity_param, False)
    mod = import_module(job)
    main = getattr(mod, "main")
    log = False if log_param not in args_dict else eval(args_dict.pop(log_param))

    # Execute job using the other passed arguments
    if not log:
        # some operation could not be logged in databricks log table
        # for example the ddl creation job will fail if we log before creating the log table 
        main(**args_dict)
    else:
        pass