"""Main entry point for the MDS job execution.
This module is responsible for parsing command line arguments,
importing the specified job module, and executing its main function.
It handles the dynamic import of job modules and passes the necessary arguments
to the job's main function.
"""
# ade/main.py

import sys
from ast import literal_eval
from importlib import import_module
from daie.utils.args_helper import parse_args

JOB_PARAM = "job"
LOG_PARAM = "log"
IS_UNITY_PARAM = "is_unity"

def main() -> None:
    """
    Main function to execute the job specified in the command line arguments.
    It imports the job module dynamically and calls its main function with the provided arguments.
    """
    # Parse command line arguments
    args_dict = parse_args(sys.argv[1:])
    job = args_dict.pop(JOB_PARAM)
    _ = args_dict.get(IS_UNITY_PARAM, False)
    mod = import_module(job)
    fun = getattr(mod, "main")
    log = False if LOG_PARAM not in args_dict else literal_eval(args_dict.pop(LOG_PARAM))

    # Execute job using the other passed arguments
    if not log:
        # some operation could not be logged in databricks log table
        # for example the ddl creation job will fail if we log before creating the log table
        fun(**args_dict)
    else:
        pass