""" Module for command-line argument management.
This module provides functions to parse command-line arguments,
merge dictionaries, and convert strings to snake_case."""

# ade/utils/args_helper.py

from typing import List, Dict, Any

def parse_args(args: List[str]) -> Dict[str, str]:
    """
    Parse command-line arguments.
    Args:
        args (List[str]): List of command-line arguments.
    Returns:
        Dict[str, str]: Dictionary of parsed arguments.
    """
    parsed_args = dict(arg.replace("--","").split('=') for arg in args)
    if 'is_unity' in parsed_args:
        if parsed_args['is_unity'].lower() == 'true':
            parsed_args['is_unity'] = True
        else:
            parsed_args['is_unity'] = False
    return parsed_args

def merge_dicts(dict1: Dict[Any, Any], dict2: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Merge two dictionaries.
    Args:
        dict1 (Dict[Any, Any]): First dictionary.
        dict2 (Dict[Any, Any]): Second dictionary.
    Returns:
        Dict[Any, Any]: Merged dictionary.
    """
    return {**dict1, **dict2}

def snake_case(value:str)->str:
    """
    Convert a string to snake_case.
    Args:
        value (str): String to convert.
    Returns:
        str: Converted string in snake_case.
    """
    return value.lower().replace(" ", "_").replace("/", "_").replace(".", "_").replace("\\", "_").replace("-", "_")
