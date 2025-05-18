from typing import List, Dict, Any

def parse_args(args: List[str]) -> Dict[str, str]:
    parsed_args = dict(arg.replace("--","").split('=') for arg in args)
    if 'is_unity' in parsed_args:
        if parsed_args['is_unity'].lower() == 'true':
            parsed_args['is_unity'] = True
        else:
            parsed_args['is_unity'] = False
    return parsed_args

def merge_dicts(dict1: Dict[Any, Any], dict2: Dict[Any, Any]) -> Dict[Any, Any]:
    return {**dict1, **dict2}

def snake_case(value:str)->str:
    return value.lower().replace(" ", "_").replace("/", "_").replace(".", "_").replace("\\", "_").replace("-", "_")