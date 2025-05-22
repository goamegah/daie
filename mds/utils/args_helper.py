"""Gestionnaire de secrets pour Databricks."""

from typing import List, Dict, Any

def parse_args(args: List[str]) -> Dict[str, str]:
    """
    Parse les arguments de la ligne de commande.
    Args:
        args (List[str]): Liste d'arguments de la ligne de commande.
    Returns:
        Dict[str, str]: Dictionnaire des arguments parsés.
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
    Merge deux dictionnaires.
    Args:
        dict1 (Dict[Any, Any]): Premier dictionnaire.
        dict2 (Dict[Any, Any]): Deuxième dictionnaire.
    Returns:
        Dict[Any, Any]: Dictionnaire fusionné.
    """
    return {**dict1, **dict2}

def snake_case(value:str)->str:
    """
    Convertit une chaîne en snake_case.
    Args:
        value (str): Chaîne à convertir.
    Returns:
        str: Chaîne convertie en snake_case.
    """
    return value.lower().replace(" ", "_").replace("/", "_").replace(".", "_").replace("\\", "_").replace("-", "_")
