"""config.py - Module for configuration utilities."""
import json
from pathlib import Path
from typing import Dict, Any
# mds/utils/config.py

def read_json(path: Path) -> Dict[str, Any]:
    """Charge un JSON et renvoie un dictionnaire."""
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)