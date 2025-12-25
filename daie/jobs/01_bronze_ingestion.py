import os
import shutil
from pathlib import Path

LANDING = "../../data"
BRONZE_ROOT = "../../data/bronze"

FILES = [
    "caract-2023.csv",
    "lieux-2023.csv",
    "usagers-2023.csv",
    "vehicules-2023.csv",
]

def main():
    os.makedirs(BRONZE_ROOT, exist_ok=True)

    missing = []
    for f in FILES:
        src = Path(LANDING) / f
        if not src.exists():
            missing.append(str(src))
    if missing:
        raise FileNotFoundError("Fichiers manquants:\n" + "\n".join(missing))

    for f in FILES:
        src = Path(LANDING) / f
        dst = Path(BRONZE_ROOT) / f
        shutil.copyfile(src, dst)

    print("JOB 1 OK - fichiers copiés en bronze:")
    for f in FILES:
        print(" -", str(Path(BRONZE_ROOT) / f))

if __name__ == "__main__":
    main()
