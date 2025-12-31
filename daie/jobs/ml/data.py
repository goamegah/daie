import pandas as pd

def load_data(caract_path: str, usagers_path: str) -> pd.DataFrame:
    # Load CSVs
    caract = pd.read_csv(
        caract_path,
        sep=";",
        encoding="latin1",
        low_memory=False
    )
    usagers = pd.read_csv(
        usagers_path,
        sep=";",
        encoding="latin1",
        low_memory=False
    )

    # Normalize column names
    caract.columns = caract.columns.str.strip().str.lower()
    usagers.columns = usagers.columns.str.strip().str.lower()

    # Keep useful columns
    caract = caract[ feature := [
        "num_acc",
        "lum",
        "atm",
        "col",
        "int",
        "hrmn",
    ]]

    usagers = usagers[[
        "num_acc",
        "grav",
    ]]

    # Convert hrmn "HH:MM" -> minutes
    caract["hrmn"] = (
        caract["hrmn"]
        .astype(str)
        .str.split(":")
        .apply(lambda x: int(x[0]) * 60 + int(x[1]) if len(x) == 2 else None)
    )

    # Aggregate grav per accident
    grav_per_acc = (
        usagers
        .groupby("num_acc", as_index=False)
        .agg({"grav": "max"})
    )

    # Join
    df = caract.merge(grav_per_acc, on="num_acc", how="inner")

    df = df.dropna()

    return df
