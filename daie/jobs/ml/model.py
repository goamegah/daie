from __future__ import annotations
from sklearn.ensemble import RandomForestClassifier

def get_model(n_estimators: int = 200) -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced",
    )
