"""
model.py — Chargement et utilitaires modèle XGBoost
"""
import os
import json
import numpy as np
from typing import Dict, List

_HERE = os.path.dirname(os.path.abspath(__file__))


def load_metrics(segment: str, saved_dir: str) -> dict:
    path = os.path.join(saved_dir, f'xgb_metrics_{segment}.json')
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {'r2': 0.0, 'mae': 0.0, 'mape': 0.0}


def get_feature_importance(model, feature_cols: List[str], top_n: int = 5) -> Dict[str, float]:
    try:
        fi  = model.feature_importances_
        tot = fi.sum()
        if tot == 0:
            return {}
        scores = {c: round(float(fi[i] / tot), 4) for i, c in enumerate(feature_cols)}
        return dict(sorted(scores.items(), key=lambda x: -x[1])[:top_n])
    except Exception:
        return {}
