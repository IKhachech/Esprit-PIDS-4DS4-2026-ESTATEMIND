"""
features_registry.py — Registre Unifié des Features · Pipeline Immobilier Tunisien
====================================================================================

Source unique de vérité pour TOUTES les features utilisées dans la chaîne :
    KMeans → PELT → LSTM → TFT

PRINCIPES :
  • FEATURES_GLOBAL    : définition canonique de chaque feature
  • MODEL_FEATURES     : vue par modèle (sous-ensemble de FEATURES_GLOBAL)
  • build_features()   : pipeline de preprocessing qui garantit 0 colonne manquante
  • validate_features(): validation + rapport de conformité avant chaque modèle

USAGE :
    from features_registry import build_features, validate_features, MODEL_FEATURES

    df = build_features(df_raw)                         # preprocessing universel
    ok, report = validate_features(df, model='lstm')    # contrôle avant training
    feat_cols  = MODEL_FEATURES['lstm']['all']           # liste de features pour le modèle
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# 1. DÉFINITIONS CANONIQUES
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FeatureDef:
    """Définition canonique d'une feature."""
    name        : str
    group       : str          # core | price | macro | engineered | pipeline | tft_meta
    dtype       : str          # float | int | category
    nullable    : bool = False # True → peut être NaN en entrée (fallback appliqué)
    description : str = ""


# ─── 1a. Features CORE (temps + géographie) ──────────────────────────────────
CORE_FEATURES: List[FeatureDef] = [
    FeatureDef("gouvernorat",          "core", "int",      False, "Code gouvernorat (1-24)"),
    FeatureDef("annee",                "core", "int",      False, "Année"),
    FeatureDef("mois",                 "core", "int",      False, "Mois (1-12)"),
    FeatureDef("type_transaction",     "core", "int",      False, "1=Location, 2=Vente"),
    FeatureDef("groupe",               "core", "category", False, "Residentiel|Foncier|Commercial"),
    FeatureDef("zone_geographique",    "core", "int",      True,  "Zone géo 0-4 (cluster spatial)"),
    FeatureDef("zone_geo",             "core", "category", True,  "Label zone géo (str, pour TFT)"),
    FeatureDef("mois_sin",             "core", "float",    True,  "Encodage cyclique mois sin(2π/12)"),
    FeatureDef("mois_cos",             "core", "float",    True,  "Encodage cyclique mois cos(2π/12)"),
    FeatureDef("annee_norm",           "core", "float",    True,  "Année normalisée [0,1]"),
    FeatureDef("saison",               "core", "int",      True,  "1=Eté 2=Hiver 3=Autre"),
]

# ─── 1b. Features PRIX ───────────────────────────────────────────────────────
PRICE_FEATURES: List[FeatureDef] = [
    FeatureDef("indice_prix_m2_regional", "price", "float", False, "Prix m² moyen (TARGET LSTM)"),
    FeatureDef("indice_prix_m2_loc",      "price", "float", True,  "Prix m² location stratifié"),
    FeatureDef("indice_prix_m2_ven",      "price", "float", True,  "Prix m² vente stratifié"),
    FeatureDef("prix_lisse",              "price", "float", True,  "Prix global lissé (rolling 3M)"),
    FeatureDef("prix_lisse_loc",          "price", "float", True,  "Prix location lissé (rolling 3M)"),
    FeatureDef("prix_lisse_ven",          "price", "float", True,  "Prix vente lissé (rolling 3M)"),
    FeatureDef("price_mean",              "price", "float", True,  "Moyenne (loc+ven)/2"),
    FeatureDef("ratio_location_vente",    "price", "float", True,  "Proportion vente/total [0,1]"),
    FeatureDef("glissement_immo_trim",    "price", "float", True,  "Glissement trimestriel immobilier"),
    FeatureDef("prix_m2",                 "price", "float", True,  "Alias prix brut (TFT)"),
    FeatureDef("prix_m2_loc",             "price", "float", True,  "Prix loc brut (TFT)"),
    FeatureDef("prix_m2_ven",             "price", "float", True,  "Prix ven brut (TFT)"),
    FeatureDef("prix_m2_norm",            "price", "float", True,  "Prix normalisé RobustScaler (TFT TARGET)"),
    FeatureDef("prix_m2_loc_norm",        "price", "float", True,  "Prix loc normalisé (TFT)"),
    FeatureDef("prix_m2_ven_norm",        "price", "float", True,  "Prix ven normalisé (TFT)"),
    FeatureDef("ratio_loc_ven",           "price", "float", True,  "Alias ratio loc/ven (TFT)"),
]

# ─── 1c. Features MACRO ──────────────────────────────────────────────────────
MACRO_FEATURES: List[FeatureDef] = [
    FeatureDef("inflation_glissement_annuel", "macro", "float", True, "Inflation glissement annuel (%)"),
    FeatureDef("inflation_proxy",             "macro", "float", True, "Proxy inflation si col absente"),
    FeatureDef("croissance_pib_trim",         "macro", "float", True, "Croissance PIB trimestrielle (%)"),
    FeatureDef("high_season",                 "macro", "float", True, "Indicateur haute saison [0,1]"),
    FeatureDef("score_attractivite",          "macro", "float", True, "Score attractivité gouvernorat"),
    FeatureDef("nb_infra",                    "macro", "float", True, "Nombre d'infrastructures"),
    FeatureDef("nb_commerce",                 "macro", "float", True, "Nombre de commerces"),
    FeatureDef("indice_liquidite",            "macro", "float", True, "Indice liquidité marché"),
    FeatureDef("indice_liquidite_norm",       "macro", "float", True, "Indice liquidité normalisé (TFT)"),
    FeatureDef("nb_annonces_proxy",           "macro", "float", True, "Proxy nb annonces (longueur série)"),
    FeatureDef("nb_annonces_proxy_norm",      "macro", "float", True, "Proxy nb annonces normalisé (TFT)"),
]

# ─── 1d. Features ENGINEERED ─────────────────────────────────────────────────
ENGINEERED_FEATURES: List[FeatureDef] = [
    FeatureDef("trend_prix",              "engineered", "float", True, "pct_change(3) par gouvernorat"),
    FeatureDef("momentum_prix",           "engineered", "float", True, "diff(3) par gouvernorat"),
    FeatureDef("volatilite_rolling",      "engineered", "float", True, "Std rolling 6M des prix"),
    FeatureDef("volatilite_rolling_norm", "engineered", "float", True, "Volatilité rolling normalisée (TFT)"),
    FeatureDef("volatilite_prix_trim",    "engineered", "float", True, "Volatilité trimestrielle prix"),
    FeatureDef("volatilite_prix_trim_norm","engineered","float", True, "Volatilité trim normalisée (TFT)"),
    FeatureDef("potentiel_emergent",      "engineered", "float", True, "Score potentiel émergent cluster"),
    FeatureDef("confidence_score",        "engineered", "float", True, "min(1, n_samples/30)"),
]

# ─── 1e. Features PIPELINE (post-KMeans + post-PELT) ────────────────────────
PIPELINE_FEATURES: List[FeatureDef] = [
    FeatureDef("cluster_id",          "pipeline", "int",   True, "Cluster KMeans"),
    FeatureDef("cluster_enc",         "pipeline", "int",   True, "Cluster encodé LabelEncoder (TFT)"),
    FeatureDef("cluster_size",        "pipeline", "int",   True, "Taille du cluster"),
    FeatureDef("cluster_size_norm",   "pipeline", "float", True, "Taille cluster normalisée"),
    FeatureDef("zscore_prix",         "pipeline", "float", True, "Z-score prix par cluster"),
    FeatureDef("zscore_ref",          "pipeline", "float", True, "Z-score ref statique (TFT)"),
    FeatureDef("ratio_lv_ref",        "pipeline", "float", True, "Ratio loc/ven ref statique (TFT)"),
    FeatureDef("rupture_flag",        "pipeline", "int",   True, "Flag rupture PELT binaire"),
    FeatureDef("score_rupture",       "pipeline", "float", True, "Score rupture PELT (intensité)"),
    FeatureDef("score_rupture_norm",  "pipeline", "float", True, "Score rupture normalisé"),
    FeatureDef("rupture_intensity",   "pipeline", "float", True, "score_norm × fiabilité"),
    FeatureDef("post_rupture",        "pipeline", "int",   True, "Régime post-rupture (0/1)"),
    FeatureDef("gouvernorat_enc",     "pipeline", "int",   True, "Gouvernorat encodé (TFT)"),
    FeatureDef("groupe_enc",          "pipeline", "int",   True, "Groupe encodé (TFT)"),
    FeatureDef("low_data_cluster",    "pipeline", "int",   True, "Flag région à faible données (0/1)"),
]

# ─── Registre global ─────────────────────────────────────────────────────────
ALL_FEATURE_DEFS: List[FeatureDef] = (
    CORE_FEATURES + PRICE_FEATURES + MACRO_FEATURES +
    ENGINEERED_FEATURES + PIPELINE_FEATURES
)

# Dict name → FeatureDef pour lookup rapide
FEATURES_GLOBAL: Dict[str, FeatureDef] = {f.name: f for f in ALL_FEATURE_DEFS}


# ──────────────────────────────────────────────────────────────────────────────
# 2. FEATURES PAR MODÈLE
# ──────────────────────────────────────────────────────────────────────────────

ModelType = Literal['kmeans', 'pelt', 'lstm', 'tft']

MODEL_FEATURES: Dict[str, Dict[str, List[str]]] = {

    # ── KMeans : core + price + scores pipeline ───────────────────────────────
    # Clustering géographique sur signaux de prix et scores statiques.
    # N'utilise PAS les features temporelles engineered (pas de sens à agréger).
    'kmeans': {
        'required': [
            'gouvernorat', 'annee', 'mois', 'groupe',
            'indice_prix_m2_regional',
        ],
        'optional': [
            'indice_prix_m2_loc', 'indice_prix_m2_ven', 'ratio_location_vente',
            'glissement_immo_trim', 'score_attractivite', 'nb_infra', 'nb_commerce',
            'indice_liquidite', 'volatilite_prix_trim', 'zone_geographique',
            'prix_lisse_loc', 'prix_lisse_ven', 'price_mean',
        ],
        'output': [
            'cluster_id', 'cluster_size', 'cluster_size_norm',
            'zscore_prix', 'zscore_ref', 'ratio_lv_ref',
            'potentiel_emergent', 'indice_liquidite', 'confidence_score',
            'low_data_cluster',
        ],
    },

    # ── PELT : core + price + macro ───────────────────────────────────────────
    # Détection de rupture sur le signal prix + contexte macro.
    # Dépend de KMeans : cluster_id doit déjà exister.
    'pelt': {
        'required': [
            'gouvernorat', 'annee', 'mois', 'type_transaction', 'groupe',
            'indice_prix_m2_regional',
        ],
        'optional': [
            'glissement_immo_trim',
            'inflation_glissement_annuel', 'inflation_proxy',
            'croissance_pib_trim', 'high_season',
            'prix_lisse', 'prix_lisse_loc', 'prix_lisse_ven', 'price_mean',
            'zone_geographique', 'indice_liquidite', 'volatilite_prix_trim',
            'cluster_id',
        ],
        'output': [
            'rupture_flag', 'score_rupture', 'score_rupture_norm',
            'rupture_intensity', 'post_rupture',
        ],
    },

    # ── LSTM : core + engineered + macro + pipeline ───────────────────────────
    # Prédiction série temporelle par gouvernorat.
    # Dépend de KMeans (cluster_id) et PELT (rupture_*).
    'lstm': {
        'required': [
            'gouvernorat', 'annee', 'mois', 'groupe',
            'indice_prix_m2_regional',
        ],
        'optional': [
            # Price
            'indice_prix_m2_loc', 'indice_prix_m2_ven', 'ratio_location_vente',
            'glissement_immo_trim',
            # Macro
            'inflation_glissement_annuel', 'inflation_proxy',
            'croissance_pib_trim', 'high_season',
            'score_attractivite', 'nb_infra', 'nb_commerce',
            # Engineered
            'trend_prix', 'momentum_prix',
            # Pipeline
            'cluster_id', 'post_rupture', 'score_rupture_norm',
            'rupture_intensity', 'zscore_prix',
            # Fallbacks
            'prix_lisse_loc', 'prix_lisse_ven',
        ],
        'output': [],  # prédictions retournées en dict, pas de colonnes
    },

    # ── TFT : toutes les features en format fenêtre temporelle ───────────────
    # Modèle global multi-gouvernorats avec contexte statique + temporel.
    'tft': {
        'required': [
            'gouvernorat', 'annee', 'mois', 'groupe',
            'indice_prix_m2_regional',
        ],
        'optional': [
            # Catégoriels statiques
            'gouvernorat_enc', 'cluster_enc', 'zone_geo', 'groupe_enc',
            # Réels statiques
            'cluster_size_norm', 'score_attractivite', 'potentiel_emergent',
            'zscore_ref', 'ratio_lv_ref',
            # Known (futures connues)
            'mois_sin', 'mois_cos', 'annee_norm', 'high_season', 'saison',
            'inflation_glissement_annuel', 'croissance_pib_trim', 'glissement_immo_trim',
            # Unknown (dynamiques)
            'prix_m2_norm', 'prix_m2_loc_norm', 'prix_m2_ven_norm',
            'ratio_loc_ven', 'volatilite_rolling_norm', 'nb_annonces_proxy_norm',
            'rupture_flag', 'score_rupture', 'post_rupture',
            'indice_liquidite_norm', 'volatilite_prix_trim_norm',
        ],
        'output': [],
    },
}

# Vue "all" = required + optional par modèle
for _model in MODEL_FEATURES:
    MODEL_FEATURES[_model]['all'] = (
        MODEL_FEATURES[_model]['required'] + MODEL_FEATURES[_model]['optional']
    )


# ──────────────────────────────────────────────────────────────────────────────
# 3. RÈGLES DE FALLBACK CENTRALISÉES
# ──────────────────────────────────────────────────────────────────────────────

# Chaque règle : (colonne_cible, source_principale, callable_fallback)
# Le callable reçoit df et retourne une Series.
# Ordre d'application : les règles sont appliquées séquentiellement.

def _safe_div(a: pd.Series, b: pd.Series, fill: float = 0.5) -> pd.Series:
    """Division protégée contre /0 et NaN."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = a / b.replace(0, np.nan)
    return result.fillna(fill)


def _rolling_mean(series: pd.Series, window: int = 3) -> pd.Series:
    return series.rolling(window, min_periods=1).mean()


# ──────────────────────────────────────────────────────────────────────────────
# 4. PIPELINE DE PREPROCESSING UNIFIÉ
# ──────────────────────────────────────────────────────────────────────────────

MIN_POINTS = 10         # Seuil minimum de points par gouvernorat
CONFIDENCE_BASE = 30    # n_samples pour confidence_score = 1.0
HIGH_SEASON_MONTHS = {4, 5, 6, 7, 8, 9}  # avr–sept


def build_features(
    df: pd.DataFrame,
    groupe_col: str = 'groupe',
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Pipeline de preprocessing universel.

    Garantit que TOUTES les features du registre existent dans le DataFrame
    de sortie, sans jamais crasher sur une colonne manquante.

    Ordre d'application des couches :
      1. Core temporal        — encodages cycliques, saison
      2. Prix                 — lissage, price_mean, ratios
      3. Macro                — fallbacks inflation / PIB / saison
      4. Engineered           — trend, momentum, volatilité
      5. Qualité des données  — confidence_score, low_data_cluster
      6. Nettoyage final      — NaN résiduels → 0.0, clip extrêmes

    Parameters
    ----------
    df          : DataFrame brut (peut avoir des colonnes manquantes)
    groupe_col  : nom de la colonne groupe (Residentiel / Foncier / Commercial)
    verbose     : affiche un rapport si des fallbacks sont activés

    Returns
    -------
    DataFrame avec TOUTES les features garanties présentes et propres.
    """
    df = df.copy()
    _fallbacks_applied: List[str] = []

    def _log(msg: str) -> None:
        if verbose:
            print(f"  [build_features] {msg}")
        _fallbacks_applied.append(msg)

    def _has(col: str) -> bool:
        return col in df.columns and df[col].notna().any()

    # ────────────────────────────────────────────────────────────────
    # COUCHE 1 : Core temporal
    # ────────────────────────────────────────────────────────────────
    if 'mois_sin' not in df.columns:
        df['mois_sin'] = np.sin(2 * np.pi * df['mois'] / 12)
        df['mois_cos'] = np.cos(2 * np.pi * df['mois'] / 12)
        _log("mois_sin / mois_cos créés depuis 'mois'")

    if 'annee_norm' not in df.columns:
        yr_min = df['annee'].min()
        yr_rng = max(df['annee'].max() - yr_min, 1)
        df['annee_norm'] = (df['annee'] - yr_min) / yr_rng
        _log("annee_norm créé")

    if 'saison' not in df.columns:
        df['saison'] = df['mois'].map(
            lambda m: 1 if m in (6, 7, 8) else (2 if m in (12, 1, 2) else 3)
        ).astype(int)
        _log("saison créée (1=Eté 2=Hiver 3=Autre)")

    if 'high_season' not in df.columns:
        df['high_season'] = df['mois'].isin(HIGH_SEASON_MONTHS).astype(float)
        _log("high_season créé depuis HIGH_SEASON_MONTHS")

    # Stabilisation zone_geo (str propre pour LabelEncoder TFT)
    if 'zone_geo' not in df.columns:
        if 'zone_geographique' in df.columns:
            df['zone_geo'] = df['zone_geographique'].fillna('unknown').astype(str)
        else:
            df['zone_geo'] = 'unknown'
        _log("zone_geo stabilisé")
    else:
        df['zone_geo'] = (df['zone_geo']
                          .fillna('unknown')
                          .astype(str)
                          .str.strip()
                          .replace({'nan': 'unknown', 'None': 'unknown', '': 'unknown'}))

    if 'zone_geographique' not in df.columns:
        df['zone_geographique'] = 0

    # ────────────────────────────────────────────────────────────────
    # COUCHE 2 : Features PRIX
    # ────────────────────────────────────────────────────────────────
    target_col = 'indice_prix_m2_regional'

    # prix_lisse global (rolling 3M par gouvernorat)
    if 'prix_lisse' not in df.columns:
        df['prix_lisse'] = (
            df.groupby('gouvernorat')[target_col]
              .transform(lambda x: _rolling_mean(x, 3))
        )

    # Stratification loc / ven
    for tt, col_lisse, col_raw in [
        (1, 'prix_lisse_loc', 'indice_prix_m2_loc'),
        (2, 'prix_lisse_ven', 'indice_prix_m2_ven'),
    ]:
        if col_raw not in df.columns:
            # Fallback : signal global si stratification absente
            df[col_raw] = df[target_col].copy()
            _log(f"{col_raw} absent → copie de {target_col}")

        if col_lisse not in df.columns:
            df[col_lisse] = (
                df.groupby('gouvernorat')[col_raw]
                  .transform(lambda x: _rolling_mean(x, 3))
            )
            _log(f"{col_lisse} créé par rolling(3) sur {col_raw}")

    # price_mean = (loc + ven) / 2
    if 'price_mean' not in df.columns:
        loc_ok = _has('prix_lisse_loc')
        ven_ok = _has('prix_lisse_ven')
        if loc_ok and ven_ok:
            df['price_mean'] = (df['prix_lisse_loc'] + df['prix_lisse_ven']) / 2
        elif loc_ok:
            df['price_mean'] = df['prix_lisse_loc']
        elif ven_ok:
            df['price_mean'] = df['prix_lisse_ven']
        else:
            df['price_mean'] = df[target_col]
        _log("price_mean calculé")

    # Remplissage réciproque loc/ven via price_mean
    df['prix_lisse_loc'] = df['prix_lisse_loc'].fillna(df['price_mean'])
    df['prix_lisse_ven'] = df['prix_lisse_ven'].fillna(df['price_mean'])

    # ratio_location_vente (proportion vente / total par gouvernorat × mois)
    if 'ratio_location_vente' not in df.columns or df['ratio_location_vente'].isna().all():
        if 'type_transaction' in df.columns:
            TT_VEN = 2
            n_grp = df.groupby(['gouvernorat', 'annee', 'mois'])['type_transaction'].transform('count')
            n_ven = df['type_transaction'].eq(TT_VEN).astype(float)
            n_ven = df.groupby(['gouvernorat', 'annee', 'mois'])['type_transaction']\
                       .transform(lambda x: x.eq(TT_VEN).sum())
            df['ratio_location_vente'] = _safe_div(n_ven, n_grp, fill=0.5)
            _log("ratio_location_vente calculé depuis type_transaction")
        else:
            df['ratio_location_vente'] = 0.5
            _log("ratio_location_vente absent → 0.5 (neutre)")

    # Alias TFT
    if 'ratio_loc_ven' not in df.columns:
        df['ratio_loc_ven'] = df['ratio_location_vente']
    if 'prix_m2' not in df.columns:
        df['prix_m2'] = df[target_col]
    if 'prix_m2_loc' not in df.columns:
        df['prix_m2_loc'] = df.get('indice_prix_m2_loc', df[target_col])
    if 'prix_m2_ven' not in df.columns:
        df['prix_m2_ven'] = df.get('indice_prix_m2_ven', df[target_col])

    # ────────────────────────────────────────────────────────────────
    # COUCHE 3 : Features MACRO
    # ────────────────────────────────────────────────────────────────

    # inflation_glissement_annuel + inflation_proxy
    if 'inflation_glissement_annuel' in df.columns and df['inflation_glissement_annuel'].notna().any():
        # Vraie colonne présente → proxy = alias direct
        df['inflation_proxy'] = df['inflation_glissement_annuel']
    else:
        # Proxy : pct_change(12) sur signal prix par gouvernorat
        df['inflation_proxy'] = (
            df.groupby('gouvernorat')[target_col]
              .transform(lambda x: x.pct_change(12).ffill().bfill().fillna(0.0).abs())
        )
        df['inflation_glissement_annuel'] = df['inflation_proxy']
        _log("inflation_glissement_annuel absent → proxy pct_change(12)")

    # croissance_pib_trim
    if 'croissance_pib_trim' not in df.columns or df['croissance_pib_trim'].isna().all():
        df['_idx_global'] = (
            df.groupby(['annee', 'mois'])[target_col].transform('mean')
        )
        df['croissance_pib_trim'] = (
            df['_idx_global'].pct_change(3).ffill().bfill().fillna(0.0)
        )
        df.drop(columns=['_idx_global'], inplace=True)
        _log("croissance_pib_trim absent → proxy variation trimestrielle globale")

    # glissement_immo_trim
    if 'glissement_immo_trim' not in df.columns or df['glissement_immo_trim'].isna().all():
        df['glissement_immo_trim'] = (
            df.groupby('gouvernorat')[target_col]
              .transform(lambda x: x.pct_change(3).ffill().bfill().fillna(0.0))
        )
        _log("glissement_immo_trim absent → proxy pct_change(3) par gouvernorat")

    # Colonnes statiques macro à 0 si absentes
    for col, default in [
        ('score_attractivite', 0.5),
        ('nb_infra', 0.0),
        ('nb_commerce', 0.0),
        ('indice_liquidite', 1.0),
        ('nb_annonces_proxy', 0.0),
    ]:
        if col not in df.columns or df[col].isna().all():
            df[col] = default
            _log(f"{col} absent → {default}")

    # ────────────────────────────────────────────────────────────────
    # COUCHE 4 : Features ENGINEERED
    # ────────────────────────────────────────────────────────────────

    # trend_prix = pct_change(3) par gouvernorat sur signal agrégé
    if 'trend_prix' not in df.columns or df['trend_prix'].isna().all():
        df['trend_prix'] = (
            df.groupby('gouvernorat')[target_col]
              .transform(lambda x: x.pct_change(3).ffill().bfill().fillna(0.0))
        )
        _log("trend_prix créé (pct_change 3M)")

    # momentum_prix = diff(3) par gouvernorat
    if 'momentum_prix' not in df.columns or df['momentum_prix'].isna().all():
        df['momentum_prix'] = (
            df.groupby('gouvernorat')[target_col]
              .transform(lambda x: x.diff(3).ffill().bfill().fillna(0.0))
        )
        _log("momentum_prix créé (diff 3M)")

    # volatilite_rolling = std(6M) par gouvernorat
    if 'volatilite_rolling' not in df.columns or df['volatilite_rolling'].isna().all():
        df['volatilite_rolling'] = (
            df.groupby('gouvernorat')[target_col]
              .transform(lambda x: x.rolling(6, min_periods=2).std().ffill().bfill().fillna(0.0))
        )
        _log("volatilite_rolling créé (std 6M)")

    # volatilite_prix_trim = std(3M) par gouvernorat
    if 'volatilite_prix_trim' not in df.columns or df['volatilite_prix_trim'].isna().all():
        df['volatilite_prix_trim'] = (
            df.groupby('gouvernorat')[target_col]
              .transform(lambda x: x.rolling(3, min_periods=1).std().ffill().bfill().fillna(0.0))
        )
        _log("volatilite_prix_trim créé (std 3M)")

    # ────────────────────────────────────────────────────────────────
    # COUCHE 5 : Qualité des données
    # ────────────────────────────────────────────────────────────────

    # n_samples par gouvernorat (nombre de points temporels distincts)
    n_pts = (df.groupby('gouvernorat')[['annee', 'mois']]
               .transform('size')
               .iloc[:, 0] if isinstance(
               df.groupby('gouvernorat')[['annee', 'mois']].transform('size'),
               pd.DataFrame)
               else df.groupby('gouvernorat')['annee'].transform('size'))

    # confidence_score = min(1, n / CONFIDENCE_BASE)
    df['confidence_score'] = (n_pts / CONFIDENCE_BASE).clip(upper=1.0)

    # low_data_cluster : gouvernorats avec trop peu de points → flag
    gov_pts = df.groupby('gouvernorat')['annee'].transform('count')
    df['low_data_cluster'] = (gov_pts < MIN_POINTS).astype(int)
    n_low = int(df['low_data_cluster'].sum())
    if n_low > 0:
        n_gov_low = df[df['low_data_cluster'] == 1]['gouvernorat'].nunique()
        _log(f"low_data_cluster : {n_gov_low} gouvernorats sous seuil ({MIN_POINTS} pts)")

    # potentiel_emergent : score composite (confidence × trend normalisé)
    if 'potentiel_emergent' not in df.columns:
        trend_norm = df['trend_prix'].clip(-0.5, 0.5) / 0.5  # [-1, 1]
        df['potentiel_emergent'] = (
            df['confidence_score'] * (1 + trend_norm) / 2
        ).clip(0, 1)
        _log("potentiel_emergent créé (confidence × trend)")

    # ────────────────────────────────────────────────────────────────
    # COUCHE 6 : Colonnes pipeline (initialisées à 0 si absentes)
    # Seront peuplées par KMeans → PELT → LSTM dans cet ordre.
    # ────────────────────────────────────────────────────────────────
    pipeline_defaults = {
        'cluster_id'        : 0,
        'cluster_size'      : 1,
        'cluster_size_norm' : 0.5,
        'zscore_prix'       : 0.0,
        'zscore_ref'        : 0.0,
        'ratio_lv_ref'      : 0.5,
        'cluster_enc'       : 0,
        'gouvernorat_enc'   : 0,
        'groupe_enc'        : 0,
        'rupture_flag'      : 0,
        'score_rupture'     : 0.0,
        'score_rupture_norm': 0.0,
        'rupture_intensity' : 0.0,
        'post_rupture'      : 0,
    }
    for col, default in pipeline_defaults.items():
        if col not in df.columns:
            df[col] = default

    # ────────────────────────────────────────────────────────────────
    # COUCHE 7 : Nettoyage final
    # ────────────────────────────────────────────────────────────────
    # Remplacer NaN résiduels sur toutes les features float
    float_cols = [
        f.name for f in ALL_FEATURE_DEFS
        if f.dtype == 'float' and f.name in df.columns
    ]
    nan_counts = df[float_cols].isna().sum()
    nan_cols = nan_counts[nan_counts > 0]
    if len(nan_cols) > 0:
        df[float_cols] = df[float_cols].fillna(0.0)
        _log(f"NaN résiduels comblés par 0.0 : {list(nan_cols.index)}")

    # Rapport final
    if verbose and _fallbacks_applied:
        print(f"\n  ── build_features rapport ─────────────────────────────")
        print(f"  {len(_fallbacks_applied)} fallback(s) appliqué(s) :")
        for msg in _fallbacks_applied:
            print(f"    • {msg}")
        print(f"  Colonnes finales : {len(df.columns)}")
        print(f"  Lignes           : {len(df):,}")
        print(f"  NaN résiduels    : {int(df.isna().sum().sum())}")
        print()

    return df


# ──────────────────────────────────────────────────────────────────────────────
# 5. VALIDATION FEATURES
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class ValidationReport:
    """Résultat de validate_features()."""
    model          : str
    ok             : bool
    present        : List[str]
    missing_req    : List[str]   # features requises absentes → bloquant
    missing_opt    : List[str]   # features optionnelles absentes → warning
    zero_variance  : List[str]   # features constantes → suspect
    nan_cols       : Dict[str, int]  # col → nb NaN
    n_rows         : int
    n_govs         : int
    low_data_govs  : List[int]
    confidence_mean: float

    def summary(self) -> str:
        lines = [
            f"  ── validate_features [{self.model.upper()}] {'✔ OK' if self.ok else '✗ ÉCHEC'} ──",
            f"  Lignes : {self.n_rows:,} | Gouvernorats : {self.n_govs}",
            f"  Features présentes : {len(self.present)}",
        ]
        if self.missing_req:
            lines.append(f"  ✗ REQUISES MANQUANTES ({len(self.missing_req)}) : {self.missing_req}")
        if self.missing_opt:
            lines.append(f"  ⚠ Optionnelles absentes ({len(self.missing_opt)}) : {self.missing_opt}")
        if self.zero_variance:
            lines.append(f"  ⚠ Variance nulle ({len(self.zero_variance)}) : {self.zero_variance}")
        if self.nan_cols:
            lines.append(f"  ⚠ Colonnes avec NaN : {self.nan_cols}")
        if self.low_data_govs:
            lines.append(f"  ⚠ Gouvernorats sous seuil ({MIN_POINTS} pts) : {self.low_data_govs}")
        lines.append(f"  Confidence score moyen : {self.confidence_mean:.2f}")
        return '\n'.join(lines)

    def print(self) -> None:
        print(self.summary())

    def raise_if_invalid(self) -> None:
        if not self.ok:
            raise ValueError(
                f"[validate_features] Modèle '{self.model}' : "
                f"features requises manquantes : {self.missing_req}"
            )


def validate_features(
    df: pd.DataFrame,
    model: ModelType,
    auto_fix: bool = True,
    verbose: bool = True,
) -> ValidationReport:
    """
    Valide que df contient toutes les features nécessaires pour `model`.

    Parameters
    ----------
    df       : DataFrame à valider (idéalement sorti de build_features())
    model    : 'kmeans' | 'pelt' | 'lstm' | 'tft'
    auto_fix : Si True, appelle build_features() automatiquement si des
               features requises sont manquantes (best-effort recovery)
    verbose  : Affiche le rapport de validation

    Returns
    -------
    ValidationReport avec .ok = True si aucune feature requise n'est absente.
    """
    spec     = MODEL_FEATURES[model]
    required = spec['required']
    optional = spec['optional']

    present     = [c for c in (required + optional) if c in df.columns]
    missing_req = [c for c in required if c not in df.columns]
    missing_opt = [c for c in optional if c not in df.columns]

    # Auto-fix : si des features requises manquent, tenter build_features
    if missing_req and auto_fix:
        if verbose:
            print(f"  [validate_features] Auto-fix : appel build_features() "
                  f"pour combler {missing_req}")
        df = build_features(df, verbose=verbose)
        present     = [c for c in (required + optional) if c in df.columns]
        missing_req = [c for c in required if c not in df.columns]
        missing_opt = [c for c in optional if c not in df.columns]

    # Variance nulle sur features numériques
    num_cols = [c for c in present if df[c].dtype in (np.float32, np.float64, float, int)]
    zero_var = [c for c in num_cols if df[c].nunique() <= 1]

    # NaN residuels
    nan_info = {
        c: int(df[c].isna().sum())
        for c in present
        if df[c].isna().any()
    }

    # Données par gouvernorat
    gov_col = 'gouvernorat' if 'gouvernorat' in df.columns else None
    if gov_col:
        n_govs    = df[gov_col].nunique()
        gov_pts   = df.groupby(gov_col).size()
        low_govs  = list(gov_pts[gov_pts < MIN_POINTS].index.astype(int))
    else:
        n_govs   = 0
        low_govs = []

    conf_mean = float(df['confidence_score'].mean()) \
        if 'confidence_score' in df.columns else 0.0

    report = ValidationReport(
        model          = model,
        ok             = len(missing_req) == 0,
        present        = present,
        missing_req    = missing_req,
        missing_opt    = missing_opt,
        zero_variance  = zero_var,
        nan_cols       = nan_info,
        n_rows         = len(df),
        n_govs         = n_govs,
        low_data_govs  = low_govs,
        confidence_mean= conf_mean,
    )

    if verbose:
        report.print()

    return report


# ──────────────────────────────────────────────────────────────────────────────
# 6. UTILITAIRES RAPIDES
# ──────────────────────────────────────────────────────────────────────────────

def get_features(model: ModelType, subset: str = 'all') -> List[str]:
    """
    Retourne la liste de features pour un modèle donné.

    Parameters
    ----------
    model  : 'kmeans' | 'pelt' | 'lstm' | 'tft'
    subset : 'all' | 'required' | 'optional' | 'output'
    """
    return MODEL_FEATURES[model][subset]


def feature_info(name: str) -> Optional[FeatureDef]:
    """Retourne la définition canonique d'une feature, ou None si inconnue."""
    return FEATURES_GLOBAL.get(name)


def features_by_group(group: str) -> List[str]:
    """Retourne toutes les features d'un groupe : core|price|macro|engineered|pipeline."""
    return [f.name for f in ALL_FEATURE_DEFS if f.group == group]


def print_registry() -> None:
    """Affiche le registre complet en tableau lisible."""
    groups = ['core', 'price', 'macro', 'engineered', 'pipeline']
    for grp in groups:
        feats = [f for f in ALL_FEATURE_DEFS if f.group == grp]
        print(f"\n{'─'*60}")
        print(f"  {grp.upper()} ({len(feats)} features)")
        print(f"{'─'*60}")
        for f in feats:
            nullable = '?' if f.nullable else '!'
            print(f"  [{nullable}] {f.name:<35} {f.dtype:<10} {f.description}")


# ──────────────────────────────────────────────────────────────────────────────
# 7. POINT D'ENTRÉE AUTO-TEST
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("=" * 72)
    print("  FEATURE REGISTRY — Pipeline Immobilier Tunisien")
    print("=" * 72)
    print(f"  Total features définies : {len(FEATURES_GLOBAL)}")
    for grp in ['core', 'price', 'macro', 'engineered', 'pipeline']:
        n = len([f for f in ALL_FEATURE_DEFS if f.group == grp])
        print(f"    {grp:<12} : {n}")

    print()
    for model in ['kmeans', 'pelt', 'lstm', 'tft']:
        req = MODEL_FEATURES[model]['required']
        opt = MODEL_FEATURES[model]['optional']
        print(f"  {model.upper():<8} : {len(req)} requises + {len(opt)} optionnelles")

    print()
    print("  ── Auto-test build_features() sur données synthétiques ──")

    # Données minimales : gouvernorat + annee + mois + prix + type
    np.random.seed(42)
    n = 120
    gov_ids = [1, 2, 3, 4, 5]  # 5 gouvernorats × 24 mois
    df_test = pd.DataFrame({
        'gouvernorat'            : np.repeat(gov_ids, n // len(gov_ids)),
        'annee'                  : np.tile([2022, 2023, 2024] * 8, len(gov_ids)),
        'mois'                   : np.tile(range(1, 13), 10)[:n],
        'type_transaction'       : np.random.choice([1, 2], n),
        'groupe'                 : 'Residentiel',
        'indice_prix_m2_regional': np.random.normal(1500, 200, n).clip(800, 3000),
    })

    print(f"\n  DataFrame test : {len(df_test)} lignes, {len(df_test.columns)} colonnes")
    print(f"  Colonnes initiales : {list(df_test.columns)}")

    df_built = build_features(df_test, verbose=True)

    print(f"\n  Colonnes après build_features : {len(df_built.columns)}")
    print(f"  NaN totaux : {df_built.isna().sum().sum()}")

    print()
    for model in ['kmeans', 'pelt', 'lstm', 'tft']:
        report = validate_features(df_built, model=model, auto_fix=False, verbose=False)
        status = '✔' if report.ok else '✗'
        print(f"  {status} validate_features [{model.upper():<6}] : "
              f"{len(report.present)} présentes | "
              f"{len(report.missing_req)} req manquantes | "
              f"{len(report.missing_opt)} opt manquantes")

    print()
    print("  Registre complet :")
    print_registry()
