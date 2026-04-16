"""
features_config.py — Feature Registry L102 pour cohérence pipeline.

Centralise les features utilisées par chaque module (KMeans, PELT, LSTM)
pour éviter le feature drift et garantir la reproductibilité.

L102 = True si registry chargé et validé.
"""

import os
import pandas as pd

# Flag L102 : registry chargé et validé
L102 = True

# Features par module et groupe
FEATURE_SETS = {
    "kmeans": {
        "Residentiel": [
            'prix_mean', 'prix_cv', 'prix_iqr', 'prix_tendance', 'prix_recent',
            'zscore_prix', 'rang_prix',
            'prix_mean_loc', 'prix_mean_ven',
            'prix_tendance_loc', 'prix_tendance_ven',
            'prix_cv_loc', 'prix_cv_ven',
            'ratio_location_vente', 'spread_type',
            'saisonnalite', 'amplitude_saisonniere', 'volatilite_saisonniere', 'hs_ratio',
            'score_att', 'nb_infra', 'nb_commerce', 'potentiel_score', 'score_composite',
            'inflation', 'pib', 'glissement', 'glissement_vol', 'spread_reel', 'yield_implicite',
            'croissance_acceleration', 'prix_recent_vs_global', 'momentum',
            'prix_par_attractivite', 'prix_par_infra',
            'weight_geo', 'weight_temp', 'poids_total', 'arima_elig',
            'type_vente_ratio', 'n_annees',
        ],
        "Foncier": [
            'prix_mean', 'prix_cv', 'prix_iqr', 'prix_tendance', 'prix_recent',
            'zscore_prix', 'rang_prix',
            'prix_mean_loc', 'prix_mean_ven',
            'prix_tendance_loc', 'prix_tendance_ven',
            'prix_cv_loc', 'prix_cv_ven',
            'ratio_location_vente', 'spread_type',
            'saisonnalite', 'amplitude_saisonniere', 'volatilite_saisonniere', 'hs_ratio',
            'score_att', 'nb_infra', 'nb_commerce', 'potentiel_score', 'score_composite',
            'inflation', 'pib', 'glissement', 'glissement_vol', 'spread_reel', 'yield_implicite',
            'croissance_acceleration', 'prix_recent_vs_global', 'momentum',
            'prix_par_attractivite', 'prix_par_infra',
            'weight_geo', 'weight_temp', 'poids_total', 'arima_elig',
            'type_vente_ratio', 'n_annees',
        ],
        "Commercial": [
            'prix_mean', 'prix_cv', 'prix_iqr', 'prix_tendance', 'prix_recent',
            'zscore_prix', 'rang_prix',
            'prix_mean_loc', 'prix_mean_ven',
            'prix_tendance_loc', 'prix_tendance_ven',
            'prix_cv_loc', 'prix_cv_ven',
            'ratio_location_vente', 'spread_type',
            'saisonnalite', 'amplitude_saisonniere', 'volatilite_saisonniere', 'hs_ratio',
            'score_att', 'nb_infra', 'nb_commerce', 'potentiel_score', 'score_composite',
            'inflation', 'pib', 'glissement', 'glissement_vol', 'spread_reel', 'yield_implicite',
            'croissance_acceleration', 'prix_recent_vs_global', 'momentum',
            'prix_par_attractivite', 'prix_par_infra',
            'weight_geo', 'weight_temp', 'poids_total', 'arima_elig',
            'type_vente_ratio', 'n_annees',
        ],
    },
    "pelt": {
        "Residentiel": [
            'gouvernorat', 'annee', 'mois', 'prix_lisse_loc', 'prix_lisse_ven',
            'indice_prix_m2_regional', 'zone_geographique', 'volatilite_prix_trim',
            'indice_liquidite', 'potentiel_emergent', 'high_season',
            'inflation_glissement_annuel', 'croissance_pib_trim',
        ],
        "Foncier": [
            'gouvernorat', 'annee', 'mois', 'prix_lisse_loc', 'prix_lisse_ven',
            'indice_prix_m2_regional', 'zone_geographique', 'volatilite_prix_trim',
            'indice_liquidite', 'potentiel_emergent', 'high_season',
            'inflation_glissement_annuel', 'croissance_pib_trim',
        ],
        "Commercial": [
            'gouvernorat', 'annee', 'mois', 'prix_lisse_loc', 'prix_lisse_ven',
            'indice_prix_m2_regional', 'zone_geographique', 'volatilite_prix_trim',
            'indice_liquidite', 'potentiel_emergent', 'high_season',
            'inflation_glissement_annuel', 'croissance_pib_trim',
        ],
    },
    "lstm": {
        "Residentiel": [
            'gouvernorat', 'annee', 'mois', 'high_season', 'inflation_glissement_annuel',
            'croissance_pib_trim', 'indice_prix_m2_regional', 'zone_geographique',
            'indice_liquidite', 'volatilite_prix_trim', 'potentiel_emergent',
            'prix_lisse_loc', 'prix_lisse_ven', 'ratio_location_vente',
            'score_attractivite', 'nb_infra', 'nb_commerce',
        ],
        "Foncier": [
            'gouvernorat', 'annee', 'mois', 'high_season', 'inflation_glissement_annuel',
            'croissance_pib_trim', 'indice_prix_m2_regional', 'zone_geographique',
            'indice_liquidite', 'volatilite_prix_trim', 'potentiel_emergent',
            'prix_lisse_loc', 'prix_lisse_ven', 'ratio_location_vente',
            'score_attractivite', 'nb_infra', 'nb_commerce',
        ],
        "Commercial": [
            'gouvernorat', 'annee', 'mois', 'high_season', 'inflation_glissement_annuel',
            'croissance_pib_trim', 'indice_prix_m2_regional', 'zone_geographique',
            'indice_liquidite', 'volatilite_prix_trim', 'potentiel_emergent',
            'prix_lisse_loc', 'prix_lisse_ven', 'ratio_location_vente',
            'score_attractivite', 'nb_infra', 'nb_commerce',
        ],
    },
    "tft": {
        "Residentiel": [
            'gouvernorat', 'zone_geographique', 'groupe',
            'annee', 'mois', 'high_season',
            'score_attractivite', 'potentiel_emergent',
            'inflation_glissement_annuel', 'croissance_pib_trim',
            'indice_prix_m2_regional', 'indice_loc_lisse', 'indice_ven_lisse',
            'indice_liquidite', 'volatilite_prix_trim',
            'glissement_immo_trim', 'nb_infra', 'nb_commerce',
        ],
        "Foncier": [
            'gouvernorat', 'zone_geographique', 'groupe',
            'annee', 'mois', 'high_season',
            'score_attractivite', 'potentiel_emergent',
            'inflation_glissement_annuel', 'croissance_pib_trim',
            'indice_prix_m2_regional', 'indice_loc_lisse', 'indice_ven_lisse',
            'indice_liquidite', 'volatilite_prix_trim',
            'glissement_immo_trim', 'nb_infra', 'nb_commerce',
        ],
        "Commercial": [
            'gouvernorat', 'zone_geographique', 'groupe',
            'annee', 'mois', 'high_season',
            'score_attractivite', 'potentiel_emergent',
            'inflation_glissement_annuel', 'croissance_pib_trim',
            'indice_prix_m2_regional', 'indice_loc_lisse', 'indice_ven_lisse',
            'indice_liquidite', 'volatilite_prix_trim',
            'glissement_immo_trim', 'nb_infra', 'nb_commerce',
        ],
    },
}


def get_features(module: str, groupe: str = None) -> list:
    """
    Retourne les features pour un module et groupe donné.
    Si groupe=None, retourne toutes les features du module.
    """
    if module not in FEATURE_SETS:
        raise ValueError(f"Module '{module}' inconnu. Modules disponibles: {list(FEATURE_SETS.keys())}")

    if groupe is None:
        # Retourne l'union de toutes les features du module
        all_features = set()
        for grp_features in FEATURE_SETS[module].values():
            all_features.update(grp_features)
        return sorted(list(all_features))

    if groupe not in FEATURE_SETS[module]:
        raise ValueError(f"Groupe '{groupe}' inconnu pour module '{module}'. Groupes disponibles: {list(FEATURE_SETS[module].keys())}")

    return FEATURE_SETS[module][groupe]


def validate_features(df: pd.DataFrame, module: str, groupe: str = None) -> tuple:
    """
    Valide les features disponibles dans df pour un module/groupe.
    Retourne (presentes, manquantes).
    """
    required = get_features(module, groupe)
    available = set(df.columns)
    present = [f for f in required if f in available]
    missing = [f for f in required if f not in available]
    return present, missing


def is_registry_valid() -> bool:
    """Vérifie si le registry est valide (toutes features définies)."""
    for module, groups in FEATURE_SETS.items():
        for groupe, features in groups.items():
            if not features:
                return False
    return True


# Validation au chargement
if not is_registry_valid():
    L102 = False
    raise Exception("Feature Registry L102 invalide — features manquantes")