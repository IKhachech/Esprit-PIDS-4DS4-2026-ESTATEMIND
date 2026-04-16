"""
score_engine.py — Market Potential Score Engine · Version Production BO3
=========================================================================

Couche finale du pipeline KMeans → PELT → LSTM → TFT → SCORE.

OBJECTIF :
  Transformer les sorties de tous les modèles en UN score de potentiel
  immobilier par gouvernorat (0 → 1), interprétable et justifiable.

FORMULE GÉNÉRALE :
  Score(gov) = w1·S_trend + w2·S_trend_hist + w3·S_rupture + w4·S_liquidite
             + w5·S_cluster + w6·S_lstm + w7·S_certitude

  Chaque sous-score est normalisé [0,1] et interprétable séparément.

SOUS-SCORES :
  S_trend      — Tendance TFT P50 T+12 vs prix actuel (croissance attendue)
  S_trend_hist — [NOUVEAU] Tendance historique réelle : normalize(trend_prix)
                 depuis cluster_map (prix_tendance) + tft_df (trend_prix).
                 Actif même sans prédictions TFT disponibles.
                 Formule : 0.60 × rank(prix_tendance_cluster)
                          + 0.40 × rank(trend_prix_tft)
  S_rupture    — Signaux PELT : rupture récente + intensité + régime positif
  S_liquidite  — Indice de liquidité du marché (annonces / mois couverts)
  S_cluster    — Profil du cluster KMeans (attractivité, infra, potentiel)
  S_lstm       — Cohérence LSTM vs TFT (convergence des modèles)
  S_certitude  — Inverse de l'incertitude TFT (P90 - P10 normalisé)

SORTIE :
  - score_potentiel (0→1) par gouvernorat × groupe
  - classement zones à fort potentiel
  - dashboard textuel + CSV
  - rapport par zone géographique

Usage :
  from score_engine import ScoreEngine
  engine = ScoreEngine(models_dir='models')
  scores = engine.compute(tft_df, cluster_map, pelt_df, lstm_results)
  engine.print_report(scores)
"""

import os
import pickle
import warnings
import numpy as np
import pandas as pd
from typing import Dict, Optional

warnings.filterwarnings('ignore')

# ─── Import optionnel mappings ─────────────────────────────────────────────────
try:
    import sys
    for root in [os.path.dirname(os.path.abspath(__file__)),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')]:
        sys.path.insert(0, root)
    from mappings_BO3 import GOUVERNORAT_DEC
except ImportError:
    GOUVERNORAT_DEC = {}

ZONE_LABELS = {
    0: 'Grand Tunis',
    1: 'Littoral Nord',
    2: 'Sahel',
    3: 'Centre/Intérieur',
    4: 'Sud',
}

GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']


# ================================================================
# 1. WEIGHTS
# ================================================================

DEFAULT_WEIGHTS = {
    'S_trend'     : 0.20,   # TFT P50 T+12 (réduit de 0.35 → 0.20 ; gain vers S_trend_hist)
    'S_trend_hist': 0.15,   # [NOUVEAU] Tendance historique normalize(trend_prix) — toujours dispo
    'S_rupture'   : 0.20,
    'S_liquidite' : 0.15,
    'S_cluster'   : 0.15,
    'S_certitude' : 0.15,
}

GROUP_WEIGHTS = {
    # Résidentiel : équilibre standard — tendance historique utile sur l'ensemble du territoire
    'Residentiel': DEFAULT_WEIGHTS,
    # Foncier : liquidité faible → S_cluster et S_trend_hist plus informatifs que le TFT
    'Foncier'    : {**DEFAULT_WEIGHTS,
                    'S_trend'     : 0.15,
                    'S_trend_hist': 0.20,
                    'S_liquidite' : 0.10,
                    'S_cluster'   : 0.20},
    # Commercial : S_rupture fort (marché réactif), S_trend_hist maintenu
    'Commercial' : {**DEFAULT_WEIGHTS,
                    'S_trend'     : 0.20,
                    'S_trend_hist': 0.15,
                    'S_rupture'   : 0.25,
                    'S_certitude' : 0.10},
}


# ================================================================
# 2. HELPERS
# ================================================================

def _rank_normalize(series: pd.Series, clip_quantile: float = 0.02) -> pd.Series:
    """Normalise par rang percentile [0,1] — robuste aux outliers de prix."""
    if len(series) == 0:
        return series
    lo = series.quantile(clip_quantile)
    hi = series.quantile(1 - clip_quantile)
    clipped = series.clip(lower=lo, upper=hi)
    if hi == lo:
        return pd.Series(0.5, index=series.index)
    return (clipped - lo) / (hi - lo)


def _safe_float(val, default: float = 0.0) -> float:
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except (TypeError, ValueError):
        return default


# ================================================================
# 3. SOUS-SCORES
# ================================================================

def compute_S_trend(tft_preds: pd.DataFrame,
                    tft_df: pd.DataFrame) -> pd.DataFrame:
    """
    S_trend : croissance relative T+12 / prix actuel.

    Si TFT prédit +15% dans 12 mois → S_trend ≈ 1.
    Si TFT prédit stagnation → S_trend ≈ 0.5.
    Si TFT prédit baisse → S_trend ≈ 0.

    Formule : growth = (q50_tnd_T12 - prix_actuel) / prix_actuel
    """
    if tft_preds.empty:
        return pd.DataFrame(columns=['gouvernorat', 'groupe', 'S_trend'])

    # Prix T+12 (P50)
    pred_t12 = tft_preds[tft_preds['horizon'] == 'T+12'][
        ['gouvernorat', 'groupe', 'q50_tnd', 'q10_tnd', 'q90_tnd']
    ].copy()

    # Prix actuel moyen par gouvernorat × groupe
    if 'prix_m2' in tft_df.columns:
        prix_actuel = (tft_df.groupby(['gouvernorat', 'groupe'])['prix_m2']
                       .last().reset_index()
                       .rename(columns={'prix_m2': 'prix_actuel'}))
        pred_t12 = pred_t12.merge(prix_actuel, on=['gouvernorat', 'groupe'],
                                   how='left')
        pred_t12['prix_actuel'] = pred_t12['prix_actuel'].fillna(
            pred_t12['q50_tnd'] * 0.9)
    else:
        pred_t12['prix_actuel'] = pred_t12['q50_tnd'] * 0.9

    pred_t12['growth'] = (
        (pred_t12['q50_tnd'] - pred_t12['prix_actuel'])
        / (pred_t12['prix_actuel'] + 1e-8)
    )

    # Normalisation rank-based
    pred_t12['S_trend'] = _rank_normalize(pred_t12['growth'])

    return pred_t12[['gouvernorat', 'groupe', 'growth', 'S_trend']]


def compute_S_trend_hist(cluster_map: Dict,
                          tft_df: pd.DataFrame,
                          groupes: list) -> pd.DataFrame:
    """
    S_trend_hist — Tendance historique réelle : normalize(trend_prix).

    [NOUVEAU] Composante de tendance basée sur les données historiques
    observées, indépendante des prédictions TFT.

    Sources (fusionnées par moyenne pondérée) :
      1. prix_tendance   depuis cluster_map.pkl  (poids 0.60)
         → slope linéaire annualisé calculé dans kmeans_A.build_features()
         → unité : TND/m²/an — capte la trajectoire structurelle
      2. trend_prix      depuis tft_df           (poids 0.40)
         → colonne optionnelle produite par modeling_BO3 ou tft_dataset_builder
         → variation relative récente sur 6-12 mois

    Normalisation : _rank_normalize() sur le composite — robuste aux outliers.
    Fallback      : si trend_prix absent de tft_df, seul prix_tendance est utilisé.
    Toujours actif : retourne 0.5 (neutre) si aucune source disponible.

    Interprétation :
      S_trend_hist ≈ 1.0 → marché en forte hausse historique → fort potentiel
      S_trend_hist ≈ 0.5 → marché stable / stagnant
      S_trend_hist ≈ 0.0 → marché en recul historique → prudence
    """
    records = []
    has_trend_col = 'trend_prix' in tft_df.columns

    # ── Source 1 : prix_tendance depuis cluster_map ───────────────────────────
    cluster_trends: dict = {}
    for groupe in groupes:
        cm_grp = cluster_map.get(groupe, {})
        for gov_id, info in cm_grp.items():
            cluster_trends[(int(gov_id), groupe)] = _safe_float(
                info.get('prix_tendance', 0.0))

    # ── Source 2 (optionnelle) : trend_prix depuis tft_df ────────────────────
    tft_trends: dict = {}
    if has_trend_col:
        for (gov, groupe), grp in tft_df.groupby(['gouvernorat', 'groupe']):
            tft_trends[(gov, groupe)] = float(
                grp['trend_prix'].fillna(0.0).mean())

    # ── Fusion ────────────────────────────────────────────────────────────────
    for (gov, groupe) in (cluster_trends.keys() | tft_trends.keys()):
        t_cluster = cluster_trends.get((gov, groupe), 0.0)
        t_tft     = tft_trends.get((gov, groupe), 0.0)

        if has_trend_col and (gov, groupe) in tft_trends:
            # Les deux sources disponibles → composite pondéré
            raw = t_cluster * 0.60 + t_tft * 0.40
        else:
            # Uniquement cluster_map
            raw = t_cluster

        records.append({
            'gouvernorat'       : gov,
            'groupe'            : groupe,
            '_trend_hist_raw'   : raw,
            'trend_cluster'     : t_cluster,
            'trend_tft'         : t_tft,
        })

    if not records:
        return pd.DataFrame(columns=['gouvernorat', 'groupe', 'S_trend_hist'])

    df_th = pd.DataFrame(records)

    # Normalisation rank-based [0, 1] — robuste aux outliers de prix_tendance
    df_th['S_trend_hist'] = _rank_normalize(df_th['_trend_hist_raw'])

    n_sources = 2 if has_trend_col else 1
    print(f"  ✔ S_trend_hist : {len(df_th)} entrées | "
          f"sources={n_sources} (cluster_map"
          f"{' + tft_df[trend_prix]' if has_trend_col else ' uniquement'})")

    return df_th[['gouvernorat', 'groupe', 'S_trend_hist']]


def compute_S_rupture(pelt_df: pd.DataFrame,
                      tft_df: pd.DataFrame) -> pd.DataFrame:
    """
    S_rupture : potentiel issu des signaux PELT.

    Composantes :
      1. rupture_recente  : rupture dans les 6 derniers mois (opportunité)
      2. score_max        : intensité maximale des ruptures détectées
      3. post_rupture_moy : proportion de temps en régime post-rupture
                           (positif si rupture haussière)

    Une rupture RÉCENTE + FORTE = signal d'entrée sur le marché.
    Un marché en post_rupture depuis longtemps = régime établi (stable).
    """
    records = []
    ref_annee = int(tft_df['annee'].max()) if 'annee' in tft_df.columns else 2025
    ref_mois  = int(tft_df['mois'].max())  if 'mois'  in tft_df.columns else 12

    if 'rupture_flag' in tft_df.columns:
        # Utiliser les signaux PELT du dataset TFT (déjà intégrés)
        for (gov, groupe), grp in tft_df.groupby(['gouvernorat', 'groupe']):
            if 'rupture_flag' not in grp.columns:
                records.append({'gouvernorat': gov, 'groupe': groupe,
                                 'S_rupture': 0.3})
                continue

            grp_sorted = grp.sort_values(['annee', 'mois'])
            n = len(grp_sorted)

            # Derniers 6 mois
            recent = grp_sorted.iloc[-6:] if n >= 6 else grp_sorted
            rupture_recente = float(recent['rupture_flag'].max())

            # Score moyen sur toute la série
            score_moy = float(grp_sorted.get('score_rupture',
                                              pd.Series([0.0])).fillna(0).mean())

            # Post-rupture moyen
            post_moy  = float(grp_sorted.get('post_rupture',
                                              pd.Series([0.0])).fillna(0).mean())

            # Composite : rupture récente compte double
            raw = rupture_recente * 0.4 + score_moy * 0.4 + post_moy * 0.2
            records.append({'gouvernorat': gov, 'groupe': groupe,
                             '_S_rupt_raw': raw})

    elif not pelt_df.empty and 'gouvernorat' in pelt_df.columns:
        # Fallback : rapport PELT CSV directement
        for (gov, groupe), grp in pelt_df.groupby(['gouvernorat', 'groupe']):
            score_max = float(grp['score_norm'].max()) \
                        if 'score_norm' in grp.columns else 0.0
            # Rupture récente (< 6 mois avant la référence)
            grp_sorted = grp.copy()
            if 'date_rupture' in grp_sorted.columns:
                grp_sorted['date_dt'] = pd.to_datetime(
                    grp_sorted['date_rupture'], format='%Y-%m', errors='coerce')
                ref_date = pd.Timestamp(f"{ref_annee}-{ref_mois:02d}-01")
                recente  = (grp_sorted['date_dt'] >=
                            ref_date - pd.DateOffset(months=6))
                rupture_recente = float(recente.any())
            else:
                rupture_recente = 0.0

            raw = rupture_recente * 0.5 + score_max * 0.5
            records.append({'gouvernorat': gov, 'groupe': groupe,
                             '_S_rupt_raw': raw})
    else:
        # Aucun signal PELT
        for (gov, groupe), _ in tft_df.groupby(['gouvernorat', 'groupe']):
            records.append({'gouvernorat': gov, 'groupe': groupe,
                             '_S_rupt_raw': 0.0})

    df_rupt = pd.DataFrame(records)
    if '_S_rupt_raw' in df_rupt.columns:
        df_rupt['S_rupture'] = _rank_normalize(df_rupt['_S_rupt_raw'])
    elif 'S_rupture' not in df_rupt.columns:
        df_rupt['S_rupture'] = 0.3

    return df_rupt[['gouvernorat', 'groupe', 'S_rupture']]


def compute_S_liquidite(tft_df: pd.DataFrame,
                         cluster_map: Dict) -> pd.DataFrame:
    """
    S_liquidite : liquidité du marché par gouvernorat.

    Sources (par ordre de priorité) :
      1. indice_liquidite depuis le dataset TFT (calculé dans modeling_BO3)
      2. cluster_map → indice_liquidite (fallback)
      3. nb_annonces_proxy (proxy log-cumulatif)
    """
    records = []

    for (gov, groupe), grp in tft_df.groupby(['gouvernorat', 'groupe']):
        if 'indice_liquidite' in grp.columns and grp['indice_liquidite'].notna().any():
            liq = float(grp['indice_liquidite'].dropna().mean())
        elif 'indice_liquidite_norm' in grp.columns:
            liq = float(grp['indice_liquidite_norm'].dropna().mean())
        elif 'nb_annonces_proxy' in grp.columns:
            liq = float(grp['nb_annonces_proxy'].dropna().mean())
        else:
            # Fallback cluster_map
            cm_grp = cluster_map.get(groupe, {})
            liq = float(cm_grp.get(int(gov), {}).get('indice_liquidite', 1.0))

        records.append({'gouvernorat': gov, 'groupe': groupe, '_liq_raw': liq})

    df_liq = pd.DataFrame(records)
    df_liq['S_liquidite'] = _rank_normalize(df_liq['_liq_raw'])
    return df_liq[['gouvernorat', 'groupe', 'S_liquidite']]


def compute_S_cluster(cluster_map: Dict, groupes: list) -> pd.DataFrame:
    """
    S_cluster : score composite issu du clustering KMeans.

    Formule (sur données cluster_map.pkl) :
      raw = 0.40 × rank(score_att)
           + 0.25 × rank(potentiel_emergent → is_small_cluster_flag)
           + 0.20 × (nb_infra + nb_commerce normalisé)
           + 0.15 × rank(prix_tendance)
    """
    records = []
    for groupe in groupes:
        cm_grp = cluster_map.get(groupe, {})
        for gov_id, info in cm_grp.items():
            records.append({
                'gouvernorat'      : int(gov_id),
                'groupe'           : groupe,
                'score_att'        : _safe_float(info.get('score_att',
                                     info.get('score_attractivite', 0.3))),
                'nb_infra'         : _safe_float(info.get('nb_infra', 0)),
                'nb_commerce'      : _safe_float(info.get('nb_commerce', 0)),
                'prix_tendance'    : _safe_float(info.get('prix_tendance', 0)),
                'cluster_size'     : int(info.get('cluster_size', 3)),
                'zscore_prix'      : _safe_float(info.get('zscore_prix', 0)),
                'potentiel_emergent': _safe_float(info.get('potentiel_emergent',
                                      info.get('potentiel_score', 0))),
            })

    if not records:
        return pd.DataFrame(columns=['gouvernorat', 'groupe', 'S_cluster'])

    df_cl = pd.DataFrame(records)

    # Normalisation par rang
    df_cl['r_att']   = _rank_normalize(df_cl['score_att'])
    df_cl['r_infra'] = _rank_normalize(df_cl['nb_infra'] + df_cl['nb_commerce'])
    df_cl['r_trend'] = _rank_normalize(df_cl['prix_tendance'])
    df_cl['r_emerg'] = _rank_normalize(df_cl['potentiel_emergent'])

    # Bonus petits clusters (atypiques → opportunités cachées)
    df_cl['r_small'] = (df_cl['cluster_size'] <= 2).astype(float) * 0.1

    df_cl['S_cluster'] = (
        df_cl['r_att']   * 0.40
      + df_cl['r_infra'] * 0.20
      + df_cl['r_trend'] * 0.15
      + df_cl['r_emerg'] * 0.15
      + df_cl['r_small'] * 0.10
    ).clip(0, 1)

    return df_cl[['gouvernorat', 'groupe', 'S_cluster',
                  'score_att', 'prix_tendance', 'cluster_size']]


def compute_S_certitude(tft_preds: pd.DataFrame) -> pd.DataFrame:
    """
    S_certitude : confiance du modèle TFT.
    Inverse de l'intervalle d'incertitude (P90 - P10) normalisé.
    Utilise T+3 comme référence (horizon intermédiaire).
    """
    if tft_preds.empty:
        return pd.DataFrame(columns=['gouvernorat', 'groupe', 'S_certitude'])

    pred_t3 = tft_preds[tft_preds['horizon'] == 'T+3'][
        ['gouvernorat', 'groupe', 'incertitude']
    ].copy()

    if 'incertitude' not in pred_t3.columns or pred_t3['incertitude'].isna().all():
        pred_t3['S_certitude'] = 0.5
        return pred_t3[['gouvernorat', 'groupe', 'S_certitude']]

    pred_t3['S_certitude'] = 1.0 - _rank_normalize(
        pred_t3['incertitude'].fillna(pred_t3['incertitude'].median()))

    return pred_t3[['gouvernorat', 'groupe', 'S_certitude']]


# ================================================================
# RISK SCORE
# ================================================================

def compute_risk_score(tft_preds   : pd.DataFrame,
                        tft_df      : pd.DataFrame,
                        pelt_df     : pd.DataFrame,
                        cluster_map : Dict) -> pd.DataFrame:
    """
    Risk Score par gouvernorat × groupe.

    Le risque est UNE INFORMATION DISTINCTE du score de potentiel :
    une zone peut avoir fort potentiel ET fort risque (marchés émergents
    volatils). Une zone peut avoir faible potentiel ET faible risque
    (marchés matures stagnants).

    Composantes
    -----------
    R_volatilite : volatilité historique des prix (std / moyenne)
                   → donnée depuis tft_df (volatilite_prix_trim ou
                   volatilite_rolling)
    R_incertitude : amplitude P90 - P10 normalisée sur T+12
                   → incertitude du TFT à horizon long
    R_rupture_freq : fréquence de ruptures PELT
                   → marchés qui changent souvent de régime sont instables
    R_liquidite_inverse : marchés illiquides = risque de non-cession
                         → inverse de S_liquidite

    Formule :
      risk = 0.30·R_volatilite + 0.35·R_incertitude
           + 0.20·R_rupture_freq + 0.15·R_liquidite_inverse

    Note : risk élevé (→1) = marché risqué / volatile
           risk faible (→0) = marché stable / prévisible
    """
    records = []

    for (gov, groupe), grp in tft_df.groupby(['gouvernorat', 'groupe']):

        # ── R_volatilite ─────────────────────────────────────────
        if 'volatilite_prix_trim' in grp.columns:
            r_vol = float(grp['volatilite_prix_trim'].fillna(0).mean())
        elif 'volatilite_rolling' in grp.columns:
            r_vol = float(grp['volatilite_rolling'].fillna(0).mean())
        else:
            # Calculer depuis prix brut
            if 'prix_m2' in grp.columns:
                prix = grp['prix_m2'].dropna()
                r_vol = float(prix.std() / (prix.mean() + 1e-8)) if len(prix) > 2 else 0.3
            else:
                r_vol = 0.3

        # ── R_incertitude (depuis TFT P90-P10 horizon T+12) ──────
        r_ince = 0.3
        if not tft_preds.empty:
            t12 = tft_preds[(tft_preds['gouvernorat'] == gov) &
                             (tft_preds['groupe'] == groupe) &
                             (tft_preds['horizon'] == 'T+12')]
            if not t12.empty and 'incertitude' in t12.columns:
                r_ince = float(t12['incertitude'].iloc[0])
                # Normaliser par rapport au prix (ratio %)
                if 'q50_tnd' in t12.columns and float(t12['q50_tnd'].iloc[0]) > 0:
                    r_ince = r_ince / (float(t12['q50_tnd'].iloc[0]) + 1e-8)

        # ── R_rupture_freq ────────────────────────────────────────
        if 'rupture_flag' in grp.columns:
            r_rupt = float(grp['rupture_flag'].fillna(0).mean())
        elif not pelt_df.empty and 'gouvernorat' in pelt_df.columns:
            n_rupt = len(pelt_df[(pelt_df['gouvernorat'] == gov) &
                                   (pelt_df.get('groupe', pelt_df.get('groupe', pd.Series())) == groupe)])
            r_rupt = min(n_rupt / 5.0, 1.0)   # 5+ ruptures = score max
        else:
            r_rupt = 0.1

        # ── R_liquidite_inverse ───────────────────────────────────
        if 'indice_liquidite' in grp.columns:
            liq = float(grp['indice_liquidite'].fillna(1.0).mean())
        else:
            cm_grp = cluster_map.get(groupe, {})
            liq = float(cm_grp.get(int(gov), {}).get('indice_liquidite', 1.0))
        r_liq_inv = 1.0 / (1.0 + liq)   # liquide → faible risque

        records.append({
            'gouvernorat'    : gov,
            'groupe'         : groupe,
            '_R_vol'         : r_vol,
            '_R_ince'        : r_ince,
            '_R_rupt'        : r_rupt,
            '_R_liq_inv'     : r_liq_inv,
        })

    df_risk = pd.DataFrame(records)
    if df_risk.empty:
        return df_risk

    # Normaliser chaque composante par rang
    for col in ['_R_vol', '_R_ince', '_R_rupt', '_R_liq_inv']:
        df_risk[col + '_n'] = _rank_normalize(df_risk[col])

    df_risk['risk_score'] = (
        df_risk['_R_vol_n']     * 0.30
      + df_risk['_R_ince_n']   * 0.35
      + df_risk['_R_rupt_n']   * 0.20
      + df_risk['_R_liq_inv_n']* 0.15
    ).clip(0, 1).round(4)

    # Niveau de risque texte
    def _risk_label(r):
        if r >= 0.70: return '🔴 Risque élevé'
        if r >= 0.45: return '🟡 Risque modéré'
        return '🟢 Risque faible'

    df_risk['risk_label'] = df_risk['risk_score'].apply(_risk_label)

    cols_keep = ['gouvernorat', 'groupe', 'risk_score', 'risk_label']
    return df_risk[cols_keep]


# ================================================================
# INVESTMENT SCORE
# ================================================================

def compute_investment_score(scores_df : pd.DataFrame,
                              risk_df   : pd.DataFrame) -> pd.DataFrame:
    """
    Investment Score = combinaison score potentiel + profil risque.

    Matrice risque × rendement (4 quadrants) :
    ┌─────────────────┬──────────────────────────┬───────────────────────────┐
    │                 │  Risque FAIBLE           │  Risque ÉLEVÉ             │
    ├─────────────────┼──────────────────────────┼───────────────────────────┤
    │ Potentiel FORT  │ 🏆 OPPORTUNITÉ PREMIUM   │ ⚡ OPPORTUNITÉ SPÉCULAT. │
    │ Potentiel FAIBLE│ 💤 MARCHÉ STABLE/SÛRE   │ ❌ ÉVITER                 │
    └─────────────────┴──────────────────────────┴───────────────────────────┘

    Formule :
      invest_score = score_potentiel × (1 - risk_penalty)
      où risk_penalty = 0.3 × risk_score  (le risque pénalise, pas élimine)

    Permet un classement final où une zone à fort potentiel et faible risque
    surclasse une zone à fort potentiel mais très risquée.
    """
    df = scores_df.copy()

    if not risk_df.empty:
        df = df.merge(risk_df[['gouvernorat', 'groupe', 'risk_score', 'risk_label']],
                      on=['gouvernorat', 'groupe'], how='left')
        df['risk_score'] = df['risk_score'].fillna(0.3)
        df['risk_label'] = df['risk_label'].fillna('🟡 Risque modéré')
    else:
        df['risk_score'] = 0.3
        df['risk_label'] = '🟡 Risque modéré'

    # Investment score avec pénalité risque
    df['invest_score'] = (
        df['score_potentiel'] * (1.0 - 0.30 * df['risk_score'])
    ).clip(0, 1).round(4)

    # Rang investment (par groupe)
    df['invest_rang'] = df.groupby('groupe')['invest_score'].rank(
        ascending=False, method='min').astype(int)

    # Quadrant risque × potentiel
    def _quadrant(row):
        high_pot  = row['score_potentiel'] >= 0.60
        high_risk = row['risk_score']      >= 0.50
        if   high_pot and not high_risk: return '🏆 Opportunité premium'
        elif high_pot and     high_risk: return '⚡ Spéculatif'
        elif not high_pot and not high_risk: return '💤 Stable/Sûr'
        else:                            return '❌ Éviter'

    df['quadrant'] = df.apply(_quadrant, axis=1)

    return df


# ================================================================
# 4. SCORE ENGINE PRINCIPAL
# ================================================================

class ScoreEngine:
    """
    Moteur de scoring de potentiel immobilier.

    Agrège les sorties de KMeans + PELT + LSTM + TFT en un score
    unique, interprétable et comparable entre gouvernorats.
    """

    def __init__(self, models_dir: str = 'models',
                 weights: Dict = None):
        self.models_dir = models_dir
        self.weights    = weights or GROUP_WEIGHTS

    def compute(self,
                tft_df       : pd.DataFrame,
                cluster_map  : Dict,
                pelt_df      : pd.DataFrame = None,
                tft_preds    : pd.DataFrame = None,
                lstm_results : Dict         = None,
                groupes      : list         = None) -> pd.DataFrame:
        """
        Calcule le score de potentiel pour tous les gouvernorats.

        Paramètres
        ----------
        tft_df       : DataFrame TFT (depuis tft_dataset_builder)
        cluster_map  : dict depuis cluster_map.pkl
        pelt_df      : DataFrame depuis pelt_A_rapport.csv (optionnel)
        tft_preds    : DataFrame depuis predict_tft() (optionnel)
        lstm_results : dict depuis run_lstm() (optionnel)
        groupes      : liste des groupes à scorer (défaut : GROUPES_ACTIFS)

        Retourne
        --------
        DataFrame avec colonnes :
          gouvernorat | nom | groupe | zone | cluster | cluster_size
          S_trend | S_rupture | S_liquidite | S_cluster | S_certitude
          score_potentiel | rang | interpretation
        """
        if groupes is None:
            groupes = GROUPES_ACTIFS

        if pelt_df is None:
            pelt_df = pd.DataFrame()

        if tft_preds is None:
            tft_preds = pd.DataFrame()

        print("\n" + "=" * 72)
        print("  MARKET POTENTIAL SCORE ENGINE · MARCHÉ IMMOBILIER TUNISIEN")
        print("=" * 72)

        # ── Calcul des sous-scores ─────────────────────────────────
        print("\n  Calcul des sous-scores...")

        s_trend    = compute_S_trend(tft_preds, tft_df)
        s_trend_h  = compute_S_trend_hist(cluster_map, tft_df, groupes)   # [NOUVEAU]
        s_rupture  = compute_S_rupture(pelt_df, tft_df)
        s_liq      = compute_S_liquidite(tft_df, cluster_map)
        s_cluster  = compute_S_cluster(cluster_map, groupes)
        s_cert     = compute_S_certitude(tft_preds)

        print(f"  ✔ S_trend     : {len(s_trend)} entrées")
        print(f"  ✔ S_trend_hist: {len(s_trend_h)} entrées  ← tendance historique [NOUVEAU]")
        print(f"  ✔ S_rupture   : {len(s_rupture)} entrées")
        print(f"  ✔ S_liquidite : {len(s_liq)} entrées")
        print(f"  ✔ S_cluster   : {len(s_cluster)} entrées")
        print(f"  ✔ S_certitude : {len(s_cert)} entrées")

        # ── Calcul risk score ──────────────────────────────────────
        print("\n  Calcul du risk score...")
        risk_df = compute_risk_score(tft_preds, tft_df, pelt_df, cluster_map)
        print(f"  ✔ Risk score  : {len(risk_df)} entrées")

        # ── Fusion sur (gouvernorat, groupe) ──────────────────────
        # Base : toutes les combinaisons gov × groupe
        base = (tft_df[['gouvernorat', 'groupe']]
                .drop_duplicates()
                .copy())

        for df_sub, col in [
            (s_trend,   'S_trend'),
            (s_trend_h, 'S_trend_hist'),   # [NOUVEAU] tendance historique
            (s_rupture, 'S_rupture'),
            (s_liq,     'S_liquidite'),
            (s_cert,    'S_certitude'),
        ]:
            if not df_sub.empty and col in df_sub.columns:
                base = base.merge(
                    df_sub[['gouvernorat', 'groupe', col]],
                    on=['gouvernorat', 'groupe'], how='left')
            else:
                base[col] = 0.5

        # Cluster (par gouvernorat uniquement, pas par groupe)
        if not s_cluster.empty:
            base = base.merge(
                s_cluster[['gouvernorat', 'groupe', 'S_cluster',
                            'score_att', 'prix_tendance', 'cluster_size']],
                on=['gouvernorat', 'groupe'], how='left')
        else:
            base['S_cluster']   = 0.5
            base['score_att']   = 0.3
            base['prix_tendance'] = 0.0
            base['cluster_size'] = 3

        # Remplir les NaN par 0.5 (score neutre)
        for col in ['S_trend', 'S_trend_hist', 'S_rupture', 'S_liquidite',
                    'S_cluster', 'S_certitude']:
            base[col] = base[col].fillna(0.5)

        # ── Score final pondéré par groupe ─────────────────────────
        def _score_row(row):
            groupe = row['groupe']
            w = self.weights.get(groupe, DEFAULT_WEIGHTS)
            total_w = sum(w.values())
            score = (
                row['S_trend']      * w.get('S_trend',      0.20)
              + row['S_trend_hist'] * w.get('S_trend_hist', 0.15)   # [NOUVEAU]
              + row['S_rupture']    * w.get('S_rupture',    0.20)
              + row['S_liquidite']  * w.get('S_liquidite',  0.15)
              + row['S_cluster']    * w.get('S_cluster',    0.15)
              + row['S_certitude']  * w.get('S_certitude',  0.15)
            ) / total_w
            return round(min(max(score, 0.0), 1.0), 4)

        base['score_potentiel'] = base.apply(_score_row, axis=1)

        # ── Rang (par groupe) ──────────────────────────────────────
        base['rang'] = base.groupby('groupe')['score_potentiel'].rank(
            ascending=False, method='min').astype(int)

        # ── Informations contextuelles ─────────────────────────────
        base['nom'] = base['gouvernorat'].apply(
            lambda g: GOUVERNORAT_DEC.get(int(g), str(g)))

        # Zone géographique depuis cluster_map
        def _get_zone(gov, groupe):
            cm_grp = cluster_map.get(groupe, {})
            return cm_grp.get(int(gov), {}).get('zone_geographique',
                   cm_grp.get(int(gov), {}).get('zone_geo', 3))

        base['zone'] = base.apply(
            lambda r: _get_zone(r['gouvernorat'], r['groupe']), axis=1)
        base['zone_label'] = base['zone'].apply(
            lambda z: ZONE_LABELS.get(int(z), 'Inconnu'))

        # Cluster
        def _get_cluster(gov, groupe):
            cm_grp = cluster_map.get(groupe, {})
            return cm_grp.get(int(gov), {}).get('cluster', 0)

        base['cluster'] = base.apply(
            lambda r: _get_cluster(r['gouvernorat'], r['groupe']), axis=1)

        # ── Interprétation texte ───────────────────────────────────
        def _interpret(score):
            if score >= 0.75: return '🔥 Fort potentiel'
            if score >= 0.60: return '📈 Potentiel modéré'
            if score >= 0.45: return '➡️  Marché stable'
            if score >= 0.30: return '📉 Potentiel faible'
            return '⚠️  Marché risqué'

        base['interpretation'] = base['score_potentiel'].apply(_interpret)

        # ── Investment score (potentiel ajusté du risque) ──────────
        print("\n  Calcul de l'investment score...")
        base = compute_investment_score(base, risk_df)

        # ── Rang global (tous groupes, par invest_score moyen) ─────
        gov_global = (base.groupby(['gouvernorat', 'nom', 'zone_label'])
                          .agg(score_moyen      = ('score_potentiel', 'mean'),
                               invest_moyen     = ('invest_score',    'mean'),
                               risk_moyen       = ('risk_score',      'mean'))
                          .reset_index()
                          .sort_values('invest_moyen', ascending=False)
                          .reset_index(drop=True))
        gov_global['rang_global'] = gov_global.index + 1
        base = base.merge(
            gov_global[['gouvernorat', 'rang_global']],
            on='gouvernorat', how='left')

        # Ordre final
        cols_order = [
            'gouvernorat', 'nom', 'groupe', 'zone_label', 'cluster', 'cluster_size',
            'score_potentiel', 'rang', 'rang_global', 'invest_score', 'invest_rang',
            'risk_score', 'risk_label', 'quadrant', 'interpretation',
            'S_trend', 'S_trend_hist', 'S_rupture', 'S_liquidite', 'S_cluster', 'S_certitude',
            'score_att', 'prix_tendance',
        ]
        base = base[[c for c in cols_order if c in base.columns]]
        base = base.sort_values(['groupe', 'invest_rang']).reset_index(drop=True)

        return base

    def print_report(self, scores: pd.DataFrame, top_n: int = 5) -> None:
        """Affiche le rapport complet : potentiel + risque + investment + classement."""
        print("\n" + "=" * 80)
        print("  RAPPORT — MARKET POTENTIAL SCORE ENGINE · MARCHÉ IMMOBILIER TUNISIEN")
        print("=" * 80)

        for groupe in scores['groupe'].unique():
            sub = scores[scores['groupe'] == groupe].copy()
            print(f"\n  ─── {groupe.upper()} ─── (classé par invest_score)")
            print(f"  {'#':>3} {'Gouvernorat':<16} {'Quadrant':<26} "
                  f"{'Pot':>5} {'Risk':>5} {'Invest':>6} "
                  f"{'Trend':>5} {'TrHist':>6} {'Rupt':>5} {'Liq':>5}")
            print(f"  {'─'*96}")
            for _, row in sub.head(top_n * 2).iterrows():
                print(f"  {int(row.get('invest_rang', 0)):>3} "
                      f"{str(row['nom']):<16} "
                      f"{str(row.get('quadrant','?')):<26} "
                      f"{row['score_potentiel']:>5.2f} "
                      f"{row.get('risk_score', 0):>5.2f} "
                      f"{row.get('invest_score', 0):>6.3f} "
                      f"{row['S_trend']:>5.2f} "
                      f"{row.get('S_trend_hist', 0):>6.2f} "
                      f"{row['S_rupture']:>5.2f} "
                      f"{row['S_liquidite']:>5.2f}")

        # ── Classement global invest ───────────────────────────────
        print("\n" + "─" * 80)
        print("  CLASSEMENT GLOBAL — INVEST SCORE MOYEN (tous groupes) :")
        top_global = (scores.groupby(['gouvernorat', 'nom', 'zone_label'])
                             .agg(invest_moyen=('invest_score', 'mean'),
                                  risk_moyen  =('risk_score',   'mean'),
                                  pot_moyen   =('score_potentiel','mean'))
                             .reset_index()
                             .sort_values('invest_moyen', ascending=False)
                             .head(top_n))
        for i, (_, row) in enumerate(top_global.iterrows(), 1):
            bar  = '█' * int(row['invest_moyen'] * 20)
            risk = '🔴' if row['risk_moyen'] >= 0.70 else ('🟡' if row['risk_moyen'] >= 0.45 else '🟢')
            print(f"  {i}. {row['nom']:<16} [{row['zone_label']:<18}] "
                  f"invest={row['invest_moyen']:.3f} "
                  f"pot={row['pot_moyen']:.2f} "
                  f"risk={row['risk_moyen']:.2f}{risk}  {bar}")

        # ── Résumé quadrants ──────────────────────────────────────
        if 'quadrant' in scores.columns:
            print("\n  DISTRIBUTION QUADRANTS :")
            for q, grp in scores.groupby('quadrant'):
                print(f"  {q:<28} : {len(grp)} gouvernorats×groupes")

        print("=" * 80)

    def save(self, scores: pd.DataFrame, output_dir: str = 'models') -> str:
        """Sauvegarde le scoring en CSV."""
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, 'market_potential_scores.csv')
        scores.to_csv(path, index=False)
        print(f"  → Scores sauvegardés : {path}")
        return path


# ================================================================
# 5. POINT D'ENTRÉE
# ================================================================

if __name__ == '__main__':
    import sys

    BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
    MODELS_DIR = os.path.join(BASE_DIR, 'models')

    print("Chargement des artefacts pipeline...")

    # ── cluster_map ───────────────────────────────────────────────
    cm_path = os.path.join(MODELS_DIR, 'cluster_map.pkl')
    if not os.path.exists(cm_path):
        print(f"✗ cluster_map.pkl introuvable. Exécutez d'abord kmeans_A.py")
        sys.exit(1)
    with open(cm_path, 'rb') as f:
        cluster_map = pickle.load(f)
    print(f"✔ cluster_map : {sum(len(v) for v in cluster_map.values())} gouvernorats")

    # ── tft_dataset ───────────────────────────────────────────────
    tft_pkl = os.path.join(MODELS_DIR, 'tft_dataset.pkl')
    if not os.path.exists(tft_pkl):
        print(f"✗ tft_dataset.pkl introuvable. Exécutez d'abord tft_dataset_builder.py")
        sys.exit(1)
    with open(tft_pkl, 'rb') as f:
        data = pickle.load(f)
    tft_df = data['df']
    print(f"✔ tft_dataset : {len(tft_df):,} lignes")

    # ── pelt_rapport (optionnel) ──────────────────────────────────
    pelt_csv = os.path.join(MODELS_DIR, 'pelt_A_rapport.csv')
    pelt_df  = pd.read_csv(pelt_csv) if os.path.exists(pelt_csv) else pd.DataFrame()
    if not pelt_df.empty:
        print(f"✔ pelt_rapport : {len(pelt_df)} ruptures")

    # ── tft_predictions (optionnel) ───────────────────────────────
    tft_pred_csv = os.path.join(MODELS_DIR, 'tft_predictions.csv')
    tft_preds    = pd.read_csv(tft_pred_csv) \
                   if os.path.exists(tft_pred_csv) else pd.DataFrame()
    if not tft_preds.empty:
        print(f"✔ tft_predictions : {len(tft_preds)} prédictions")

    # ── Calcul scores ─────────────────────────────────────────────
    engine = ScoreEngine(models_dir=MODELS_DIR)
    scores = engine.compute(
        tft_df      = tft_df,
        cluster_map = cluster_map,
        pelt_df     = pelt_df,
        tft_preds   = tft_preds,
    )
    engine.print_report(scores, top_n=5)
    engine.save(scores, MODELS_DIR)
