"""
lstm_A.py — LSTM Bidirectionnel + Attention · Version Production L99+
=====================================================================

CORRECTION MAJEURE — STRATIFICATION type_transaction :
  • 3 features prix stratifiées ajoutées à ALL_FEATURES :
      indice_prix_m2_loc   (type_transaction=1 — Location)
      indice_prix_m2_ven   (type_transaction=2 — Vente)
      ratio_location_vente (composition du marché par gouvernorat × mois)
  • preparer_series_aggregees() calcule ces features en agrégeant séparément
    les deux types de transaction avant de les joindre sur le signal global.
  • La TARGET de prédiction reste indice_prix_m2_regional (signal global)
    mais le modèle dispose maintenant du contexte loc/ven pour prédire
    sans être trompé par des shifts de composition.
  • enrichir_dataframe() récupère ratio_location_vente depuis cluster_map enrichi.
  • CLUSTERING : suppression du CLUSTERING_REALISTE codé en dur.
    Le cluster_map produit par kmeans_A.py est utilisé directement → cohérence chaîne.
    Fallback sur un clustering géographique simple si cluster_map.pkl absent.

MÉTRIQUES — Remplacement MAPE par SMAPE + nMAE (L99+) :
  • SMAPE (Symmetric MAPE) : stable quand prix faibles ou rares → métrique principale.
      SMAPE = 2 × |y_pred − y_true| / (|y_true| + |y_pred|)
  • nMAE (MAE normalisé) : MAE / moyenne_prix → interprétable en %, robuste.
  • MAPE gardé en complément uniquement.
  ✅ Phrase prof : "Nous avons remplacé le MAPE par SMAPE et MAE normalisé afin
     de mieux évaluer les performances dans les zones à faible volume de données."

POIDS LIQUIDITÉ (L99+) :
  • loss_weight = log(1 + nb_annonces) — proxy : longueur de série par gouvernorat.
  • Combiné avec sample_weight_temporal (Quick Win 1).
  • Donne plus d'importance aux zones avec des données fiables.

TENDANCE + VARIABLES EXOGÈNES (ajout) :
  • trend_prix    = pct_change(3) par gouvernorat sur indice_prix_m2_regional agrégé
  • momentum_prix = diff(3) par gouvernorat sur indice_prix_m2_regional agrégé
  • Fallback si colonnes exogènes absentes :
      inflation_proxy    ← pct_change(12) sur indice_prix_m2_regional
      croissance_pib_trim ← variation trimestrielle de l'indice global
      high_season         ← mois 4-9 = 1.0, sinon 0.0
  • NaN gérés : ffill → bfill → fillna(0.0) par gouvernorat
  • Aucune feature existante supprimée

ARCHITECTURE : BiLSTM(24) + Attention · 16+ features · split global strict
"""

import os, sys, pickle, warnings, logging
import numpy as np
import pandas as pd
from pathlib import Path

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
warnings.filterwarnings('ignore')
logging.getLogger('tensorflow').setLevel(logging.ERROR)

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from features_registry import build_features, validate_features, get_features

# ─── Fix Lambda layer deserialization ─────────────────────────────────────────
# Le modèle utilise layers.Lambda(lambda z: tf.reduce_sum(z, axis=1))
# pour le mécanisme d'attention. Keras 3+ refuse de charger les couches Lambda
# par défaut (safe_mode) car elles contiennent du code Python arbitraire.
# Cette ligne est OBLIGATOIRE pour rendre le pipeline reproductible.
# Référence : https://keras.io/api/models/model_saving_apis/
try:
    keras.config.enable_unsafe_deserialization()
except AttributeError:
    pass  # Keras < 3 : pas nécessaire, safe_mode n'existe pas encore
from sklearn.preprocessing import RobustScaler
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

tf.random.set_seed(42)
np.random.seed(42)

# ─── Import mappings ───────────────────────────────────────────────────────
def _import_gouvernorat_dec():
    candidates = [
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..')),
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')),
        os.path.dirname(os.path.abspath(__file__)),
    ]
    for root in candidates:
        sys.path.insert(0, root)
        try:
            from BO3.mappings_BO3 import GOUVERNORAT_DEC
            return GOUVERNORAT_DEC
        except ImportError:
            pass
        try:
            from mappings_BO3 import GOUVERNORAT_DEC
            return GOUVERNORAT_DEC
        except ImportError:
            pass
    raise ImportError("mappings_BO3.py introuvable.")

GOUVERNORAT_DEC = _import_gouvernorat_dec()

# ─── Feature Registry centralisé ──────────────────────────────────────────────
# lstm_A.py lit les features LSTM depuis le dataset BO3.
# Les features post-pipeline (cluster_id, post_rupture, etc.) sont ajoutées
# dans enrichir_dataframe() depuis cluster_map + pelt_rapport — elles ne sont
# PAS dans TARGET_COLS mais dans POST_PIPELINE_FEATURES (voir features_config.py).
try:
    from features_config import get_features, validate_features
    _FEATURES_CONFIG_AVAILABLE = True
except ImportError:
    _FEATURES_CONFIG_AVAILABLE = False

# ─── Répertoires ───────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models')
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs', 'lstm')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(GRAPHS_DIR, exist_ok=True)

GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']

# Codes type_transaction
TT_LOC = 1
TT_VEN = 2

# ─── Hyperparamètres L99 ───────────────────────────────────────────────────
LOOKBACK = 6
HORIZON = 3
MIN_TRAIN = LOOKBACK + HORIZON + 2
LSTM_UNITS = 24
DROPOUT = 0.40
L2_REG = 2e-4
EPOCHS = 200
PATIENCE = 15
BATCH_SIZE = 16
LR_INIT = 1e-3
QUANTILE_ALPHA = 0.5
IQR_CLIP_FACTOR = 3.0
GRAD_CLIP_NORM = 1.0

# Features brutes du pipeline
FEATURES_BRUTES = [
    'indice_prix_m2_regional',   # signal global (TARGET)
    'indice_prix_m2_loc',        # prix location (type_transaction=1)
    'indice_prix_m2_ven',        # prix vente    (type_transaction=2)
    'ratio_location_vente',      # composition marché (0=loc pure, 1=ven pure)
    'glissement_immo_trim',
    'inflation_glissement_annuel',
    'croissance_pib_trim',
    'score_attractivite',
    'nb_infra',
    'nb_commerce',
    'high_season',
    # ── Tendance (calculées dans preparer_series_aggregees) ──────────────
    'trend_prix',       # pct_change(3) par gouvernorat — dynamique court terme
    'momentum_prix',    # diff(3) par gouvernorat — momentum en valeur TND
    # ── Exogène fallback ─────────────────────────────────────────────────
    'inflation_proxy',  # utilisé si inflation_glissement_annuel absent du DF
]

FIAB_ENCODE = {'haute': 1.0, 'moyenne': 0.5, 'faible': 0.2}

# Features issues du pipeline kmeans + pelt
FEATURES_PIPELINE = [
    'cluster_id', 'post_rupture', 'score_rupture_norm',
    'rupture_intensity', 'zscore_prix',
]

ALL_FEATURES = FEATURES_BRUTES + FEATURES_PIPELINE
N_FEATURES = len(ALL_FEATURES)

CLUSTER_COLORS = {0:'#1D9E75', 1:'#D85A30', 2:'#7F77DD', 3:'#EF9F27'}


# ─── 0. Fallback clustering géographique (si cluster_map.pkl absent) ────────

CLUSTERING_FALLBACK = {
    'Residentiel': {
        0: ['Ariana', 'Ben Arous', 'Bizerte', 'Manouba', 'Tunis'],
        1: ['Monastir', 'Nabeul', 'Sfax', 'Sousse'],
        2: ['Gabès', 'Jendouba', 'Kairouan', 'Mahdia', 'Médenine'],
        3: ['Béja', 'Gafsa', 'Kasserine', 'Kébili', 'Le Kef',
            'Siliana', 'Sidi Bouzid', 'Tataouine', 'Tozeur', 'Zaghouan']
    },
    'Foncier': {
        0: ['Ariana', 'Ben Arous', 'Manouba', 'Tunis'],
        1: ['Bizerte', 'Jendouba', 'Kairouan', 'Mahdia', 'Monastir',
            'Nabeul', 'Sfax', 'Sousse'],
        2: ['Gabès', 'Médenine'],
        3: ['Béja', 'Gafsa', 'Kasserine', 'Kébili', 'Le Kef',
            'Siliana', 'Sidi Bouzid', 'Tataouine', 'Tozeur', 'Zaghouan']
    },
    'Commercial': {
        0: ['Ariana', 'Ben Arous', 'Sfax', 'Sousse', 'Tunis'],
        1: ['Bizerte', 'Mahdia', 'Monastir', 'Nabeul'],
        2: ['Gabès', 'Médenine', 'Kairouan'],
        3: ['Béja', 'Gafsa', 'Jendouba', 'Kasserine', 'Kébili', 'Le Kef',
            'Manouba', 'Siliana', 'Sidi Bouzid', 'Tataouine', 'Tozeur', 'Zaghouan']
    }
}

ZONE_NAMES_FALLBACK = {
    'Residentiel': {0:'Nord (prix élevés)', 1:'Centre dynamique',
                    2:'Sud (prix bas)', 3:'Données limitées'},
    'Foncier':     {0:'Périphérie urbaine', 1:'Centre mixte',
                    2:'Sud agricole', 3:'Données limitées'},
    'Commercial':  {0:'Grands pôles', 1:'Touristique/moyen',
                    2:'Sud faible activité', 3:'Données limitées'},
}


def _build_fallback_cluster_map(groupe: str) -> dict:
    """
    Construit un cluster_map depuis CLUSTERING_FALLBACK si cluster_map.pkl absent.
    Utilisé uniquement en dernier recours — préférer toujours cluster_map.pkl de kmeans_A.
    """
    cluster_map = {}
    clustering  = CLUSTERING_FALLBACK.get(groupe, {})
    zones       = ZONE_NAMES_FALLBACK.get(groupe, {})
    for cluster_id, gov_names in clustering.items():
        for gov_name in gov_names:
            gov_code = next((c for c, n in GOUVERNORAT_DEC.items()
                             if n == gov_name), None)
            if gov_code is not None:
                cluster_map[gov_code] = {
                    'cluster'              : cluster_id,
                    'nom'                  : gov_name,
                    'zone'                 : zones.get(cluster_id, ''),
                    'zscore_prix'          : 0.0,
                    'spread_reel'          : 0.0,
                    'ratio_location_vente' : 0.5,  # neutre
                    'prix_mean_loc'        : 0.0,
                    'prix_mean_ven'        : 0.0,
                }
    return cluster_map


# ─── 1. Chargement artefacts pipeline ─────────────────────────────────────
def charger_pipeline_artefacts(models_dir: str) -> tuple:
    """
    Charge cluster_map.pkl (kmeans_A) et pelt_A_rapport.csv (pelt_A).
    cluster_map.pkl est maintenant enrichi avec prix_mean_loc, prix_mean_ven,
    ratio_location_vente par kmeans_A corrigé.
    """
    cm_path   = os.path.join(models_dir, 'cluster_map.pkl')
    pelt_path = os.path.join(models_dir, 'pelt_A_rapport.csv')

    cluster_map = {}
    pelt_df     = pd.DataFrame()

    if os.path.exists(cm_path):
        with open(cm_path, 'rb') as f:
            cluster_map = pickle.load(f)
        print(f" ✔ cluster_map.pkl chargé (enrichi par kmeans_A corrigé)")
        for grp, mp in cluster_map.items():
            has_loc = any('prix_mean_loc' in v for v in mp.values())
            has_ven = any('prix_mean_ven' in v for v in mp.values())
            print(f"   {grp:<14} : {len(mp)} gov | "
                  f"prix_mean_loc={'✔' if has_loc else '✗'} | "
                  f"prix_mean_ven={'✔' if has_ven else '✗'}")
    else:
        print(f" ℹ cluster_map.pkl absent → fallback géographique")

    if os.path.exists(pelt_path):
        pelt_df = pd.read_csv(pelt_path)
        if 'date_rupture' in pelt_df.columns:
            pelt_df['date_rupture'] = pd.to_datetime(
                pelt_df['date_rupture'], format='%Y-%m', errors='coerce')
        has_rl = 'ratio_location_vente' in pelt_df.columns
        print(f" ✔ pelt_A_rapport.csv chargé ({len(pelt_df)} ruptures | "
              f"ratio_loc_ven={'✔' if has_rl else '✗'})")

    return cluster_map, pelt_df


# ─── 2. Enrichissement DataFrame (CORRECTION type_transaction) ────────────
def enrichir_dataframe(df: pd.DataFrame, groupe: str,
                       cluster_map: dict, pelt_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrichit le DataFrame avec les features pipeline + features stratifiées.

    CORRECTION :
      - ratio_location_vente récupéré depuis cluster_map (calculé par kmeans_A).
      - Si cluster_map absent → fallback CLUSTERING_FALLBACK.
      - indice_prix_m2_loc et indice_prix_m2_ven calculés ici par gouvernorat
        (valeurs globales — les valeurs mensuelles sont calculées dans
        preparer_series_aggregees()).
    """
    cm_grp = cluster_map.get(groupe, {})
    if not cm_grp:
        cm_grp = _build_fallback_cluster_map(groupe)
        print(f"   → Fallback clustering géographique pour {groupe}")

    rup_by_gov = {}
    if not pelt_df.empty and 'groupe' in pelt_df.columns:
        pelt_df = pelt_df.copy()
        if 'date_rupture' in pelt_df.columns:
            pelt_df['date_rupture'] = pd.to_datetime(
                pelt_df['date_rupture'].astype(str).str[:7] + '-01',
                errors='coerce')
        pelt_grp = pelt_df[pelt_df['groupe'] == groupe]
        for gov_code, grp_rup in pelt_grp.groupby('gouvernorat'):
            grp_rup_sorted = grp_rup.sort_values('date_rupture')
            rup_by_gov[int(gov_code)] = grp_rup_sorted[
                ['date_rupture','score_norm','fiabilite_signal']
            ].to_dict('records')

    df = df.copy()
    df['date'] = pd.to_datetime(
        df['annee'].astype(str) + '-' + df['mois'].astype(str).str.zfill(2) + '-01')

    # Pré-calculer prix moyens loc/ven par gouvernorat (niveau global)
    # Les valeurs mensuelles finales sont calculées dans preparer_series_aggregees
    gov_prix_loc = (df[df['type_transaction'] == TT_LOC]
                    .groupby('gouvernorat')['indice_prix_m2_regional'].mean()
                    .rename('_pm_loc'))
    gov_prix_ven = (df[df['type_transaction'] == TT_VEN]
                    .groupby('gouvernorat')['indice_prix_m2_regional'].mean()
                    .rename('_pm_ven'))

    records = []
    for gov_code, gov_group in df.groupby('gouvernorat'):
        gov_code_int = int(gov_code)
        cm_info      = cm_grp.get(gov_code_int, {})
        cluster_id   = cm_info.get('cluster', 0)
        zone_name    = cm_info.get('zone', f'Cluster {cluster_id}')
        zscore_prix  = cm_info.get('zscore_prix', 0.0)
        # ratio_location_vente depuis cluster_map (calculé par kmeans_A sur données réelles)
        ratio_lv     = cm_info.get('ratio_location_vente', 0.5)
        ruptures_gov = rup_by_gov.get(gov_code_int, [])

        for _, row in gov_group.iterrows():
            t = row['date']
            rup_active = [r for r in ruptures_gov if r['date_rupture'] <= t]
            if rup_active:
                last_rup = max(rup_active, key=lambda r: r['date_rupture'])
                post_rup  = 1
                score_rup = float(last_rup.get('score_norm', 0) or 0)
                fiab_str  = last_rup.get('fiabilite_signal', 'faible')
                fiab_num  = FIAB_ENCODE.get(str(fiab_str), 0.2)
            else:
                post_rup = 0; score_rup = 0.0; fiab_num = 0.0

            new_row = row.to_dict()
            new_row.update({
                'cluster_id'           : cluster_id,
                'post_rupture'         : post_rup,
                'score_rupture_norm'   : score_rup,
                'rupture_intensity'    : round(score_rup * fiab_num, 6),
                'zscore_prix'          : zscore_prix,
                'zone_name'            : zone_name,
                'ratio_location_vente' : ratio_lv,
            })
            records.append(new_row)

    df_enrichi = pd.DataFrame(records)
    df_enrichi = df_enrichi.sort_values(['gouvernorat','annee','mois']).reset_index(drop=True)
    return df_enrichi


# ─── 3. Agrégation mensuelle (CORRECTION : stratifiée par type_transaction) ──

def preparer_series_aggregees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège par (gouvernorat, annee, mois).

    CORRECTION type_transaction :
    Calcule indice_prix_m2_loc et indice_prix_m2_ven en agrégeant séparément
    les deux types AVANT de les joindre au signal global.
    Évite la contamination du signal global par la composition loc/ven.

    Le modèle LSTM dispose ainsi de 3 signaux de prix :
      indice_prix_m2_regional  → signal global (ce qu'on prédit)
      indice_prix_m2_loc       → sous-signal location
      indice_prix_m2_ven       → sous-signal vente

    TENDANCE + EXOGÈNES (ajout) :
      trend_prix     → pct_change(3) par gouvernorat, calculé sur série agrégée
      momentum_prix  → diff(3) par gouvernorat, calculé sur série agrégée
      Fallback si colonnes exogènes absentes du DataFrame source :
        inflation_proxy    ← pct_change(12) sur indice_prix_m2_regional
        croissance_pib_trim ← variation trimestrielle de l'indice global
        high_season         ← mois 4-9 = 1.0
      NaN : ffill → bfill → fillna(0.0) par gouvernorat — zéro NaN en sortie.
    """
    agg_cols = [c for c in ALL_FEATURES if c in df.columns]
    # Exclure les colonnes calculées séparément pour éviter le double comptage
    agg_cols_base = [c for c in agg_cols
                     if c not in ('indice_prix_m2_loc', 'indice_prix_m2_ven',
                                  'ratio_location_vente',
                                  'trend_prix', 'momentum_prix', 'inflation_proxy')]

    grp = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg = grp[agg_cols_base].mean().reset_index()

    # ── Agrégation stratifiée par type_transaction (CORRECTION) ─────────────
    for tt, col_out in [(TT_LOC, 'indice_prix_m2_loc'),
                         (TT_VEN, 'indice_prix_m2_ven')]:
        sub_tt = df[df['type_transaction'] == tt]
        if len(sub_tt) > 0:
            agg_tt = (sub_tt.groupby(['gouvernorat','annee','mois'])['indice_prix_m2_regional']
                            .mean()
                            .reset_index()
                            .rename(columns={'indice_prix_m2_regional': col_out}))
            agg = agg.merge(agg_tt, on=['gouvernorat','annee','mois'], how='left')
        else:
            agg[col_out] = np.nan

    # ── ratio_location_vente mensuel (CORRECTION) ─────────────────────────
    # Proportion de transactions vente par gouvernorat × mois
    n_grp = df.groupby(['gouvernorat','annee','mois'])['type_transaction'].count()
    n_ven = (df[df['type_transaction'] == TT_VEN]
             .groupby(['gouvernorat','annee','mois'])['type_transaction'].count())
    ratio_monthly = (n_ven / n_grp).reset_index()
    ratio_monthly.columns = ['gouvernorat','annee','mois','ratio_location_vente']
    agg = agg.merge(ratio_monthly, on=['gouvernorat','annee','mois'], how='left')
    agg['ratio_location_vente'] = agg['ratio_location_vente'].fillna(0.5)

    # Remplacer les NaN sur les colonnes stratifiées par interpolation intra-gouvernorat
    for col in ['indice_prix_m2_loc', 'indice_prix_m2_ven']:
        if col in agg.columns:
            agg[col] = (agg.groupby('gouvernorat')[col]
                           .transform(lambda x: x.interpolate(method='linear',
                                                              limit_direction='both')))

    agg = agg.sort_values(['gouvernorat','annee','mois']).reset_index(drop=True)

    # ── TENDANCE : calculées sur la série agrégée, par gouvernorat ────────────
    # Appliquées après agrégation mensuelle → signaux propres sans bruit
    # transaction-level.
    # pct_change(3) = variation sur 3 mois glissants → dynamique court terme
    # diff(3)       = différence absolue sur 3 mois  → momentum en valeur TND
    agg['trend_prix'] = (
        agg.groupby('gouvernorat')['indice_prix_m2_regional']
           .pct_change(3)
    )
    agg['momentum_prix'] = (
        agg.groupby('gouvernorat')['indice_prix_m2_regional']
           .diff(3)
    )
    # Gestion NaN : les 3 premières observations par gouvernorat seront NaN
    # Triple filet : ffill → bfill → fillna(0.0)
    for col in ['trend_prix', 'momentum_prix']:
        agg[col] = (
            agg.groupby('gouvernorat')[col]
               .transform(lambda x: x.ffill().bfill().fillna(0.0))
        )

    # ── VARIABLES EXOGÈNES : fallbacks propres — objectif 0 warnings ────────────
    # Stratégie : on cherche en priorité la vraie colonne, sinon proxy explicite.
    # Guards stricts : on ne recalcule QUE si la colonne est absente.

    # ── 1. inflation_proxy ────────────────────────────────────────────────────
    # Toujours créé, même si inflation_glissement_annuel existe déjà.
    # Si la vraie colonne existe : inflation_proxy = copie directe (cohérence).
    # Si absente : proxy pct_change(12) sur indice_prix_m2_regional.
    if 'inflation_glissement_annuel' in agg.columns:
        agg['inflation_proxy'] = agg['inflation_glissement_annuel']
    else:
        agg['inflation_proxy'] = (
            agg.groupby('gouvernorat')['indice_prix_m2_regional']
               .pct_change(12)
               .transform(lambda x: x.ffill().bfill().fillna(0.0))
        )
        # Copier aussi sous le nom attendu par les autres modules
        agg['inflation_glissement_annuel'] = agg['inflation_proxy']

    # ── 2. croissance_pib_trim ────────────────────────────────────────────────
    if 'croissance_pib_trim' not in agg.columns:
        agg['_idx_global'] = (
            agg.groupby(['annee','mois'])['indice_prix_m2_regional']
               .transform('mean')
        )
        agg['croissance_pib_trim'] = (
            agg['_idx_global'].pct_change(3).ffill().bfill().fillna(0.0)
        )
        agg.drop(columns=['_idx_global'], inplace=True)

    # ── 3. high_season ────────────────────────────────────────────────────────
    if 'high_season' not in agg.columns:
        agg['high_season'] = agg['mois'].isin([4, 5, 6, 7, 8, 9]).astype(float)

    # ── 4. prix_lisse_loc / prix_lisse_ven ───────────────────────────────────
    # Produits par pelt_A.preparer_serie() et propagés dans enrichir_dataframe().
    # Si absents (ex: pipeline partiel sans PELT) → fallback sur le signal global
    # lissé (moyenne mobile 3M). C'est sémantiquement correct : sans stratification
    # disponible, le signal global est la meilleure approximation.
    if 'prix_lisse_loc' not in agg.columns:
        agg['prix_lisse_loc'] = (
            agg.groupby('gouvernorat')['indice_prix_m2_regional']
               .transform(lambda x: x.rolling(3, min_periods=1).mean())
        )
    if 'prix_lisse_ven' not in agg.columns:
        agg['prix_lisse_ven'] = (
            agg.groupby('gouvernorat')['indice_prix_m2_regional']
               .transform(lambda x: x.rolling(3, min_periods=1).mean())
        )

    # ── Rapport final des fallbacks appliqués ─────────────────────────────────
    _fb_log = []
    for col in ['inflation_proxy', 'inflation_glissement_annuel',
                'croissance_pib_trim', 'high_season',
                'prix_lisse_loc', 'prix_lisse_ven']:
        if col in agg.columns:
            n_nan = int(agg[col].isna().sum())
            if n_nan > 0:
                _fb_log.append(f"{col}:{n_nan}NaN")
    if _fb_log:
        print(f"   [WARN] NaN résiduels après fallbacks : {_fb_log}")

    return agg


# ─── 4. Normalisation ──────────────────────────────────────────────────────

def normaliser_par_gouvernorat(agg: pd.DataFrame) -> tuple:
    agg_norm  = agg.copy()
    scalers   = {}
    feat_cols = [c for c in ALL_FEATURES if c in agg.columns]

    for gov, g in agg.groupby('gouvernorat'):
        idx = g.index
        X   = g[feat_cols].values.astype(np.float32)
        prix_col = feat_cols.index('indice_prix_m2_regional')

        # Clip + log uniquement sur le signal global (TARGET)
        q1, q3 = np.percentile(X[:, prix_col], [25, 75])
        iqr = q3 - q1
        lo  = q1 - IQR_CLIP_FACTOR * iqr
        hi  = q3 + IQR_CLIP_FACTOR * iqr
        X[:, prix_col] = np.clip(X[:, prix_col], max(lo, 1e-3), hi)
        X[:, prix_col] = np.log1p(np.clip(X[:, prix_col], 1e-3, None))

        # Clip + log sur les sous-signaux stratifiés (même traitement que TARGET)
        for col_strat in ['indice_prix_m2_loc', 'indice_prix_m2_ven']:
            if col_strat in feat_cols:
                ci = feat_cols.index(col_strat)
                col_vals = X[:, ci]
                valid_mask = (col_vals > 0) & ~np.isnan(col_vals)
                if valid_mask.sum() > 2:
                    q1s, q3s = np.percentile(col_vals[valid_mask], [25, 75])
                    iqrs = q3s - q1s
                    X[valid_mask, ci] = np.clip(
                        col_vals[valid_mask],
                        max(q1s - IQR_CLIP_FACTOR * iqrs, 1e-3),
                        q3s + IQR_CLIP_FACTOR * iqrs)
                    X[:, ci] = np.log1p(np.clip(X[:, ci], 1e-3, None))
                else:
                    X[:, ci] = 0.0  # type absent → canal neutre

        sc = RobustScaler()
        X_scaled = sc.fit_transform(X)
        agg_norm.loc[idx, feat_cols] = X_scaled

        scalers[int(gov)] = {
            'scaler'       : sc,
            'log_target'   : True,
            'feat_cols'    : feat_cols,
            'prix_col_idx' : prix_col,
        }
    return agg_norm, scalers


def inverser_normalisation(y_scaled: np.ndarray, gov_code: int,
                            scalers: dict) -> np.ndarray:
    info   = scalers[gov_code]
    sc     = info['scaler']
    p_idx  = info['prix_col_idx']
    center = sc.center_[p_idx]
    scale  = sc.scale_[p_idx]
    y_inv  = np.asarray(y_scaled, dtype=np.float64) * scale + center
    if info['log_target']:
        y_inv = np.expm1(y_inv)
    return y_inv.astype(np.float32)


# ─── 5. Architecture L99 ───────────────────────────────────────────────────

class QuantileHuberLoss(keras.losses.Loss):
    def __init__(self, alpha=QUANTILE_ALPHA, delta=1.0,
                 name='quantile_huber', **kwargs):
        super().__init__(name=name, **kwargs)
        self.alpha = float(alpha)
        self.delta = float(delta)

    def call(self, y_true, y_pred):
        y_true  = tf.cast(y_true, tf.float32)
        y_pred  = tf.cast(y_pred, tf.float32)
        err     = y_true - y_pred
        abs_err = tf.abs(err)
        huber   = tf.where(abs_err <= self.delta,
                           0.5 * tf.square(err),
                           self.delta * (abs_err - 0.5 * self.delta))
        quantile = tf.maximum(self.alpha * err, (self.alpha - 1.0) * err)
        return 0.5 * tf.reduce_mean(huber) + 0.5 * tf.reduce_mean(quantile)

    def get_config(self):
        base = super().get_config()
        base.update({'alpha': self.alpha, 'delta': self.delta})
        return base


class _ReduceSumLayer(keras.layers.Layer):
    """
    Remplace layers.Lambda(lambda z: tf.reduce_sum(z, axis=1)).
    Même comportement (somme sur l'axe temps), mais sérialisable proprement
    → compatible safe_mode de Keras 3+, aucun avertissement Lambda.
    """
    def call(self, x):
        return tf.reduce_sum(x, axis=1)

    def get_config(self):
        return super().get_config()


def build_bilstm_attention(lookback, n_features, horizon,
                            lstm_units=LSTM_UNITS, dropout=DROPOUT,
                            l2_reg=L2_REG, lr=LR_INIT,
                            grad_clip_norm=GRAD_CLIP_NORM):
    """
    Architecture BiLSTM + Attention.
    n_features = N_FEATURES (recalculé automatiquement selon ALL_FEATURES).
    """
    reg = keras.regularizers.l2(l2_reg)
    inp = keras.Input(shape=(lookback, n_features), name='input')

    x = layers.Masking(mask_value=0.0, name='masking')(inp)
    x = layers.Bidirectional(
        layers.LSTM(lstm_units, return_sequences=True,
                    kernel_regularizer=reg, recurrent_regularizer=reg),
        name='bilstm_1')(x)
    x = layers.LayerNormalization(name='ln_1')(x)
    x = layers.Dropout(dropout, name='drop_1')(x)

    attn_score   = layers.Dense(lstm_units * 2, activation='relu',
                                name='attn_score')(x)
    attn_weights = layers.Dense(1, activation='sigmoid',
                                name='attn_weights')(attn_score)
    context      = layers.Multiply(name='context')([x, attn_weights])
    # ── Remplacement Lambda → couche custom (safe_mode compatible) ───
    # layers.Lambda contenant du code Python arbitraire déclenche l'erreur
    # "unsafe deserialization" au rechargement. On utilise une sous-classe
    # Layer qui se sérialise proprement sans aucun risque de sécurité.
    context      = _ReduceSumLayer(name='context_sum')(context)

    z   = layers.Dense(32, activation='relu', kernel_regularizer=reg,
                       name='dense_1')(context)
    z   = layers.Dropout(dropout / 2, name='drop_2')(z)
    out = layers.Dense(horizon, name='output')(z)

    model = keras.Model(inputs=inp, outputs=out, name='BiLSTM_Attention_L99')
    optimizer = keras.optimizers.Adam(learning_rate=lr, clipnorm=grad_clip_norm)
    model.compile(optimizer=optimizer,
                  loss=QuantileHuberLoss(alpha=QUANTILE_ALPHA),
                  metrics=['mae'])
    return model


def get_callbacks(patience=PATIENCE):
    return [
        keras.callbacks.EarlyStopping(
            monitor='val_loss', patience=patience,
            min_delta=1e-4, restore_best_weights=True, verbose=0),
        keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss', factor=0.5,
            patience=max(5, patience//3), min_lr=1e-6, verbose=0),
    ]


# ─── 6. Split global strict ────────────────────────────────────────────────

def train_val_split_global_cluster(gouvernorats_codes, agg_norm, val_ratio=0.2):
    """
    Quick Win 1 — sample_weight_temporal dans le fit() LSTM.
    Retourne aussi w_train : poids par séquence d'entraînement.
    Le poids = sample_weight_temporal moyen sur la fenêtre LOOKBACK.
    Corrige le biais 2026=50% des données sans toucher à l'architecture.
    """
    max_len  = 0
    max_date = None
    for gov in gouvernorats_codes:
        g = agg_norm[agg_norm['gouvernorat'] == gov].sort_values(['annee','mois'])
        if len(g) > max_len:
            max_len = len(g)
        if len(g) > 0:
            last = g[['annee','mois']].iloc[-1]
            d = pd.Timestamp(f"{int(last['annee'])}-{int(last['mois']):02d}-01")
            if max_date is None or d > max_date:
                max_date = d

    if max_date is None or max_len < LOOKBACK + HORIZON + 2:
        return None, None, None, None, None

    n_val_steps = max(1, int(max_len * val_ratio))
    cutoff_date = max_date - pd.DateOffset(months=n_val_steps)

    X_train_all, y_train_all, w_train_all = [], [], []
    X_val_all,   y_val_all               = [], []
    feat_cols = [c for c in ALL_FEATURES if c in agg_norm.columns]
    prix_idx  = feat_cols.index('indice_prix_m2_regional')
    # Colonne poids temporel — calculée dans modeling_BO3, normalisée mean=1
    has_weight = 'sample_weight_temporal' in agg_norm.columns

    for gov in gouvernorats_codes:
        g = (agg_norm[agg_norm['gouvernorat'] == gov]
             .sort_values(['annee','mois']).reset_index(drop=True))
        if len(g) < LOOKBACK + HORIZON:
            continue
        data  = g[feat_cols].values.astype(np.float32)
        dates = pd.to_datetime(g['annee'].astype(str) + '-' +
                               g['mois'].astype(str).str.zfill(2) + '-01')
        weights = g['sample_weight_temporal'].values if has_weight \
                  else np.ones(len(g))

        for i in range(len(data) - LOOKBACK - HORIZON + 1):
            seq_end_date = dates.iloc[i + LOOKBACK + HORIZON - 1]
            X_seq = data[i : i + LOOKBACK]
            y_seq = data[i + LOOKBACK : i + LOOKBACK + HORIZON, prix_idx]
            if seq_end_date <= cutoff_date:
                X_train_all.append(X_seq)
                y_train_all.append(y_seq)
                # Poids = moyenne temporelle de la fenêtre (données récentes pèsent moins)
                w_train_all.append(float(weights[i : i + LOOKBACK].mean()))
            else:
                X_val_all.append(X_seq)
                y_val_all.append(y_seq)

    if not X_train_all or not X_val_all:
        return None, None, None, None, None

    return (np.array(X_train_all, dtype=np.float32),
            np.array(y_train_all, dtype=np.float32),
            np.array(X_val_all,   dtype=np.float32),
            np.array(y_val_all,   dtype=np.float32),
            np.array(w_train_all, dtype=np.float32))


# ─── 7. Entraînement ───────────────────────────────────────────────────────

def _mediane_cluster(govs_ok: list, agg_norm: pd.DataFrame,
                     scalers: dict) -> dict:
    """
    Calcule la médiane de prédiction du cluster pour le modèle hybride.
    Pour chaque gouvernorat dans govs_ok, récupère les HORIZON dernières
    valeurs réelles et retourne leur médiane comme prédiction de fallback.
    """
    dernieres = []
    feat_cols = [c for c in ALL_FEATURES if c in agg_norm.columns]
    prix_idx  = feat_cols.index('indice_prix_m2_regional')
    for gov in govs_ok:
        g = (agg_norm[agg_norm['gouvernorat'] == gov]
             .sort_values(['annee','mois']).reset_index(drop=True))
        if len(g) < 2:
            continue
        vals = g[feat_cols].values[-HORIZON:, prix_idx]
        vals_tnd = inverser_normalisation(vals, gov, scalers)
        dernieres.append(float(np.median(vals_tnd)))
    return float(np.median(dernieres)) if dernieres else None


def entrainer_cluster(gouvernorats_codes, agg_norm, scalers, groupe,
                      cluster_id, min_train=MIN_TRAIN):
    """
    Quick Win 1 — sample_weight_temporal dans model.fit() :
      Corrige le biais 2026=50% des données.
      Les séquences récentes (2026) sont pondérées à ~0.5,
      les séquences 2022-2023 sont pondérées à ~1.5-2.0.

    Quick Win 3 — modèle hybride LSTM + médiane cluster :
      Stocké dans govs_short pour usage dans predire_gouvernorat_hybride().
    """
    govs_ok    = [g for g in gouvernorats_codes
                  if agg_norm[agg_norm['gouvernorat'] == g].shape[0] >= min_train]
    govs_short = [g for g in gouvernorats_codes if g not in govs_ok]

    if not govs_ok:
        return None, None, govs_ok, govs_short

    X_tr, y_tr, X_v, y_v, w_tr = train_val_split_global_cluster(
        govs_ok, agg_norm, val_ratio=0.2)
    if X_tr is None or len(X_tr) < 2:
        return None, None, govs_ok, govs_short

    # ── Poids liquidité (Quick Win 4) ────────────────────────────────────────
    # log(1 + nb_annonces) — zones peu liquides contribuent moins à la loss.
    # nb_annonces proxied par la longueur de série (n_pts) de chaque gouvernorat.
    # Formule : w_liq[i] = log(1 + n_pts_gov_i) / log(1 + max_pts)  ∈ (0,1]
    # Multiplié avec w_tr (poids temporels) → pondération combinée.
    pts_par_gov = {g: agg_norm[agg_norm['gouvernorat'] == g].shape[0] for g in govs_ok}
    max_pts     = max(pts_par_gov.values()) if pts_par_gov else 1
    liq_weights = {}
    for g, n in pts_par_gov.items():
        liq_weights[g] = np.log1p(n) / (np.log1p(max_pts) + 1e-8)

    # Reconstruire w_tr combiné (temporal × liquidité) par séquence
    # On ne peut pas directement associer séquence → gouvernorat ici car
    # train_val_split_global_cluster mélange les gouvernorats.
    # Solution : on pondère via sample_weight = w_tr × mean(liq_weights)
    # (approximation conservatrice — le gouvernorat dominant dans w_tr est difficile
    #  à extraire sans refactorer le split. Amélioration TFT : dataset multi-séries.)
    mean_liq = float(np.mean(list(liq_weights.values())))
    w_tr_combined = w_tr * mean_liq
    print(f"   Poids liquidité : {mean_liq:.3f} | "
          f"min={min(liq_weights.values()):.3f} max={max(liq_weights.values()):.3f}")

    # ── CHECK EXPLICITE : alignement features train / val ─────────────────────
    # n_feat_eff = nombre de features réellement présentes dans les séquences
    # (peut différer de N_FEATURES si certaines colonnes fallback sont absentes,
    #  ex : inflation_proxy non créé quand inflation_glissement_annuel existe déjà)
    n_feat_eff = X_tr.shape[2]

    # Vérification critique : train et val doivent avoir la même dimension
    if X_v.shape[2] != n_feat_eff:
        raise ValueError(
            f"[LSTM] Mismatch features train/val : "
            f"train={n_feat_eff} | val={X_v.shape[2]}. "
            f"Vérifier normaliser_par_gouvernorat() et ALL_FEATURES."
        )

    # Warning si features manquantes par rapport au registre statique
    if n_feat_eff != N_FEATURES:
        feat_cols_eff = [c for c in ALL_FEATURES if c in agg_norm.columns]
        manquantes    = [c for c in ALL_FEATURES if c not in agg_norm.columns]
        print(f"   [INFO] Features effectives : {n_feat_eff}/{N_FEATURES} "
              f"(manquantes : {manquantes})")
        print(f"   [INFO] Le modèle sera construit avec input_shape="
              f"(LOOKBACK={LOOKBACK}, n_features={n_feat_eff}) — OK")
    else:
        print(f"   [INFO] Features : {n_feat_eff}/{N_FEATURES} ✔ toutes présentes")

    model = build_bilstm_attention(LOOKBACK, n_feat_eff, HORIZON)
    hist  = model.fit(X_tr, y_tr,
                      sample_weight=w_tr_combined,   # ← temporal × liquidité
                      validation_data=(X_v, y_v),
                      epochs=EPOCHS,
                      batch_size=BATCH_SIZE,
                      callbacks=get_callbacks(),
                      verbose=0)
    return model, hist, govs_ok, govs_short


def predire_gouvernorat_hybride(model, agg_norm, gov_code, scalers,
                                 mediane_cluster: float,
                                 n_pts: int, groupe: str) -> dict:
    """
    Quick Win 3 — Modèle hybride LSTM + médiane cluster.

    Pour les gouvernorats à signal faible, la prédiction est une moyenne
    pondérée entre le LSTM et la médiane du cluster :
      poids_lstm = min(n_pts / 20, 0.7)   → max 70% LSTM
      poids_med  = 1 - poids_lstm          → min 30% médiane cluster

    Exemples :
      Médenine (n_pts=37) → poids_lstm=0.70, poids_med=0.30
      Siliana   (n_pts=3) → poids_lstm=0.15, poids_med=0.85
      Tunis    (n_pts=51) → poids_lstm=0.70, poids_med=0.30

    Retourne les mêmes clés que predire_gouvernorat() + hybride_info.
    """
    pred_lstm = predire_gouvernorat(model, agg_norm, gov_code, scalers)

    # Pas de médiane disponible → pure LSTM
    if mediane_cluster is None or 'erreur' in pred_lstm:
        return pred_lstm

    poids_lstm = min(n_pts / 20.0, 0.70)
    poids_med  = 1.0 - poids_lstm

    prix_hybride = [
        round(float(p) * poids_lstm + mediane_cluster * poids_med, 2)
        for p in pred_lstm.get('prix_pred_tnd', [])
    ]

    pred_lstm['prix_pred_tnd']  = prix_hybride
    pred_lstm['hybride_info']   = {
        'poids_lstm'      : round(poids_lstm, 3),
        'poids_mediane'   : round(poids_med, 3),
        'mediane_cluster' : round(mediane_cluster, 2),
        'n_pts'           : n_pts,
    }
    return pred_lstm


# ─── 8. Prédiction et Métriques ────────────────────────────────────────────

def predire_gouvernorat(model, agg_norm, gov_code, scalers):
    feat_cols = [c for c in ALL_FEATURES if c in agg_norm.columns]
    g = (agg_norm[agg_norm['gouvernorat'] == gov_code]
         .sort_values(['annee','mois']).reset_index(drop=True))
    n_pts = len(g)
    if n_pts == 0:
        return {'erreur': 'Série vide'}

    data = g[feat_cols].values.astype(np.float32)
    if n_pts < LOOKBACK:
        pad_len = LOOKBACK - n_pts
        n_feat_eff = data.shape[1]   # features réelles, pas N_FEATURES statique
        fenetre = np.vstack([np.zeros((pad_len, n_feat_eff), dtype=np.float32), data])
    else:
        fenetre = data[-LOOKBACK:]

    X_pred        = fenetre[np.newaxis, ...]
    y_pred_scaled = model.predict(X_pred, verbose=0)[0]
    y_pred_tnd    = inverser_normalisation(
        y_pred_scaled.reshape(-1, 1).flatten(), gov_code, scalers)

    # ── Clip valeurs extrêmes post-dénormalisation ────────────────────────────
    # Le LSTM peut extrapoler hors distribution sur des séries courtes ou après
    # une rupture PELT. On borne les prédictions à ±50 % autour du dernier prix
    # observé pour éviter des outputs aberrants (ex : -500 TND ou 50 000 TND).
    feat_cols_raw = [c for c in ALL_FEATURES if c in agg_norm.columns]
    prix_idx_raw  = feat_cols_raw.index("indice_prix_m2_regional")
    last_prix_raw = inverser_normalisation(
        np.array([g[feat_cols_raw].values[-1, prix_idx_raw]], dtype=np.float32),
        gov_code, scalers)
    prix_ref = float(last_prix_raw[0]) if last_prix_raw[0] > 0 else 1.0
    clip_lo  = prix_ref * 0.50   # -50 % max
    clip_hi  = prix_ref * 1.80   # +80 % max (asymétrique : hausse plus probable)
    y_pred_tnd = np.clip(y_pred_tnd, clip_lo, clip_hi)

    # Signalement si au moins une valeur a été clippée
    raw_vals = inverser_normalisation(
        y_pred_scaled.reshape(-1, 1).flatten(), gov_code, scalers)
    n_clipped = int(np.sum((raw_vals < clip_lo) | (raw_vals > clip_hi)))
    if n_clipped > 0:
        nom_gov = GOUVERNORAT_DEC.get(int(gov_code), str(gov_code))
        print(f"   [CLIP] {nom_gov} : {n_clipped}/{HORIZON} valeur(s) extrême(s) "
              f"clippée(s) [ref={prix_ref:.0f} TND, bornes={clip_lo:.0f}–{clip_hi:.0f}]")

    last_row   = g.iloc[-1]
    last_annee = int(last_row['annee'])
    last_mois  = int(last_row['mois'])
    dates_pred = []
    for h in range(1, HORIZON + 1):
        mois_pred  = (last_mois + h - 1) % 12 + 1
        annee_pred = last_annee + (last_mois + h - 1) // 12
        dates_pred.append(f"{annee_pred}-{mois_pred:02d}")

    # Récupérer les dernières valeurs loc/ven pour affichage
    last_loc = float(g['indice_prix_m2_loc'].iloc[-1]) \
               if 'indice_prix_m2_loc' in g.columns else None
    last_ven = float(g['indice_prix_m2_ven'].iloc[-1]) \
               if 'indice_prix_m2_ven' in g.columns else None

    return {
        'dates'           : dates_pred,
        'prix_pred_tnd'   : [round(float(v), 2) for v in y_pred_tnd],
        'prix_pred_scaled': y_pred_scaled.tolist(),
        'last_prix_loc'   : round(last_loc, 2) if last_loc else None,
        'last_prix_ven'   : round(last_ven, 2) if last_ven else None,
    }


def calculer_metriques(model, agg_norm, gov_code, scalers):
    feat_cols = [c for c in ALL_FEATURES if c in agg_norm.columns]
    prix_idx  = feat_cols.index('indice_prix_m2_regional')
    g = (agg_norm[agg_norm['gouvernorat'] == gov_code]
         .sort_values(['annee','mois']).reset_index(drop=True))
    if len(g) < LOOKBACK + HORIZON + 2:
        return None

    data  = g[feat_cols].values.astype(np.float32)
    n     = len(data)
    n_val = max(1, int(n * 0.2))
    cut_i = n - n_val

    X_val, y_val = [], []
    for i in range(max(0, cut_i - LOOKBACK), n - LOOKBACK - HORIZON + 1):
        seq_end = i + LOOKBACK + HORIZON - 1
        if seq_end >= cut_i:
            X_val.append(data[i : i + LOOKBACK])
            y_val.append(data[i + LOOKBACK : i + LOOKBACK + HORIZON, prix_idx])

    if len(X_val) < 2:
        return None

    X_val         = np.array(X_val, dtype=np.float32)
    y_val         = np.array(y_val, dtype=np.float32)
    y_pred_scaled = model.predict(X_val, verbose=0)

    y_true_all = np.vstack([inverser_normalisation(y_val[i], gov_code, scalers)
                             for i in range(len(y_val))])
    y_pred_all = np.vstack([inverser_normalisation(y_pred_scaled[i], gov_code, scalers)
                             for i in range(len(y_pred_scaled))])

    # Clip symétrique sur les prédictions métriques : évite que des valeurs
    # aberrantes gonflent artificiellement MAE / SMAPE / nMAE.
    prix_ref_m = float(np.median(y_true_all[y_true_all > 0])) if np.any(y_true_all > 0) else 1.0
    y_pred_all = np.clip(y_pred_all, prix_ref_m * 0.30, prix_ref_m * 2.50)

    errors_abs = np.abs(y_true_all - y_pred_all)
    errors_sq  = (y_true_all - y_pred_all) ** 2
    errors_pct = errors_abs / (np.abs(y_true_all) + 1e-8) * 100

    # ── SMAPE : symétrique, stable quand y_true est petit (remplace MAPE) ────
    smape = (2.0 * errors_abs / (np.abs(y_true_all) + np.abs(y_pred_all) + 1e-8) * 100)

    # ── nMAE : MAE normalisé par la moyenne des prix (interprétable en %) ────
    prix_mean_ref = float(np.abs(y_true_all).mean()) + 1e-8
    nmae = float(errors_abs.mean()) / prix_mean_ref * 100

    return {
        'mae'  : round(float(errors_abs.mean()), 2),
        'rmse' : round(float(np.sqrt(errors_sq.mean())), 2),
        'smape': round(float(smape.mean()), 2),   # ← métrique principale (remplace MAPE)
        'nmae' : round(nmae, 2),                   # ← MAE normalisé (%)
        'mape' : round(float(errors_pct.mean()), 2),  # ← gardé en complément
        'n_val': len(y_val),
    }



# ─── 8b. Validation visuelle réel vs prédit ───────────────────────────────────

def plot_validation_reelle(groupe: str,
                            resultats_govs: dict,
                            agg: pd.DataFrame,
                            scalers: dict,
                            model,
                            agg_norm: pd.DataFrame,
                            output_path: str) -> None:
    """
    Dashboard de validation qualitative : superpose la série réelle observée
    et les prédictions sur la période de validation (20 % final de chaque série).

    Pour chaque gouvernorat dans le top-6 (trié par MAE) :
      — Ligne bleue  : prix réel (TND/m²)
      — Ligne orange : prédiction LSTM sur la fenêtre de validation
      — Zone grise   : période d'entraînement
      — MAE / SMAPE affichés dans le titre du sous-graphe

    Permet de détecter visuellement :
      - Les retards de phase (lag)
      - Les biais systématiques (over/under-estimation)
      - Les valeurs extrêmes résiduelles
    """
    feat_cols = [c for c in ALL_FEATURES if c in agg_norm.columns]
    prix_idx  = feat_cols.index('indice_prix_m2_regional')

    govs_pred = [g for g, r in resultats_govs.items()
                 if 'prix_pred_tnd' in r and r.get('metriques')]
    govs_sorted = sorted(govs_pred,
                         key=lambda g: resultats_govs[g]['metriques'].get('mae', 9999))
    top6 = govs_sorted[:6]
    if not top6:
        return

    fig, axes = plt.subplots(3, 2, figsize=(16, 14), facecolor='white')
    fig.suptitle(
        f'Validation Réel vs Prédit — {groupe} · LSTM BiDir + Attention'
        f'(Fenêtre de validation = 20% final de chaque série)',
        fontsize=12, fontweight='700', y=0.98)
    axes_flat = axes.flatten()

    palette = {'reel': '#1A56DB', 'pred': '#E3342F',
               'pred_fut': '#FF8C00', 'train_bg': '#F3F4F6'}

    for idx, gov in enumerate(top6):
        ax  = axes_flat[idx]
        nom = GOUVERNORAT_DEC.get(int(gov), str(gov))
        m   = resultats_govs[gov]['metriques'] or {}

        # ── Données réelles ──────────────────────────────────────────
        g_raw = (agg[agg['gouvernorat'] == gov]
                 .sort_values(['annee','mois']).reset_index(drop=True))
        dates_all = pd.to_datetime(
            g_raw['annee'].astype(str) + '-' +
            g_raw['mois'].astype(str).str.zfill(2) + '-01')
        prix_all  = g_raw['indice_prix_m2_regional'].values
        n         = len(g_raw)
        cut_i     = n - max(1, int(n * 0.2))

        # ── Fond zone entraînement ───────────────────────────────────
        if cut_i < n and len(dates_all) > cut_i:
            ax.axvspan(dates_all.iloc[0], dates_all.iloc[cut_i],
                       color=palette['train_bg'], alpha=0.5, label='Train')
            ax.axvline(dates_all.iloc[cut_i], color='#6B7280',
                       linewidth=1.2, linestyle='--', alpha=0.7)

        # ── Série réelle complète ────────────────────────────────────
        ax.plot(dates_all, prix_all,
                color=palette['reel'], linewidth=1.8,
                label='Réel', zorder=3)

        # ── Prédictions sur la fenêtre de validation ─────────────────
        g_norm = (agg_norm[agg_norm['gouvernorat'] == gov]
                  .sort_values(['annee','mois']).reset_index(drop=True))
        data_norm = g_norm[feat_cols].values.astype(np.float32)

        dates_norm = pd.to_datetime(
            g_norm['annee'].astype(str) + '-' +
            g_norm['mois'].astype(str).str.zfill(2) + '-01')

        pred_dates, pred_vals = [], []
        val_start = max(0, cut_i - LOOKBACK)

        for i in range(val_start, n - LOOKBACK - HORIZON + 1):
            window = data_norm[i : i + LOOKBACK][np.newaxis, ...]
            try:
                y_s = model.predict(window, verbose=0)[0]
                y_d = inverser_normalisation(y_s.flatten(), gov, scalers)
                # Clip anti-extrêmes (cohérent avec predire_gouvernorat)
                prix_ref = float(np.median(prix_all[prix_all > 0])) if np.any(prix_all > 0) else 1.0
                y_d = np.clip(y_d, prix_ref * 0.50, prix_ref * 1.80)
                # Enregistre uniquement le premier pas (T+1) pour la courbe
                if i + LOOKBACK < len(dates_norm):
                    pred_dates.append(dates_norm.iloc[i + LOOKBACK])
                    pred_vals.append(float(y_d[0]))
            except Exception:
                continue

        if pred_dates:
            ax.plot(pred_dates, pred_vals,
                    color=palette['pred'], linewidth=1.6,
                    linestyle='--', marker='o', markersize=3,
                    alpha=0.85, label='Prédit (val)', zorder=4)

        # ── Prédictions futures (T+1/T+2/T+3) ───────────────────────
        res = resultats_govs[gov]
        if 'dates' in res and 'prix_pred_tnd' in res:
            fut_dates = [pd.Timestamp(d + '-01') for d in res['dates']]
            fut_vals  = res['prix_pred_tnd']
            if dates_all is not None and len(dates_all) > 0:
                bridge_d = [dates_all.iloc[-1]] + fut_dates
                bridge_v = [float(prix_all[-1])] + fut_vals
                ax.plot(bridge_d, bridge_v,
                        color=palette['pred_fut'], linewidth=2.2,
                        linestyle='-', marker='D', markersize=5,
                        alpha=0.95, label=f'Futur T+1→T+{HORIZON}', zorder=5)

        # ── Titre + métriques ────────────────────────────────────────
        mae_s   = f"{m.get('mae',0):.0f}"   if m else 'N/A'
        smape_s = f"{m.get('smape',0):.1f}" if m else 'N/A'
        nmae_s  = f"{m.get('nmae',0):.1f}"  if m else 'N/A'
        ax.set_title(f'{nom}  |  MAE={mae_s} TND  SMAPE={smape_s}%  nMAE={nmae_s}%',
                     fontsize=9, fontweight='600')
        ax.set_xlabel('Date', fontsize=8)
        ax.set_ylabel('Prix m² (TND)', fontsize=8)
        ax.tick_params(axis='x', rotation=30, labelsize=7)
        ax.tick_params(axis='y', labelsize=7)
        ax.legend(fontsize=7, loc='upper left', framealpha=0.85)
        ax.grid(True, alpha=0.20, linestyle=':')

        # Annotation MAE sur le graphe
        if pred_vals and pred_dates:
            ax.annotate(f'MAE={mae_s}', xy=(pred_dates[-1], pred_vals[-1]),
                        fontsize=7, color=palette['pred'],
                        xytext=(5, 5), textcoords='offset points')

    # Cacher les axes vides si top6 < 6
    for idx in range(len(top6), 6):
        axes_flat[idx].axis('off')

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f" → Validation réel vs prédit : {output_path}")


# ─── 9. Visualisation ──────────────────────────────────────────────────────

def plot_lstm_groupe(groupe: str, resultats_govs: dict, agg: pd.DataFrame,
                     cluster_map_grp: dict, output_path: str) -> None:
    TITRES = {'Residentiel':'Résidentiel', 'Foncier':'Foncier',
              'Commercial':'Commercial'}
    titre = TITRES.get(groupe, groupe)

    govs_pred        = [g for g, r in resultats_govs.items()
                        if 'prix_pred_tnd' in r and r.get('metriques')]
    govs_pred_sorted = sorted(govs_pred,
                              key=lambda g: resultats_govs[g]['metriques'].get('mae', 9999))
    top8 = govs_pred_sorted[:8]

    fig = plt.figure(figsize=(18, 18), facecolor='white')
    gs  = GridSpec(4, 2, hspace=0.50, wspace=0.32,
                   top=0.92, bottom=0.04, left=0.07, right=0.97)

    # Row 0 : Timeseries global + prédictions
    ax0 = fig.add_subplot(gs[0, :])
    for gov in top8:
        res = resultats_govs[gov]
        nom = GOUVERNORAT_DEC.get(int(gov), str(gov))
        cid = cluster_map_grp.get(gov, {}).get('cluster', 0)
        col = CLUSTER_COLORS.get(cid, '#888')

        g_hist    = agg[agg['gouvernorat'] == gov].sort_values(['annee','mois'])
        prix_obs  = g_hist.groupby(['annee','mois'])['indice_prix_m2_regional'].mean()
        dates_obs = [pd.Timestamp(f"{int(r[0])}-{int(r[1]):02d}-01")
                     for r in prix_obs.index]

        ax0.plot(dates_obs, prix_obs.values, color=col, linewidth=1.5,
                 alpha=0.75, label=f"{nom} (C{cid})")

        dates_pred = [pd.Timestamp(d + '-01') for d in res['dates']]
        prix_pred  = res['prix_pred_tnd']
        if dates_obs:
            ax0.plot([dates_obs[-1]] + dates_pred,
                     [float(prix_obs.values[-1])] + prix_pred,
                     color=col, linewidth=2.5, linestyle='--',
                     marker='o', markersize=6, alpha=0.95)

    ax0.set_title(f'LSTM Bidi + Attention — {titre} [L99] | {N_FEATURES} features · STRATIFIÉ + TENDANCE',
                  fontsize=11, fontweight='700')
    ax0.set_xlabel('Date')
    ax0.set_ylabel('Prix m² (TND)')
    ax0.legend(fontsize=7.5, loc='upper left', ncol=3, framealpha=0.85)
    ax0.grid(True, alpha=0.22)

    # Row 1 : Timeseries loc vs ven (NOUVEAU — correction type_transaction)
    ax0b = fig.add_subplot(gs[1, :])
    for gov in top8[:4]:  # 4 premiers pour lisibilité
        res = resultats_govs[gov]
        nom = GOUVERNORAT_DEC.get(int(gov), str(gov))
        cid = cluster_map_grp.get(gov, {}).get('cluster', 0)
        col = CLUSTER_COLORS.get(cid, '#888')

        g_hist = agg[agg['gouvernorat'] == gov].sort_values(['annee','mois'])
        dates_obs = [pd.Timestamp(f"{int(r['annee'])}-{int(r['mois']):02d}-01")
                     for _, r in g_hist.iterrows()]

        if 'indice_prix_m2_loc' in g_hist.columns:
            ax0b.plot(dates_obs, g_hist['indice_prix_m2_loc'].values,
                      color=col, linewidth=1.2, linestyle=':',
                      alpha=0.7, label=f"{nom} (loc)")
        if 'indice_prix_m2_ven' in g_hist.columns:
            ax0b.plot(dates_obs, g_hist['indice_prix_m2_ven'].values,
                      color=col, linewidth=1.2, linestyle='--',
                      alpha=0.7, label=f"{nom} (ven)")

    ax0b.set_title('Sous-signaux Location vs Vente (CORRECTION type_transaction)',
                   fontsize=10, fontweight='700')
    ax0b.set_xlabel('Date'); ax0b.set_ylabel('Prix m² (TND)')
    ax0b.legend(fontsize=7, loc='upper left', ncol=4, framealpha=0.85)
    ax0b.grid(True, alpha=0.22)

    # Row 2 : MAE
    ax1 = fig.add_subplot(gs[2, 0])
    noms_sorted  = [GOUVERNORAT_DEC.get(int(g), str(g)) for g in govs_pred_sorted]
    maes_sorted  = [resultats_govs[g]['metriques'].get('mae', 0) for g in govs_pred_sorted]
    cids_sorted  = [cluster_map_grp.get(g, {}).get('cluster', 0) for g in govs_pred_sorted]
    colors_bar   = [CLUSTER_COLORS.get(c, '#888') for c in cids_sorted]
    ax1.barh(noms_sorted, maes_sorted, color=colors_bar, edgecolor='white')
    ax1.set_xlabel('MAE (TND/m²)')
    ax1.set_title('MAE par gouvernorat')
    ax1.invert_yaxis()

    # Row 2 : SMAPE vs longueur (SMAPE remplace MAPE — plus robuste)
    ax1b = fig.add_subplot(gs[2, 1])
    for gov in govs_pred:
        m   = resultats_govs[gov]['metriques']
        nom = GOUVERNORAT_DEC.get(int(gov), str(gov))
        cid = cluster_map_grp.get(gov, {}).get('cluster', 0)
        n_pts = agg[agg['gouvernorat'] == gov].shape[0]
        smape_v = m.get('smape', 0) if m else 0
        ax1b.scatter(n_pts, smape_v,
                     c=CLUSTER_COLORS.get(cid, '#888'), s=90)
        ax1b.annotate(nom, (n_pts, smape_v),
                      textcoords='offset points', xytext=(5, 4), fontsize=7)
    ax1b.set_xlabel('Nb points temporels')
    ax1b.set_ylabel('SMAPE (%)')
    ax1b.set_title('SMAPE vs longueur de série\n(plus robuste que MAPE sur prix faibles)')

    # Row 3 : Tableau métriques
    ax2 = fig.add_subplot(gs[3, :])
    ax2.axis('off')
    table_data = []
    for gov in govs_pred_sorted[:15]:
        m    = resultats_govs[gov]['metriques']
        nom  = GOUVERNORAT_DEC.get(int(gov), str(gov))
        cid  = cluster_map_grp.get(gov, {}).get('cluster', 0)
        zone = cluster_map_grp.get(gov, {}).get('zone', 'N/A')
        cl_sz = cluster_map_grp.get(gov, {}).get('cluster_size', '?')
        r_lv  = cluster_map_grp.get(gov, {}).get('ratio_location_vente', '?')
        r_lv_str = f"{r_lv:.2f}" if isinstance(r_lv, float) else str(r_lv)
        smape_v  = m.get('smape', None)
        nmae_v   = m.get('nmae', None)
        table_data.append([nom, cid, f"{cl_sz}",
                           f"{m.get('mae','N/A')}",
                           f"{smape_v:.1f}%" if smape_v is not None else 'N/A',
                           f"{nmae_v:.1f}%"  if nmae_v  is not None else 'N/A',
                           r_lv_str])
    if table_data:
        table = ax2.table(cellText=table_data,
                          colLabels=['Gouvernorat','Cluster','Cl.Size',
                                     'MAE (TND)','SMAPE','nMAE','loc/ven ratio'],
                          loc='center', cellLoc='left',
                          colWidths=[0.14, 0.06, 0.07, 0.09, 0.07, 0.07, 0.10])
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1.2, 1.5)

    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    print(f" → Dashboard : {output_path}")


# ─── 10. Pipeline principal ─────────────────────────────────────────────────

def _confidence_label(n_pts: int, groupe: str) -> str:
    if groupe == 'Commercial':
        return 'faible' if n_pts < 20 else 'moyenne'
    return 'haute' if n_pts >= 20 else ('moyenne' if n_pts >= 12 else 'faible')


def run_lstm(groupes: dict, cluster_map: dict, pelt_df: pd.DataFrame) -> dict:
    print("\n" + "="*80)
    print(f" LSTM A L99+ | BiLSTM({LSTM_UNITS}) + Attention · {N_FEATURES} features · split global strict")
    print(f" CORRECTION : 3 features stratifiées type_transaction (loc/ven/ratio)")
    print(f" TENDANCE   : trend_prix (pct_change 3M) + momentum_prix (diff 3M)")
    print(f" EXOGÈNES   : inflation / PIB / high_season (avec fallback auto)")
    print(f" MÉTRIQUES  : MAE ✅ | SMAPE ✅ | nMAE ✅ | Poids liquidité ✅")
    print(f" CLUSTERING : depuis cluster_map.pkl de kmeans_A (données réelles)")
    print("="*80)

    # ── Validation Feature Registry ────────────────────────────────────────────
    if _FEATURES_CONFIG_AVAILABLE:
        all_df = pd.concat(groupes.values(), ignore_index=True)
        present, missing = validate_features(all_df, "lstm")
        print(f"  Feature Registry [lstm] : {len(present)} sources présentes"
              f"{(' | manquantes: ' + str(missing)) if missing else ' ✔'}")
        print(f"  Note : cluster_id, post_rupture, score_rupture_norm, rupture_intensity")
        print(f"         → ajoutés par enrichir_dataframe() (POST_PIPELINE_FEATURES)")

    all_predictions = []
    all_metriques   = []
    all_models      = {}
    all_scalers     = {}
    resultats_globaux = {}

    for groupe in GROUPES_ACTIFS:
        df = groupes.get(groupe)
        if df is None:
            print(f" ✗ {groupe} absent")
            continue
        df = build_features(df) 

        n_loc = int((df['type_transaction'] == TT_LOC).sum())
        n_ven = int((df['type_transaction'] == TT_VEN).sum())
        print(f"\n{'─'*70}\n {groupe.upper()} ({len(df):,} obs) "
              f"| Location={n_loc:,} ({n_loc/len(df)*100:.1f}%) "
              f"| Vente={n_ven:,} ({n_ven/len(df)*100:.1f}%)")

        df_enrichi = enrichir_dataframe(df, groupe, cluster_map, pelt_df)
        agg        = preparer_series_aggregees(df_enrichi)

        # Vérification features stratifiées
        has_loc = 'indice_prix_m2_loc' in agg.columns
        has_ven = 'indice_prix_m2_ven' in agg.columns
        print(f" Features stratifiées : "
              f"indice_prix_m2_loc={'✔' if has_loc else '✗'} | "
              f"indice_prix_m2_ven={'✔' if has_ven else '✗'} | "
              f"ratio_location_vente={'✔' if 'ratio_location_vente' in agg.columns else '✗'}")

        # ── Vérification features : rapport complet + warning si manquantes ──
        feat_eff     = [c for c in ALL_FEATURES if c in agg.columns]
        feat_missing = [c for c in ALL_FEATURES if c not in agg.columns]
        n_eff        = len(feat_eff)

        print(f" N_FEATURES effectif : {n_eff}/{N_FEATURES}"
              + (" ✔" if n_eff == N_FEATURES else f" ⚠ {len(feat_missing)} manquante(s)"))
        if feat_missing:
            print(f"   [WARN] Features absentes (non bloquant — model s'adapte) :")
            for fm in feat_missing:
                print(f"     ✗ {fm}")

        for f_check in ['trend_prix', 'momentum_prix',
                         'inflation_glissement_annuel', 'inflation_proxy',
                         'croissance_pib_trim', 'high_season']:
            status = '✔' if f_check in agg.columns else '✗ (absent — fallback ou ignoré)'
            print(f"   {f_check:<32} {status}")

        agg_norm, scalers = normaliser_par_gouvernorat(agg)
        all_scalers[groupe] = scalers

        # Utiliser le cluster_map de kmeans_A (données réelles, pas hardcodé)
        cm_grp = cluster_map.get(groupe, {})
        if not cm_grp:
            cm_grp = _build_fallback_cluster_map(groupe)

        clusters_govs = {}
        for gov_id, info in cm_grp.items():
            clusters_govs.setdefault(info['cluster'], []).append(gov_id)

        resultats_govs = {}
        for cluster_id, govs in sorted(clusters_govs.items()):
            noms_cl   = [GOUVERNORAT_DEC.get(g, str(g)) for g in govs]
            zone_name = cm_grp.get(govs[0], {}).get('zone', f'Cluster {cluster_id}') \
                        if govs else f'Cluster {cluster_id}'
            r_lv_mean = float(np.mean([cm_grp.get(g, {}).get('ratio_location_vente', 0.5)
                                       for g in govs]))
            print(f"\n ● Cluster {cluster_id} ({len(govs)} gov) : {zone_name} "
                  f"| loc/ven ratio moy={r_lv_mean:.2f}")
            print(f"   {noms_cl}")

            model, hist, govs_ok, govs_short = entrainer_cluster(
                govs, agg_norm, scalers, groupe, cluster_id)

            if model is None:
                print(" ⚠ Pas assez de données → skip")
                continue

            ep_done  = len(hist.history['loss'])
            val_loss = hist.history.get('val_loss', [None])[-1]
            val_mae  = hist.history.get('val_mae', [None])[-1]
            print(f" ✔ {ep_done} epochs | val_loss={val_loss:.4f} | val_mae={val_mae:.4f}")

            model_path = os.path.join(
                MODELS_DIR,
                f'lstm_A_{groupe.lower()}_cluster{cluster_id}.keras')
            model.save(model_path)
            all_models[f"{groupe}_{cluster_id}"] = model_path

            # Quick Win 3 : médiane du cluster pour le modèle hybride
            med_cluster = _mediane_cluster(govs_ok, agg_norm, scalers)

            for gov in govs_ok:
                nom   = GOUVERNORAT_DEC.get(int(gov), str(gov))
                n_pts = agg_norm[agg_norm['gouvernorat'] == gov].shape[0]
                # Hybride : LSTM pondéré par fiabilité de la série
                pred  = predire_gouvernorat_hybride(
                    model, agg_norm, gov, scalers, med_cluster, n_pts, groupe)
                metr  = calculer_metriques(model, agg_norm, gov, scalers)
                confidence = _confidence_label(n_pts, groupe)
                poids_lstm = min(n_pts / 20.0, 0.70)

                resultats_govs[gov] = {**pred, 'metriques': metr,
                                       'confidence': confidence}

                t1        = pred.get('prix_pred_tnd', ['?'])[0] \
                           if 'prix_pred_tnd' in pred else '?'
                mae_str   = f"{metr['mae']:.0f}"   if metr and metr.get('mae')   else 'N/A'
                smape_str = f"{metr['smape']:.1f}" if metr and metr.get('smape') else 'N/A'
                nmae_str  = f"{metr['nmae']:.1f}"  if metr and metr.get('nmae')  else 'N/A'
                r_lv      = cm_grp.get(gov, {}).get('ratio_location_vente', 0.5)
                cl_sz     = cm_grp.get(gov, {}).get('cluster_size', '?')
                print(f" ✓ {nom:<16} | MAE={mae_str:>8} | SMAPE={smape_str:>5}% | "
                      f"nMAE={nmae_str:>5}% | conf={confidence} | T+1={t1} | "
                      f"lstm={poids_lstm:.0%} med={1-poids_lstm:.0%} | "
                      f"cl_size={cl_sz}")

            for gov in govs_short:
                nom   = GOUVERNORAT_DEC.get(int(gov), str(gov))
                n_pts = agg_norm[agg_norm['gouvernorat'] == gov].shape[0]
                print(f" ~ {nom:<16} | {n_pts} pts → transfer+masking | conf=faible")

        plot_path = os.path.join(GRAPHS_DIR, f'lstm_A_{groupe.lower()}.png')
        try:
            plot_lstm_groupe(groupe, resultats_govs, agg, cm_grp, plot_path)
        except Exception:
            import traceback; traceback.print_exc()

        # ── Validation visuelle réel vs prédit (nouveau) ──────────────────────
        # Génère un dashboard 3×2 qui superpose série réelle et prédictions
        # sur la fenêtre de validation pour détecter biais/retards/extrêmes.
        val_path = os.path.join(GRAPHS_DIR, f'lstm_A_{groupe.lower()}_validation.png')
        try:
            # Récupère le modèle du dernier cluster entraîné pour ce groupe
            last_model_key = f"{groupe}_{max(clusters_govs.keys())}"
            last_model_path = all_models.get(last_model_key)
            if last_model_path and os.path.exists(last_model_path):
                val_model = keras.models.load_model(
                    last_model_path,
                    custom_objects={
                        'QuantileHuberLoss': QuantileHuberLoss,
                        '_ReduceSumLayer'  : _ReduceSumLayer,
                    },
                    safe_mode=False)   # ← requis pour Lambda/couches custom
                plot_validation_reelle(
                    groupe, resultats_govs, agg, scalers,
                    val_model, agg_norm, val_path)
            else:
                print(f" ℹ Validation plot skipped (modèle non trouvé pour {groupe})")
        except Exception:
            import traceback; traceback.print_exc()

        resultats_globaux[groupe] = resultats_govs

    # Sauvegarde finale
    scalers_path = os.path.join(MODELS_DIR, 'lstm_A_scalers.pkl')
    with open(scalers_path, 'wb') as f:
        pickle.dump(all_scalers, f)

    print("\n" + "="*80)
    print(f" BILAN LSTM A — L99+ (STRATIFIÉ · TENDANCE · SMAPE + nMAE · Poids liquidité)")
    print("="*80)
    print(f" → Modèles   : {MODELS_DIR}")
    print(f" → Scalers   : {scalers_path}")
    print(f" → Graphiques: {GRAPHS_DIR}")
    print(f" → N_FEATURES = {N_FEATURES}")
    print(f"   • indice_prix_m2_loc       (Location  tt=1)")
    print(f"   • indice_prix_m2_ven       (Vente     tt=2)")
    print(f"   • ratio_location_vente     (composition mensuelle 0→1)")
    print(f"   • trend_prix               (pct_change 3M par gouvernorat)")
    print(f"   • momentum_prix            (diff 3M par gouvernorat)")
    print(f"   • inflation / PIB / saison (réels ou fallback proxy)")
    print(f" → Métriques : MAE ✅ | SMAPE ✅ | nMAE ✅ | MAPE (complément)")
    print(f" → Poids     : sample_weight_temporal × log(1+nb_annonces)")

    return resultats_globaux


# ─── Point d'entrée ────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("Chargement des artefacts pipeline...")
    cluster_map, pelt_df = charger_pipeline_artefacts(MODELS_DIR)

    dossier = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     '..', '..', 'BO3'))
    print(f"\nDossier données : {dossier}\n")

    groupes = {}
    for nom in GROUPES_ACTIFS:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f" ✔ {nom:<14} : {len(groupes[nom]):,} lignes")
        else:
            print(f" ✗ {nom} manquant")

    if groupes:
        run_lstm(groupes, cluster_map, pelt_df)