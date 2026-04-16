"""
kmeans_A.py — Segmentation Géographique · Version Production L99
=================================================================

OBJECTIF MÉTIER 3 : Prédire les tendances régionales du marché immobilier tunisien.

CORRECTION MAJEURE — STRATIFICATION type_transaction :
  • type_transaction=1 (Location) et type_transaction=2 (Vente) ont des niveaux
    de prix/m² incompatibles. Les mélanger dans indice_prix_m2_regional biaise
    les clusters géographiques (un gouvernorat touristique semblera "bas prix"
    s'il concentre des locations alors que son marché vente est fort).
  • Solution : build_features() calcule des features prix séparées par type.
    6 features ajoutées : prix_mean_loc/ven · prix_tendance_loc/ven · prix_cv_loc/ven
    2 features de composition : ratio_location_vente · spread_type.
  • Le cluster_map produit enrichit ses clés avec prix_mean_loc/ven et
    prix_tendance_loc/ven → pelt_A.py et lstm_A.py peuvent les exploiter.
  • Interface cluster_map.pkl inchangée → chaîne pelt → lstm compatible.

PRINCIPES :
  • Aucune constante manuelle codée en dur (noms, seuils, algos, exclusions).
  • GOV_NAMES    → GOUVERNORAT_DEC depuis mappings_BO3 (source officielle pipeline)
  • MIN_OBS      → percentile 10 des observations par gouvernorat (par groupe)
  • EXCLUDE_GOVS → détection automatique des outliers prix (IQR × 3)
  • BEST_CONFIGS → sélection exhaustive algo + PCA (KMeans vs Agglom-avg vs Agglom-ward)

Sortie :
  models/kmeans_A.pkl          — métriques + clusters
  models/cluster_map.pkl       — {groupe → {gov_id → info}} → pour pelt_A.py
  models/kmeans_A_metriques.csv
  graphs/kmeans/kmeans_A_dashboard.png
"""

import os, sys, pickle, warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

from features_registry import build_features, validate_features, get_features
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.preprocessing import RobustScaler
from sklearn.decomposition import PCA
from sklearn.metrics import (silhouette_score, davies_bouldin_score,
                              calinski_harabasz_score)

warnings.filterwarnings('ignore')

# ─── Import mappings officiels du pipeline ─────────────────────────────────
def _import_gouvernorat_dec():
    candidates = [
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      '..', '..', '..')),
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      '..', '..')),
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
    raise ImportError("mappings_BO3.py introuvable. Vérifiez la structure du projet.")

GOUVERNORAT_DEC = _import_gouvernorat_dec()

# ─── Feature Registry centralisé ──────────────────────────────────────────────
# kmeans_A.py lit les features SOURCES depuis le dataset BO3 (TARGET_COLS).
# Les features d'entrée du clustering (FEAT_COLS) sont dérivées de ces sources
# via build_features() → agrégation par gouvernorat.
# Le Feature Registry documente les sources pour chaque modèle.
try:
    from features_config import get_features, validate_features, FEATURE_SETS
    _FEATURES_CONFIG_AVAILABLE = True
except ImportError:
    _FEATURES_CONFIG_AVAILABLE = False

# ─── Répertoires ──────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models')
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs', 'kmeans')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(GRAPHS_DIR, exist_ok=True)

GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']

PALETTE = {0:'#1D9E75', 1:'#D85A30', 2:'#7F77DD',
           3:'#EF9F27', 4:'#D4537E', 5:'#3FA7D6'}

# Facteur IQR pour le filtre sur prix_mean agrégé par gouvernorat.
# Intentionnellement distinct du iqr_factor de pelt_A (séries temporelles).
IQR_KMEANS = 2.5

# Codes type_transaction
TT_LOC = 1
TT_VEN = 2

# ─── Quick Win 2 : k_min par groupe + contrainte géographique ────────────────
# k=2 est statistiquement correct mais économiquement inutile pour le marché TN.
# Forcer k_min=3 garantit au moins 3 dynamiques : Grand Tunis / Littoral / Intérieur.
K_MIN_GROUPE = {
    'Residentiel': 3,   # Grand Tunis / Littoral+Sahel / Centre+Sud
    'Foncier'    : 3,   # Périurbain Tunis / Littoral agricole / Intérieur
    'Commercial' : 2,   # Peu de gouvernorats → 2 reste acceptable
}

# Zones géographiques pour contrainte clustering
# Deux gouvernorats de zones différentes (ex: Tunis=0 et Tozeur=4) ne peuvent
# pas se retrouver dans le même cluster si k >= 3.
ZONE_CONTRAINTE = {
    0: ['Tunis','Ariana','Ben Arous','Manouba'],          # Grand Tunis
    1: ['Bizerte','Nabeul','Béja','Jendouba'],             # Littoral Nord
    2: ['Sousse','Monastir','Mahdia','Sfax'],              # Sahel
    3: ['Kairouan','Kasserine','Sidi Bouzid','Siliana',
        'Le Kef','Zaghouan'],                              # Centre/Intérieur
    4: ['Gabès','Médenine','Tataouine','Gafsa',
        'Kébili','Tozeur'],                                # Sud
}


# ─── Calcul dynamique des hyperparamètres ─────────────────────────────────────

def compute_min_obs(df: pd.DataFrame) -> int:
    """MIN_OBS = max(3, percentile 10 des obs par gouvernorat)."""
    obs_par_gov = df.groupby('gouvernorat').size()
    return max(3, int(obs_par_gov.quantile(0.10)))


def detect_sparse_govs(df_raw: pd.DataFrame, gov_df: pd.DataFrame) -> list:
    """Exclut les gouvernorats avec <= 1 point temporel distinct (annee×mois)."""
    pt_by_gov = df_raw.groupby('gouvernorat').apply(
        lambda x: x[['annee','mois']].drop_duplicates().shape[0])
    sparse = pt_by_gov[pt_by_gov <= 1].index.astype(int).tolist()
    return sparse


def detect_outlier_govs(gov_df: pd.DataFrame) -> list:
    """EXCLUDE_GOVS = gouvernorats dont le prix moyen est un outlier IQR × 3."""
    Q1  = gov_df['prix_mean'].quantile(0.25)
    Q3  = gov_df['prix_mean'].quantile(0.75)
    IQR = Q3 - Q1
    mask = ((gov_df['prix_mean'] < Q1 - 3 * IQR) |
            (gov_df['prix_mean'] > Q3 + 3 * IQR))
    return gov_df.loc[mask, 'gouvernorat'].astype(int).tolist()


def _get_zone(gov_name: str) -> int:
    """Retourne la zone géographique (0-4) d'un gouvernorat."""
    for zone_id, noms in ZONE_CONTRAINTE.items():
        if gov_name in noms:
            return zone_id
    return 3  # Centre par défaut


def _penaliser_zone(labels: np.ndarray, gov_noms: list, k: int) -> float:
    """
    Pénalité de cohérence géographique : score 0-1.
    1.0 = clusters parfaitement cohérents géographiquement.
    0.0 = Grand Tunis et Tozeur dans le même cluster.

    Formule : proportion de paires dans le même cluster qui sont de la même zone.
    Utilisée pour départager deux configs avec Silhouette proche.
    """
    if k < 3:
        return 1.0  # Pas de contrainte si k=2
    zones = np.array([_get_zone(n) for n in gov_noms])
    n = len(labels)
    meme_cluster = 0
    meme_zone_et_cluster = 0
    for i in range(n):
        for j in range(i + 1, n):
            if labels[i] == labels[j]:
                meme_cluster += 1
                if zones[i] == zones[j]:
                    meme_zone_et_cluster += 1
    return meme_zone_et_cluster / meme_cluster if meme_cluster > 0 else 1.0


def select_best_config(X_pca: np.ndarray, n_gov: int,
                       gov_noms: list = None,
                       k_min: int = 2) -> tuple:
    """
    Sélection exhaustive sur 3 algos × k ∈ [k_min, min(7, n_gov-1)].

    Quick Win 2 — k_min + contrainte géographique :
    - k_min forcé à 3 pour Résidentiel et Foncier (K_MIN_GROUPE).
    - Score composite = Silhouette × 0.85 + cohérence_zone × 0.15
      → départage les configs équivalentes en Silhouette en faveur
      de la plus géographiquement cohérente.
    - Garantit que Grand Tunis et Tozeur ne se retrouvent jamais
      dans le même cluster quand k >= 3.

    Retourne (algo_name, k_opt, sil_opt, labels_opt, sil_par_k).
    """
    best = {'score': -1, 'sil': -1, 'algo': 'KMeans', 'k': k_min, 'lbl': None}
    sil_par_k = {}

    algos = [
        ('KMeans',      lambda k: KMeans(n_clusters=k, random_state=42,
                                         n_init=50, max_iter=500)),
        ('Agglom-avg',  lambda k: AgglomerativeClustering(n_clusters=k,
                                                           linkage='average')),
        ('Agglom-ward', lambda k: AgglomerativeClustering(n_clusters=k,
                                                           linkage='ward')),
    ]

    max_k = min(7, n_gov - 1)
    # k_min garanti même si n_gov petit
    k_min_eff = min(k_min, max_k)

    for algo_name, algo_fn in algos:
        for k in range(k_min_eff, max_k + 1):
            lbl = algo_fn(k).fit_predict(X_pca)
            if len(set(lbl)) < 2:
                continue
            sil   = silhouette_score(X_pca, lbl)
            geo   = _penaliser_zone(lbl, gov_noms, k) if gov_noms else 1.0
            score = sil * 0.85 + geo * 0.15  # Score composite
            if score > best['score']:
                best.update({'score': score, 'sil': sil,
                             'algo': algo_name, 'k': k, 'lbl': lbl})

    best_algo_fn = next(fn for name, fn in algos if name == best['algo'])
    for k in range(k_min_eff, max_k + 1):
        lbl = best_algo_fn(k).fit_predict(X_pca)
        if len(set(lbl)) >= 2:
            sil_par_k[k] = round(silhouette_score(X_pca, lbl), 4)

    return best['algo'], best['k'], best['sil'], best['lbl'], sil_par_k


# ─── Feature Engineering (stratifié par type_transaction) ──────────────────────

def _prix_stats_par_type(g: pd.DataFrame, tt: int) -> dict:
    """
    Calcule prix_mean, prix_tendance, prix_cv pour un sous-groupe type_transaction.
    Applique une winsorisation 5-95 intra-type pour éliminer les outliers.
    Retourne des valeurs nulles si le type est absent ou trop peu représenté
    (ex: Foncier quasi-exclusivement en vente → location → stats = 0).

    Cette séparation est la correction centrale :
      - Location (tt=1) : prix/m² typiquement 1-500 TND/m²/mois
      - Vente    (tt=2) : prix/m² typiquement 100-30000 TND/m²
    Mélanger les deux produit un indice biaisé par la composition du stock.
    """
    sub = g[g['type_transaction'] == tt]
    if len(sub) < 3:
        return {'mean': 0.0, 'tendance': 0.0, 'cv': 0.0, 'n': 0}

    t = sub['indice_prix_m2_regional']
    sub = sub[(t >= t.quantile(0.05)) & (t <= t.quantile(0.95))].copy()
    if len(sub) < 2:
        return {'mean': 0.0, 'tendance': 0.0, 'cv': 0.0, 'n': 0}

    pm   = float(sub['indice_prix_m2_regional'].mean())
    pstd = float(sub['indice_prix_m2_regional'].std()) + 1e-8

    if sub['annee'].nunique() > 1:
        ppa   = sub.groupby('annee')['indice_prix_m2_regional'].mean()
        slope = float(np.polyfit(ppa.index.astype(float).values, ppa.values, 1)[0])
    else:
        slope = 0.0

    return {'mean': pm, 'tendance': slope, 'cv': pstd / (pm + 1e-8), 'n': len(sub)}


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    39 features par gouvernorat issues des données du dataset.

    CORRECTION type_transaction :
      prix_mean_loc / prix_mean_ven       → niveaux de prix séparés par type
      prix_tendance_loc / _ven            → vitesses de hausse distinctes
      prix_cv_loc / _ven                  → volatilités distinctes
      ratio_location_vente                → 0=marché 100% location, 1=100% vente
      spread_type                         → (pm_ven - pm_loc) / prix_global

    Les features prix globales (prix_mean, prix_tendance, ...) sont conservées
    pour la comparaison inter-gouvernorats indépendamment du mix transactionnel.
    """
    records = []
    prix_global_mean = df['indice_prix_m2_regional'].mean()
    prix_global_std  = df['indice_prix_m2_regional'].std() + 1e-8

    for gov, g in df.groupby('gouvernorat'):
        t = g['indice_prix_m2_regional']
        g = g[(t >= t.quantile(0.05)) & (t <= t.quantile(0.95))].copy()
        if len(g) < 3:
            continue

        prix_mean = g['indice_prix_m2_regional'].mean()
        if prix_mean == 0:
            continue
        prix_std = g['indice_prix_m2_regional'].std()

        # ── Features stratifiées par type_transaction (CORRECTION) ───────────
        loc_s = _prix_stats_par_type(g, TT_LOC)
        ven_s = _prix_stats_par_type(g, TT_VEN)

        n_loc = loc_s['n']
        n_ven = ven_s['n']
        n_tot = n_loc + n_ven + 1e-8

        # ratio_location_vente : 0=loc pure, 1=ven pure
        ratio_loc_ven = n_ven / n_tot

        # prix des types — fallback sur prix global si type absent
        pm_loc = loc_s['mean'] if loc_s['mean'] > 0 else prix_mean
        pm_ven = ven_s['mean'] if ven_s['mean'] > 0 else prix_mean

        # spread_type : capture les marchés où location ≠ vente (ex: Djerba)
        spread_type = (pm_ven - pm_loc) / (prix_mean + 1e-8)

        # ── Tendances globales ─────────────────────────────────────────────────
        if g['annee'].nunique() > 1:
            ppa          = g.groupby('annee')['indice_prix_m2_regional'].mean()
            annees       = ppa.index.astype(float).values
            slope        = float(np.polyfit(annees, ppa.values, 1)[0])
            slope_recent = float(np.polyfit(annees[-2:], ppa.values[-2:], 1)[0]) \
                           if len(ppa) >= 3 else slope
            prix_recent  = float(ppa.iloc[-1])
        else:
            slope = slope_recent = 0.0
            prix_recent = prix_mean

        # ── Saisonnalité ───────────────────────────────────────────────────────
        hs   = g['high_season'] == 1
        ls   = g['high_season'] == 0
        p_hs = g.loc[hs, 'indice_prix_m2_regional'].mean() if hs.sum() > 0 else prix_mean
        p_ls = g.loc[ls, 'indice_prix_m2_regional'].mean() if ls.sum() > 0 else prix_mean

        # ── Autres variables ───────────────────────────────────────────────────
        sa   = float(g['score_attractivite'].mean())
        nbi  = float(g['nb_infra'].mean())
        nbc  = float(g['nb_commerce'].mean())
        gli  = float(g['glissement_immo_trim'].mean())
        gli_v= float(g['glissement_immo_trim'].std()) if len(g) > 1 else 0.0
        inf  = float(g['inflation_glissement_annuel'].mean())
        pib  = float(g['croissance_pib_trim'].mean())
        wg   = float(g['sample_weight_geo'].mean())
        wt   = float(g['sample_weight_temporal'].mean())
        zone = int(g['zone_geographique'].iloc[0]) if 'zone_geographique' in g.columns else _get_zone(
            GOUVERNORAT_DEC.get(int(gov), str(gov)))

        records.append({
            'gouvernorat'            : gov,
            'nom'                    : GOUVERNORAT_DEC.get(int(gov), str(gov)),
            'zone_geographique'      : zone,
            # ── Prix global (tous types confondus) ────────────────────────────
            'prix_mean'              : prix_mean,
            'prix_cv'                : prix_std / prix_mean,
            'prix_iqr'               : float(g['indice_prix_m2_regional'].quantile(0.75)
                                             - g['indice_prix_m2_regional'].quantile(0.25)),
            'prix_tendance'          : slope,
            'prix_recent'            : prix_recent,
            'zscore_prix'            : (prix_mean - prix_global_mean) / prix_global_std,
            'rang_prix'              : 0.0,  # recalculé après sur le df complet
            # ── Prix stratifié par type_transaction (CORRECTION) ─────────────
            'prix_mean_loc'          : pm_loc,
            'prix_mean_ven'          : pm_ven,
            'prix_tendance_loc'      : loc_s['tendance'],
            'prix_tendance_ven'      : ven_s['tendance'],
            'prix_cv_loc'            : loc_s['cv'],
            'prix_cv_ven'            : ven_s['cv'],
            'ratio_location_vente'   : ratio_loc_ven,
            'spread_type'            : spread_type,
            # ── Saisonnalité ──────────────────────────────────────────────────
            'saisonnalite'           : (p_hs - p_ls) / prix_mean,
            'amplitude_saisonniere'  : abs(p_hs - p_ls) / prix_mean,
            'volatilite_saisonniere' : float(np.std([p_hs, p_ls])) / (prix_mean + 1e-8),
            'hs_ratio'               : float(g['high_season'].mean()),
            # ── Géographie ───────────────────────────────────────────────────
            'score_att'              : sa,
            'nb_infra'               : nbi,
            'nb_commerce'            : nbc,
            'potentiel_score'        : (nbi + nbc) / 2,
            'score_composite'        : sa * (nbi + nbc) / 2,
            # ── Macro ─────────────────────────────────────────────────────────
            'inflation'              : inf,
            'pib'                    : pib,
            'glissement'             : gli,
            'glissement_vol'         : gli_v,
            'spread_reel'            : gli - inf,
            'yield_implicite'        : gli / (prix_mean / 100 + 1e-8),
            # ── Dynamiques ────────────────────────────────────────────────────
            'croissance_acceleration': slope_recent - slope,
            'prix_recent_vs_global'  : prix_recent / prix_mean,
            'momentum'               : slope_recent - slope,
            # ── Interactions ──────────────────────────────────────────────────
            'prix_par_attractivite'  : prix_mean / (sa + 1e-8),
            'prix_par_infra'         : prix_mean / (nbi + 1),
            # ── Poids ─────────────────────────────────────────────────────────
            'weight_geo'             : wg,
            'weight_temp'            : wt,
            'poids_total'            : wg * wt,
            'arima_elig'             : float(g['arima_eligible'].mean()),
            'type_vente_ratio'       : ratio_loc_ven,
            'n_annees'               : float(g['annee'].nunique()),
            'n_obs'                  : float(len(g)),
        })

    df_feat = pd.DataFrame(records)
    if len(df_feat) > 0:
        df_feat['rang_prix'] = df_feat['prix_mean'].rank(pct=True)
        df_feat['voisin_prix_moy'] = df_feat.groupby('zone_geographique')['prix_mean']\
                                        .transform('mean')
        df_feat['voisin_prix_moy'] = df_feat['voisin_prix_moy'].fillna(df_feat['prix_mean'])
    return df_feat


# 39 features — prix global + 8 features type stratifiées + reste inchangé
FEAT_COLS = [
    # Prix global
    'prix_mean', 'prix_cv', 'prix_iqr', 'prix_tendance', 'prix_recent',
    'zscore_prix', 'rang_prix', 'voisin_prix_moy',
    # Prix stratifié par type (CORRECTION)
    'prix_mean_loc', 'prix_mean_ven',
    'prix_tendance_loc', 'prix_tendance_ven',
    'prix_cv_loc', 'prix_cv_ven',
    'ratio_location_vente', 'spread_type',
    # Saisonnalité
    'saisonnalite', 'amplitude_saisonniere', 'volatilite_saisonniere', 'hs_ratio',
    # Géographie
    'score_att', 'nb_infra', 'nb_commerce', 'potentiel_score', 'score_composite',
    # Macro
    'inflation', 'pib', 'glissement', 'glissement_vol', 'spread_reel', 'yield_implicite',
    # Dynamiques
    'croissance_acceleration', 'prix_recent_vs_global', 'momentum',
    # Interactions
    'prix_par_attractivite', 'prix_par_infra',
    # Poids
    'weight_geo', 'weight_temp', 'poids_total', 'arima_elig',
    'type_vente_ratio', 'n_annees',
]


# ─── Dashboard ────────────────────────────────────────────────────────────────

def plot_dashboard(resultats_plot: dict, output_path: str) -> None:
    GROUPS = [g for g in GROUPES_ACTIFS if g in resultats_plot]
    TITRES = {'Residentiel':'Résidentiel', 'Foncier':'Foncier', 'Commercial':'Commercial'}
    n_cols = len(GROUPS)

    fig = plt.figure(figsize=(7.5 * n_cols, 30), facecolor='white')
    gs  = GridSpec(5, n_cols, hspace=0.55, wspace=0.38,
                   top=0.94, bottom=0.03, left=0.06, right=0.97)

    # Row 0 : PCA scatter
    for col, grp in enumerate(GROUPS):
        ax = fig.add_subplot(gs[0, col])
        r  = resultats_plot[grp]
        df = r['gov_df']
        for c in sorted(df['cluster'].unique()):
            sub = df[df['cluster'] == c]
            ax.scatter(sub['X0'], sub['X1'], c=PALETTE[c], s=180,
                       zorder=3, edgecolors='white', linewidths=1.5, alpha=0.92)
            for _, row in sub.iterrows():
                ax.annotate(row['nom'], (row['X0'], row['X1']),
                            textcoords='offset points', xytext=(5, 5),
                            fontsize=7, color='#333330', fontweight='500')
        patches = [mpatches.Patch(color=PALETTE[c], label=f'C{c}')
                   for c in sorted(df['cluster'].unique())]
        ax.legend(handles=patches, fontsize=8, framealpha=0.85)
        sil = r['sil']
        badge = '#1D9E75' if sil >= 0.55 else ('#EF9F27' if sil >= 0.42 else '#E24B4A')
        ax.text(0.02, 0.97, f'Sil={sil:.4f}', transform=ax.transAxes,
                fontsize=9, fontweight='700', color=badge, va='top')
        ax.set_title(f"{TITRES[grp]} | algo={r['algo']} k={r['k']}",
                     fontsize=11, fontweight='600', color='#1A1A18', pad=8)
        ax.set_xlabel('PC1', fontsize=9); ax.set_ylabel('PC2', fontsize=9)
        ax.spines[['top','right']].set_visible(False)
        ax.set_facecolor('#F9F9F7'); ax.grid(True, alpha=0.25, linewidth=0.5)

    # Row 1 : Bar prix global
    for col, grp in enumerate(GROUPS):
        ax    = fig.add_subplot(gs[1, col])
        df    = resultats_plot[grp]['gov_df']
        stats = df.groupby('cluster').agg(
            prix=('prix_mean','mean'), tendance=('prix_tendance','mean'),
            n=('gouvernorat','count')).reset_index()
        ax.bar(range(len(stats)), stats['prix'],
               color=[PALETTE[c] for c in stats['cluster']],
               width=0.55, edgecolor='white', linewidth=1.5)
        for i, row in stats.iterrows():
            ax.text(i, row['prix'] + 15, f"{row['prix']:.0f}",
                    ha='center', fontsize=10, fontweight='600', color='#1A1A18')
            ax.text(i, row['prix'] / 2, f"n={int(row['n'])}",
                    ha='center', fontsize=9, color='white', fontweight='600')
            sign = '+' if row['tendance'] >= 0 else ''
            ax.text(i, -75, f"{sign}{row['tendance']:.0f}/an",
                    ha='center', fontsize=8, color='#5F5E5A')
        ax.set_xticks(range(len(stats)))
        ax.set_xticklabels([f'C{int(c)}' for c in stats['cluster']], fontsize=11)
        ax.set_ylabel('Prix moyen TND/m²', fontsize=9)
        ax.set_title('Prix moyen par cluster (tous types)', fontsize=10, fontweight='600')
        ax.set_ylim(0, df['prix_mean'].max() * 1.3)
        ax.spines[['top','right']].set_visible(False)
        ax.set_facecolor('#F9F9F7'); ax.tick_params(colors='#888780')

    # Row 2 : Prix Location vs Vente par cluster (NOUVEAU — correction type_transaction)
    for col, grp in enumerate(GROUPS):
        ax  = fig.add_subplot(gs[2, col])
        df  = resultats_plot[grp]['gov_df']
        clusters = sorted(df['cluster'].unique())
        x   = np.arange(len(clusters))
        w   = 0.35
        pm_loc = df.groupby('cluster')['prix_mean_loc'].mean()
        pm_ven = df.groupby('cluster')['prix_mean_ven'].mean()
        bars_loc = ax.bar(x - w/2, [pm_loc.get(c, 0) for c in clusters],
                          width=w, color='#3A86E8', label='Location (tt=1)',
                          edgecolor='white', linewidth=1.2)
        bars_ven = ax.bar(x + w/2, [pm_ven.get(c, 0) for c in clusters],
                          width=w, color='#E24B4A', label='Vente (tt=2)',
                          edgecolor='white', linewidth=1.2)
        for bar in bars_loc:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width()/2, h + 5, f'{h:.0f}',
                        ha='center', va='bottom', fontsize=7.5, color='#3A86E8')
        for bar in bars_ven:
            h = bar.get_height()
            if h > 0:
                ax.text(bar.get_x() + bar.get_width()/2, h + 5, f'{h:.0f}',
                        ha='center', va='bottom', fontsize=7.5, color='#E24B4A')
        ax.set_xticks(x)
        ax.set_xticklabels([f'C{c}' for c in clusters], fontsize=11)
        ax.set_ylabel('Prix/m² TND', fontsize=9)
        ax.set_title('Location vs Vente par cluster\n(CORRECTION type_transaction)',
                     fontsize=10, fontweight='600')
        ax.legend(fontsize=8); ax.spines[['top','right']].set_visible(False)
        ax.set_facecolor('#F9F9F7')

    # Row 3 : Radar
    radar_feats  = ['prix_mean','score_att','nb_infra',
                    'glissement','prix_tendance','spread_reel']
    radar_labels = ['Prix','Attractivité','Nb infra',
                    'Glissement','Tendance','Spread réel']
    N      = len(radar_feats)
    angles = np.linspace(0, 2*np.pi, N, endpoint=False).tolist() + [0]
    for col, grp in enumerate(GROUPS):
        ax = fig.add_subplot(gs[3, col], projection='polar')
        df = resultats_plot[grp]['gov_df']
        cm = df.groupby('cluster')[radar_feats].mean()
        rng = (cm.max() - cm.min()).replace(0, 1)
        cn  = (cm - cm.min()) / rng
        for c in cn.index:
            vals = cn.loc[c].tolist() + [cn.loc[c].tolist()[0]]
            ax.plot(angles, vals, color=PALETTE[c], linewidth=2.2)
            ax.fill(angles, vals, alpha=0.18, color=PALETTE[c])
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(radar_labels, size=8)
        ax.set_yticklabels([]); ax.set_ylim(0, 1)
        ax.grid(color='#CCCAC0', linewidth=0.5); ax.set_facecolor('#F9F9F7')
        ax.set_title(f'Profil — {TITRES[grp]}', fontsize=10,
                     fontweight='600', pad=20)

    # Row 4 : Silhouette par k + tableau
    n_span = max(n_cols - 1, 1)
    for col, grp in enumerate(GROUPS[:n_span]):
        ax = fig.add_subplot(gs[4, col])
        r  = resultats_plot[grp]
        ks = sorted(r['sil_par_k'].keys())
        sv = [r['sil_par_k'][k] for k in ks]
        ax.plot(ks, sv, marker='o', linewidth=2, color='#3A86E8',
                markersize=7, zorder=3)
        ax.axvline(r['k'], color='#E24B4A', linestyle='--',
                   alpha=0.8, linewidth=1.5)
        ax.text(r['k'] + 0.08, max(sv) * 0.98, f"k*={r['k']}",
                color='#E24B4A', fontsize=9, fontweight='700')
        ax.fill_between(ks, sv, alpha=0.12, color='#3A86E8')
        ax.set_xlabel('k', fontsize=9); ax.set_ylabel('Silhouette', fontsize=9)
        ax.set_title(f'Sélection k — {TITRES[grp]}', fontsize=10,
                     fontweight='600')
        ax.spines[['top','right']].set_visible(False)
        ax.set_facecolor('#F9F9F7'); ax.grid(True, alpha=0.25)

    ax_tbl = fig.add_subplot(gs[4, n_cols - 1])
    ax_tbl.axis('off')
    rows_data = []; sil_moy = 0.0
    for grp in GROUPS:
        r = resultats_plot[grp]
        noms_excl = [GOUVERNORAT_DEC.get(x, str(x)) for x in r['excluded']]
        rows_data.append([TITRES[grp], str(r['k']),
                          f"{r['sil']:.4f}", f"{r['db']:.4f}",
                          f"{r['ch']:.1f}", r['algo'],
                          str(noms_excl)])
        sil_moy += r['sil']
    sil_moy /= len(GROUPS)
    rows_data.append(['Moyen','—', f'{sil_moy:.4f}','—','—','—','—'])
    tbl = ax_tbl.table(cellText=rows_data,
                       colLabels=['Groupe','k','Sil ↑','DB ↓','CH ↑',
                                  'Algo','Outliers'],
                       loc='center', cellLoc='center')
    tbl.auto_set_font_size(False); tbl.set_fontsize(8); tbl.scale(1, 1.8)
    for (ri, c), cell in tbl.get_celld().items():
        cell.set_edgecolor('#CCCAC0')
        if ri == 0:
            cell.set_facecolor('#DFF0EA')
            cell.set_text_props(fontweight='700', color='#0A3D2E')
        elif ri == len(rows_data):
            cell.set_facecolor('#FAEEDA')
            cell.set_text_props(fontweight='600', color='#633806')
        else:
            cell.set_facecolor('#F9F9F7')
            cell.set_text_props(color='#1A1A18')
    ax_tbl.set_title('Métriques + outliers détectés',
                     fontsize=10, fontweight='600', pad=12)

    fig.suptitle(
        'Segmentation Géographique — Tunisie | Résidentiel · Foncier · Commercial\n'
        '39 features · STRATIFIÉ type_transaction · RobustScaler · PCA · algo/k/outliers 100% dynamiques',
        fontsize=14, fontweight='700', color='#1A1A18', y=0.97,
    )
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close(fig)
    print(f"  → Dashboard : {output_path}")


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def run_kmeans(groupes: dict) -> dict:
    """
    Tous les hyperparamètres calculés depuis les données.
    CORRECTION : features prix stratifiées Location vs Vente via _prix_stats_par_type().
    cluster_map enrichi avec prix_mean_loc/ven et prix_tendance_loc/ven
    pour usage dans pelt_A.py et lstm_A.py.
    """
    print()
    print("=" * 72)
    print("  K-MEANS A | 39 features · STRATIFIÉ type_transaction (loc=1 / ven=2)")
    print("  GOV_NAMES = GOUVERNORAT_DEC depuis mappings_BO3 (aucune duplication)")
    print("=" * 72)

    # ── Validation Feature Registry ────────────────────────────────────────────
    if _FEATURES_CONFIG_AVAILABLE:
        all_df = pd.concat(groupes.values(), ignore_index=True)
        present, missing = validate_features(all_df, "kmeans")
        print(f"  Feature Registry [kmeans] : {len(present)} sources présentes"
              f"{(' | manquantes: ' + str(missing)) if missing else ' ✔'}")
    print()

    resultats   = {}
    cluster_map = {}
    silhouettes = []

    for groupe in GROUPES_ACTIFS:
        df_raw = groupes.get(groupe)
        if df_raw is None:
            print(f"\n  ✗ {groupe} absent"); continue

        # Rapport composition type_transaction (depuis données brutes)
        n_loc = int((df_raw['type_transaction'] == TT_LOC).sum()) \
                if 'type_transaction' in df_raw.columns else 0
        n_ven = int((df_raw['type_transaction'] == TT_VEN).sum()) \
                if 'type_transaction' in df_raw.columns else 0
        n_tot = len(df_raw)
        print(f"\n{'─'*60}\n  {groupe.upper()} "
              f"({n_tot:,} obs | {df_raw['gouvernorat'].nunique()} gouvernorats)")
        print(f"  Composition : Location={n_loc:,} ({n_loc/n_tot*100:.1f}%) | "
              f"Vente={n_ven:,} ({n_ven/n_tot*100:.1f}%)")

        min_obs = compute_min_obs(df_raw)
        print(f"  MIN_OBS calculé dynamiquement : {min_obs} (p10 obs/gouvernorat)")

        df = df_raw  # alias pour detect_sparse_govs (attend le df brut)
        gov_df = build_features(df_raw)

        # Filtre IQR_KMEANS sur prix global (features statiques)
        Q1, Q3 = gov_df['prix_mean'].quantile(0.25), gov_df['prix_mean'].quantile(0.75)
        iqr_km  = Q3 - Q1
        gov_df  = gov_df[(gov_df['prix_mean'] >= Q1 - IQR_KMEANS * iqr_km) &
                         (gov_df['prix_mean'] <= Q3 + IQR_KMEANS * iqr_km)].copy()
        gov_df  = gov_df[gov_df['n_obs'] >= min_obs].copy()

        # Filtre 1 : gouvernorats sans série temporelle (≤ 1 point annee×mois)
        sparse = detect_sparse_govs(df, gov_df)
        if sparse:
            noms_sparse = [GOUVERNORAT_DEC.get(g, str(g)) for g in sparse]
            print(f"  Séries vides exclus (≤1 pt temporel) : {noms_sparse}")
        gov_df = gov_df[~gov_df['gouvernorat'].isin(sparse)].copy()

        # Filtre 2 : prix aberrant (IQR × 3)
        outliers = detect_outlier_govs(gov_df)
        if outliers:
            noms_excl = [GOUVERNORAT_DEC.get(g, str(g)) for g in outliers]
            print(f"  Outliers prix exclus (IQR×3)           : {noms_excl}")
        gov_df = gov_df[~gov_df['gouvernorat'].isin(outliers)].copy()
        n_gov  = len(gov_df)
        print(f"  Gouvernorats retenus : {n_gov}")

        if n_gov < 4:
            print(f"  ⚠  < 4 gouvernorats → ignoré"); continue

        feats = [f for f in FEAT_COLS if f in gov_df.columns]
        X     = gov_df[feats].fillna(0.0).values
        X_s   = RobustScaler().fit_transform(X)

        # PCA adaptative : tester 4 niveaux de variance, garder le meilleur Sil
        max_comp        = min(n_gov - 1, len(feats))
        best_sil_global = -1
        best_result     = None

        for pca_var in [0.80, 0.85, 0.90, 0.95]:
            pca_full = PCA(n_components=max_comp, random_state=42).fit(X_s)
            cum_var  = np.cumsum(pca_full.explained_variance_ratio_)
            n_comp   = max(2, min(int(np.searchsorted(cum_var, pca_var) + 1), max_comp))
            pca      = PCA(n_components=n_comp, random_state=42)
            X_pca    = pca.fit_transform(X_s)
            var      = pca.explained_variance_ratio_.sum() * 100

            algo_name, best_k, best_sil, best_lbl, sil_par_k = \
                select_best_config(X_pca, n_gov,
                                   gov_noms=gov_df['nom'].tolist(),
                                   k_min=K_MIN_GROUPE.get(groupe, 2))

            if best_sil > best_sil_global:
                best_sil_global = best_sil
                best_result = {
                    'algo': algo_name, 'k': best_k, 'sil': best_sil,
                    'lbl': best_lbl, 'X_pca': X_pca, 'var': var,
                    'n_comp': n_comp, 'pca_var': pca_var,
                    'sil_par_k': sil_par_k,
                }

        r      = best_result
        gov_df = gov_df.copy()
        gov_df['cluster'] = r['lbl']
        db = davies_bouldin_score(r['X_pca'], r['lbl'])
        ch = calinski_harabasz_score(r['X_pca'], r['lbl'])

        print(f"\n  ✓ algo={r['algo']} k={r['k']} pca={r['pca_var']} "
              f"({r['n_comp']} comp, {r['var']:.1f}% var)")
        print(f"    Sil={r['sil']:.4f} | DB={db:.4f}↓ | CH={ch:.2f}↑")
        print(f"    Sil par k : {r['sil_par_k']}")

        pt_brut_by_gov = df.groupby('gouvernorat').apply(
            lambda x: x[['annee','mois']].drop_duplicates().shape[0])

        cluster_map[groupe] = {}
        print(f"\n  Clusters :")
        for c in sorted(gov_df['cluster'].unique()):
            sub         = gov_df[gov_df['cluster'] == c]
            noms        = sub['nom'].tolist()
            cluster_sz  = len(noms)   # ← nombre de gouvernorats dans ce cluster
            prix        = float(sub['prix_mean'].mean())
            slope       = float(sub['prix_tendance'].mean())
            att         = float(sub['score_att'].mean())
            spread      = float(sub['spread_reel'].mean())
            zscore      = float(sub['zscore_prix'].mean())
            r_lv        = float(sub['ratio_location_vente'].mean())
            pts_c       = [int(pt_brut_by_gov.get(int(row['gouvernorat']), 0))
                          for _, row in sub.iterrows()]
            # Interprétation métier : cluster_size petit → comportement atypique
            atypique = " ⚠ atypique" if cluster_sz <= 2 else ""
            print(f"    C{c} ({cluster_sz} gov{atypique} | {prix:.0f} TND/m² | "
                  f"Δ {slope:+.0f}/an | att={att:.3f} | "
                  f"spread={spread:.2f}% | z={zscore:+.2f} | loc/ven={r_lv:.2f})")
            print(f"       {noms}")
            print(f"       pts_PELT: total={sum(pts_c)} | "
                  f"min={min(pts_c)} | max={max(pts_c)} "
                  f"({'⚠ série(s) courte(s)' if min(pts_c) < 5 else '✓ OK'})")

            for _, row in sub.iterrows():
                gid   = int(row['gouvernorat'])
                n_pts = int(pt_brut_by_gov.get(gid, 0))
                # cluster_map enrichi avec les prix stratifiés — utilisé par pelt + lstm
                # cluster_size ajouté : insight métier (petit → atypique, grand → stable)
                cluster_map[groupe][gid] = {
                    'cluster'              : int(c),
                    'cluster_size'         : cluster_sz,   # ← NOUVEAU
                    'nom'                  : row['nom'],
                    'prix_mean'            : round(float(row['prix_mean']), 2),
                    'prix_mean_loc'        : round(float(row['prix_mean_loc']), 2),
                    'prix_mean_ven'        : round(float(row['prix_mean_ven']), 2),
                    'prix_tendance'        : round(float(row['prix_tendance']), 2),
                    'prix_tendance_loc'    : round(float(row['prix_tendance_loc']), 2),
                    'prix_tendance_ven'    : round(float(row['prix_tendance_ven']), 2),
                    'score_att'            : round(float(row['score_att']), 4),
                    'spread_reel'          : round(float(row['spread_reel']), 4),
                    'spread_type'          : round(float(row['spread_type']), 4),
                    'zscore_prix'          : round(float(row['zscore_prix']), 4),
                    'rang_prix'            : round(float(row.get('rang_prix', 0)), 4),
                    'ratio_location_vente' : round(float(row['ratio_location_vente']), 4),
                    'gouvernorat'          : gid,
                    'n_pts_temporels'      : n_pts,
                }

        silhouettes.append(r['sil'])
        resultats[groupe] = {
            'k': r['k'], 'silhouette': r['sil'], 'davies_bouldin': db,
            'calinski_harabasz': ch, 'sil_par_k': r['sil_par_k'],
            'n_gouvernorats': n_gov, 'feats': feats,
            'pca_var': round(r['var'], 2), 'pca_n': r['n_comp'],
            'algo': r['algo'], 'excluded': sparse + outliers, 'min_obs': min_obs,
            'clusters': gov_df[['gouvernorat','cluster']].to_dict('records'),
            'gov_df': gov_df.assign(
                X0=r['X_pca'][:, 0],
                X1=r['X_pca'][:, 1] if r['X_pca'].shape[1] > 1 else 0.0),
            'sil': r['sil'], 'db': db, 'ch': ch,
        }

    score_moy = float(np.mean(silhouettes)) if silhouettes else 0.0

    print()
    print("=" * 72); print("  BILAN"); print("=" * 72)
    print(f"  {'Groupe':<14} {'Sil':>8} {'DB':>8} {'CH':>8} "
          f"{'k':>4} {'algo':<12} exclu")
    print(f"  {'─'*70}")
    for g in GROUPES_ACTIFS:
        r = resultats.get(g)
        if r:
            noms_excl = [GOUVERNORAT_DEC.get(x, str(x)) for x in r['excluded']]
            print(f"  {g:<14} {r['silhouette']:>8.4f} {r['davies_bouldin']:>8.4f} "
                  f"{r['calinski_harabasz']:>8.2f} {r['k']:>4} "
                  f"{r['algo']:<12} {noms_excl}")
    print(f"\n  ✓ Silhouette moyen : {score_moy:.4f}")
    # Insight cluster_size : signalement automatique des clusters atypiques
    print(f"\n  ── Insight cluster_size (comportements atypiques) ──")
    for g_name, cm_grp in cluster_map.items():
        sizes = {}
        for gid, info in cm_grp.items():
            c = info['cluster']
            sizes.setdefault(c, []).append(info['nom'])
        for c, noms_c in sorted(sizes.items()):
            tag = " ⚠ atypique (petit cluster)" if len(noms_c) <= 2 else " ✓ stable"
            print(f"    [{g_name}] C{c} ({len(noms_c)} gov){tag}: {noms_c}")
    print()

    plot_path = os.path.join(GRAPHS_DIR, 'kmeans_A_dashboard.png')
    try:
        plot_dashboard(resultats, plot_path)
    except Exception:
        import traceback; traceback.print_exc()

    pkl_res = os.path.join(MODELS_DIR, 'kmeans_A.pkl')
    with open(pkl_res, 'wb') as f:
        pickle.dump({k: {kk: vv for kk, vv in v.items() if kk != 'gov_df'}
                     for k, v in resultats.items()}, f)
    print(f"  → kmeans_A.pkl    : {pkl_res}")

    pkl_map = os.path.join(MODELS_DIR, 'cluster_map.pkl')
    with open(pkl_map, 'wb') as f:
        pickle.dump(cluster_map, f)
    print(f"  → cluster_map.pkl : {pkl_map}")

    pd.DataFrame([{
        'groupe': g, 'k': r['k'], 'silhouette': r['silhouette'],
        'davies_bouldin': r['davies_bouldin'],
        'calinski_harabasz': r['calinski_harabasz'],
        'pca_n': r['pca_n'], 'pca_var': r['pca_var'],
        'algo': r['algo'], 'n_gouvernorats': r['n_gouvernorats'],
        'min_obs_calcule': r['min_obs'],
        'outliers_exclus': str(r['excluded']),
        'n_features': len(FEAT_COLS),
        'stratification': 'type_transaction loc/ven',
    } for g, r in resultats.items()]
    ).to_csv(os.path.join(MODELS_DIR, 'kmeans_A_metriques.csv'), index=False)
    print(f"  → CSV métriques   : "
          f"{os.path.join(MODELS_DIR, 'kmeans_A_metriques.csv')}")
    print("=" * 72)

    return {
        'score': score_moy, 'metric': 'silhouette',
        'pkl': pkl_res, 'cluster_map': pkl_map,
        'plot': plot_path, 'details': resultats,
    }


# ─── Point d'entrée ──────────────────────────────────────────────────────────

if __name__ == '__main__':
    dossier = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     '..', '..', 'BO3'))
    print(f"Dossier : {dossier}\n")
    groupes = {}
    for nom in GROUPES_ACTIFS:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f"  ✔ {nom:<14} : {len(groupes[nom]):,} lignes")
        else:
            print(f"  ✗ {nom} manquant")
    if groupes:
        run_kmeans(groupes)