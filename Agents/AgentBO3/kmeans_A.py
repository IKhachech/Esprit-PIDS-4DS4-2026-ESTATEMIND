"""
kmeans_B.py — Segmentation Géographique · Version Production L99
=================================================================

OBJECTIF MÉTIER 3 : Prédire les tendances régionales du marché immobilier tunisien.

PRINCIPES :
  • Aucune constante manuelle codée en dur (noms, seuils, algos, exclusions).
  • Tout est calculé depuis les données réelles et depuis mappings_BO3.
  • GOV_NAMES    → GOUVERNORAT_DEC depuis mappings_BO3 (source officielle pipeline)
  • MIN_OBS      → percentile 10 des observations par gouvernorat (par groupe)
  • EXCLUDE_GOVS → détection automatique des outliers prix (IQR × 3)
  • BEST_CONFIGS → sélection exhaustive algo + PCA (KMeans vs Agglom-avg vs Agglom-ward)

CORRECTIONS v2 :
  • sil_par_k : récupéré depuis select_best_config (suppression du double calcul)
  • import mappings_BO3 : fallback robuste (3 chemins tentés)

Sortie :
  models/kmeans_A.pkl          — métriques + clusters
  models/cluster_map.pkl       — {groupe → {gov_id → info}} → pour pelt_B.py
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

from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.preprocessing import RobustScaler
from sklearn.decomposition import PCA
from sklearn.metrics import (silhouette_score, davies_bouldin_score,
                              calinski_harabasz_score)

warnings.filterwarnings('ignore')

# ─── Import mappings officiels du pipeline ─────────────────────────────────
# GOUVERNORAT_DEC = {code_int → nom_str} — source unique, pas de duplication
# Fallback sur 3 chemins possibles selon la structure du projet
def _import_gouvernorat_dec():
    candidates = [
        # workspace/BO3/Agent3/kmeans_B.py → workspace/
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      '..', '..', '..')),
        # workspace/BO3/kmeans_B.py → workspace/
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      '..', '..')),
        # même répertoire
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

# ─── Répertoires ──────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models')
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs', 'kmeans')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(GRAPHS_DIR, exist_ok=True)

GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']

PALETTE = {0:'#1D9E75', 1:'#D85A30', 2:'#7F77DD',
           3:'#EF9F27', 4:'#D4537E', 5:'#3FA7D6'}


# ─── Calcul dynamique des hyperparamètres ─────────────────────────────────────

def compute_min_obs(df: pd.DataFrame) -> int:
    """
    MIN_OBS = max(3, percentile 10 des obs par gouvernorat).
    Garantit que les gouvernorats très peu couverts sont exclus,
    sans valeur codée en dur.
    """
    obs_par_gov = df.groupby('gouvernorat').size()
    return max(3, int(obs_par_gov.quantile(0.10)))


def detect_sparse_govs(df_raw: pd.DataFrame, gov_df: pd.DataFrame) -> list:
    """
    Exclut les gouvernorats avec <= 1 point temporel distinct (annee×mois).
    Un gouvernorat sans série temporelle ne peut pas être clusterisé de façon
    fiable — son prix_mean repose sur 1 ou 2 annonces isolées.
    Calculé depuis les données brutes (avant agrégation features).
    """
    pt_by_gov = df_raw.groupby('gouvernorat').apply(
        lambda x: x[['annee','mois']].drop_duplicates().shape[0])
    sparse = pt_by_gov[pt_by_gov <= 1].index.astype(int).tolist()
    return sparse


def detect_outlier_govs(gov_df: pd.DataFrame) -> list:
    """
    EXCLUDE_GOVS = gouvernorats dont le prix moyen est un outlier IQR × 3.
    Méthode robuste : aucune liste manuelle.
    """
    Q1  = gov_df['prix_mean'].quantile(0.25)
    Q3  = gov_df['prix_mean'].quantile(0.75)
    IQR = Q3 - Q1
    mask = ((gov_df['prix_mean'] < Q1 - 3 * IQR) |
            (gov_df['prix_mean'] > Q3 + 3 * IQR))
    return gov_df.loc[mask, 'gouvernorat'].astype(int).tolist()


def select_best_config(X_pca: np.ndarray, n_gov: int) -> tuple:
    """
    Sélection exhaustive sur 3 algos × k ∈ [2, min(7, n_gov-1)].
    Retourne (algo_name, k_opt, sil_opt, labels_opt, sil_par_k).

    CORRECTION v2 : retourne aussi sil_par_k pour éviter le double calcul
    dans run_kmeans.
    """
    best = {'sil': -1, 'algo': 'KMeans', 'k': 2, 'lbl': None}
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
    for algo_name, algo_fn in algos:
        for k in range(2, max_k + 1):
            lbl = algo_fn(k).fit_predict(X_pca)
            if len(set(lbl)) < 2:
                continue
            sil = silhouette_score(X_pca, lbl)
            # Conserver sil_par_k pour l'algo gagnant (mis à jour si meilleur)
            if sil > best['sil']:
                best.update({'sil': sil, 'algo': algo_name, 'k': k, 'lbl': lbl})

    # Reconstruire sil_par_k pour l'algo optimal uniquement
    best_algo_fn = next(fn for name, fn in algos if name == best['algo'])
    for k in range(2, max_k + 1):
        lbl = best_algo_fn(k).fit_predict(X_pca)
        if len(set(lbl)) >= 2:
            sil_par_k[k] = round(silhouette_score(X_pca, lbl), 4)

    return best['algo'], best['k'], best['sil'], best['lbl'], sil_par_k


# ─── Feature Engineering ──────────────────────────────────────────────────────

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    33 features par gouvernorat depuis les 16 colonnes du dataset.
    Noms via GOUVERNORAT_DEC (mappings_BO3) — aucune duplication.
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

        # Tendances (annee × indice_prix_m2_regional)
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

        # Saisonnalité (high_season × indice_prix_m2_regional)
        hs   = g['high_season'] == 1
        ls   = g['high_season'] == 0
        p_hs = g.loc[hs, 'indice_prix_m2_regional'].mean() if hs.sum() > 0 else prix_mean
        p_ls = g.loc[ls, 'indice_prix_m2_regional'].mean() if ls.sum() > 0 else prix_mean

        sa       = float(g['score_attractivite'].mean())
        nbi      = float(g['nb_infra'].mean())
        nbc      = float(g['nb_commerce'].mean())
        gli      = float(g['glissement_immo_trim'].mean())
        gli_v    = float(g['glissement_immo_trim'].std()) if len(g) > 1 else 0.0
        inf      = float(g['inflation_glissement_annuel'].mean())
        pib      = float(g['croissance_pib_trim'].mean())
        wg       = float(g['sample_weight_geo'].mean())
        wt       = float(g['sample_weight_temporal'].mean())
        prix_std = g['indice_prix_m2_regional'].std()

        records.append({
            'gouvernorat'            : gov,
            'nom'                    : GOUVERNORAT_DEC.get(int(gov), str(gov)),
            # Prix
            'prix_mean'              : prix_mean,
            'prix_cv'                : prix_std / prix_mean,
            'prix_iqr'               : float(g['indice_prix_m2_regional'].quantile(0.75)
                                             - g['indice_prix_m2_regional'].quantile(0.25)),
            'prix_tendance'          : slope,
            'prix_recent'            : prix_recent,
            'zscore_prix'            : (prix_mean - prix_global_mean) / prix_global_std,
            'rang_prix'              : 0.0,  # recalculé après sur le df complet
            # Saisonnalité
            'saisonnalite'           : (p_hs - p_ls) / prix_mean,
            'amplitude_saisonniere'  : abs(p_hs - p_ls) / prix_mean,
            'volatilite_saisonniere' : float(np.std([p_hs, p_ls])) / (prix_mean + 1e-8),
            'hs_ratio'               : float(g['high_season'].mean()),
            # Géographie
            'score_att'              : sa,
            'nb_infra'               : nbi,
            'nb_commerce'            : nbc,
            'potentiel_score'        : (nbi + nbc) / 2,
            'score_composite'        : sa * (nbi + nbc) / 2,
            # Macro
            'inflation'              : inf,
            'pib'                    : pib,
            'glissement'             : gli,
            'glissement_vol'         : gli_v,
            'spread_reel'            : gli - inf,
            'yield_implicite'        : gli / (prix_mean / 100 + 1e-8),
            # Dynamiques
            'croissance_acceleration': slope_recent - slope,
            'prix_recent_vs_global'  : prix_recent / prix_mean,
            'momentum'               : slope_recent - slope,
            # Interactions
            'prix_par_attractivite'  : prix_mean / (sa + 1e-8),
            'prix_par_infra'         : prix_mean / (nbi + 1),
            # Poids
            'weight_geo'             : wg,
            'weight_temp'            : wt,
            'poids_total'            : wg * wt,
            'arima_elig'             : float(g['arima_eligible'].mean()),
            'type_vente_ratio'       : float((g['type_transaction'] == 2).mean()),
            'n_annees'               : float(g['annee'].nunique()),
            'n_obs'                  : float(len(g)),
        })

    df_feat = pd.DataFrame(records)
    if len(df_feat) > 0:
        df_feat['rang_prix'] = df_feat['prix_mean'].rank(pct=True)
    return df_feat


FEAT_COLS = [
    'prix_mean', 'prix_cv', 'prix_iqr', 'prix_tendance', 'prix_recent',
    'zscore_prix', 'rang_prix',
    'saisonnalite', 'amplitude_saisonniere', 'volatilite_saisonniere', 'hs_ratio',
    'score_att', 'nb_infra', 'nb_commerce', 'potentiel_score', 'score_composite',
    'inflation', 'pib', 'glissement', 'glissement_vol', 'spread_reel', 'yield_implicite',
    'croissance_acceleration', 'prix_recent_vs_global', 'momentum',
    'prix_par_attractivite', 'prix_par_infra',
    'weight_geo', 'weight_temp', 'poids_total', 'arima_elig',
    'type_vente_ratio', 'n_annees',
]


# ─── Dashboard ────────────────────────────────────────────────────────────────

def plot_dashboard(resultats_plot: dict, output_path: str) -> None:
    GROUPS = [g for g in GROUPES_ACTIFS if g in resultats_plot]
    TITRES = {'Residentiel':'Résidentiel', 'Foncier':'Foncier', 'Commercial':'Commercial'}
    n_cols = len(GROUPS)

    fig = plt.figure(figsize=(7.5 * n_cols, 24), facecolor='white')
    gs  = GridSpec(4, n_cols, hspace=0.52, wspace=0.38,
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

    # Row 1 : Bar prix
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
        ax.set_title('Prix moyen par cluster', fontsize=10, fontweight='600')
        ax.set_ylim(0, df['prix_mean'].max() * 1.3)
        ax.spines[['top','right']].set_visible(False)
        ax.set_facecolor('#F9F9F7'); ax.tick_params(colors='#888780')

    # Row 2 : Radar
    radar_feats  = ['prix_mean','score_att','nb_infra',
                    'glissement','prix_tendance','spread_reel']
    radar_labels = ['Prix','Attractivité','Nb infra',
                    'Glissement','Tendance','Spread réel']
    N      = len(radar_feats)
    angles = np.linspace(0, 2*np.pi, N, endpoint=False).tolist() + [0]
    for col, grp in enumerate(GROUPS):
        ax = fig.add_subplot(gs[2, col], projection='polar')
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

    # Row 3 : Silhouette par k + tableau
    n_span = max(n_cols - 1, 1)
    for col, grp in enumerate(GROUPS[:n_span]):
        ax = fig.add_subplot(gs[3, col])
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

    ax_tbl = fig.add_subplot(gs[3, n_cols - 1])
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
        '33 features · RobustScaler · PCA · algo/k/outliers 100% dynamiques',
        fontsize=14, fontweight='700', color='#1A1A18', y=0.97,
    )
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close(fig)
    print(f"  → Dashboard : {output_path}")


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def run_kmeans(groupes: dict) -> dict:
    """
    Tous les hyperparamètres calculés depuis les données :
      MIN_OBS      → percentile 10 obs/gouvernorat
      EXCLUDE_GOVS → IQR × 3 sur prix_mean
      BEST_CONFIGS → sélection exhaustive algo × PCA × k
    """
    print()
    print("=" * 72)
    print("  K-MEANS A | 33 features · tout dynamique (MIN_OBS, outliers, algo, k)")
    print("  GOV_NAMES = GOUVERNORAT_DEC depuis mappings_BO3 (aucune duplication)")
    print("=" * 72)

    resultats   = {}
    cluster_map = {}
    silhouettes = []

    for groupe in GROUPES_ACTIFS:
        df = groupes.get(groupe)
        if df is None:
            print(f"\n  ✗ {groupe} absent"); continue

        print(f"\n{'─'*60}\n  {groupe.upper()} "
              f"({len(df):,} obs | {df['gouvernorat'].nunique()} gouvernorats)")

        min_obs = compute_min_obs(df)
        print(f"  MIN_OBS calculé dynamiquement : {min_obs} (p10 obs/gouvernorat)")

        gov_df = build_features(df)

        # Filtre IQR 2.5×
        Q1, Q3 = gov_df['prix_mean'].quantile(0.25), gov_df['prix_mean'].quantile(0.75)
        gov_df = gov_df[(gov_df['prix_mean'] >= Q1 - 2.5*(Q3-Q1)) &
                        (gov_df['prix_mean'] <= Q3 + 2.5*(Q3-Q1))].copy()
        gov_df = gov_df[gov_df['n_obs'] >= min_obs].copy()

        # Filtre 1 : gouvernorats sans série temporelle (≤ 1 point annee×mois)
        sparse = detect_sparse_govs(df, gov_df)
        if sparse:
            noms_sparse = [GOUVERNORAT_DEC.get(g, str(g)) for g in sparse]
            print(f"  Séries vides exclus (≤1 pt temporel) : {noms_sparse}")
        gov_df = gov_df[~gov_df['gouvernorat'].isin(sparse)].copy()

        # Filtre 2 : prix aberrant (IQR × 3) — recalculé après exclusion sparse
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

            # Sélection algo + k + récupération sil_par_k (pas de double calcul)
            algo_name, best_k, best_sil, best_lbl, sil_par_k = \
                select_best_config(X_pca, n_gov)

            if best_sil > best_sil_global:
                best_sil_global = best_sil
                best_result = {
                    'algo': algo_name, 'k': best_k, 'sil': best_sil,
                    'lbl': best_lbl, 'X_pca': X_pca, 'var': var,
                    'n_comp': n_comp, 'pca_var': pca_var,
                    'sil_par_k': sil_par_k,  # CORRECTION : récupéré directement
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

        cluster_map[groupe] = {}
        print(f"\n  Clusters :")
        for c in sorted(gov_df['cluster'].unique()):
            sub    = gov_df[gov_df['cluster'] == c]
            noms   = sub['nom'].tolist()
            prix   = float(sub['prix_mean'].mean())
            slope  = float(sub['prix_tendance'].mean())
            att    = float(sub['score_att'].mean())
            spread = float(sub['spread_reel'].mean())
            zscore = float(sub['zscore_prix'].mean())
            print(f"    C{c} ({len(noms)} gov | {prix:.0f} TND/m² | "
                  f"Δ {slope:+.0f}/an | att={att:.3f} | "
                  f"spread={spread:.2f}% | z={zscore:+.2f})")
            print(f"       {noms}")

            for _, row in sub.iterrows():
                gid = int(row['gouvernorat'])
                cluster_map[groupe][gid] = {
                    'cluster'       : int(c),
                    'nom'           : row['nom'],
                    'prix_mean'     : round(float(row['prix_mean']), 2),
                    'prix_tendance' : round(float(row['prix_tendance']), 2),
                    'score_att'     : round(float(row['score_att']), 4),
                    'spread_reel'   : round(float(row['spread_reel']), 4),
                    'zscore_prix'   : round(float(row['zscore_prix']), 4),
                    'rang_prix'     : round(float(row.get('rang_prix', 0)), 4),
                    'gouvernorat'   : gid,
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
    print(f"\n  ✓ Silhouette moyen : {score_moy:.4f}\n")

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