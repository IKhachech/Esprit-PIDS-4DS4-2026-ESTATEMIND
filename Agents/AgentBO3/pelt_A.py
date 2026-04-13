"""
pelt_A.py — Détection des Ruptures de Régime par Cluster · Version Production L99
===================================================================================

OBJECTIF MÉTIER 3 : Prédire les tendances régionales du marché immobilier tunisien.

PRINCIPES :
  • Aucune constante manuelle codée en dur.
  • GOV_NAMES    → GOUVERNORAT_DEC depuis mappings_BO3 (source unique du pipeline)
  • MIN_ANNONCES → calculé dynamiquement : p5 du nombre d'obs par (gov, annee, mois)
  • MIN_SIGNAL   → calculé : p10 du nombre de points temporels distincts par gouvernorat
  • PENALTY_MULT → 1.0 + (std médian des signaux prix normalisés) × 0.15
  • MIN_AMPLITUDE→ calculé : p25 des variations absolues de prix lissés (par groupe)
  • MIN_GAP      → fixé à 3 (contrainte physique PELT, pas un hyperparamètre)

CORRECTIONS v2 :
  • Docstring penalty_mult : aligné sur le code (×0.15, pas ×0.3)
  • import mappings_BO3 : fallback robuste (3 chemins tentés)

Dépendance : exécuter kmeans_A.py en premier → models/cluster_map.pkl

Sortie :
  models/pelt_A.pkl
  models/pelt_A_rapport.csv
  graphs/pelt/pelt_A_{groupe}.png
"""

import os, sys, pickle, warnings
import numpy as np
import pandas as pd
import ruptures as rpt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
from matplotlib.colors import LinearSegmentedColormap

warnings.filterwarnings('ignore')

# ─── Import mappings officiels du pipeline ─────────────────────────────────
# GOUVERNORAT_DEC = {code_int → nom_str} — source unique, pas de duplication
# Fallback sur 3 chemins possibles selon la structure du projet
def _import_gouvernorat_dec():
    candidates = [
        # workspace/BO3/Agent3/pelt_A.py → workspace/
        os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      '..', '..', '..')),
        # workspace/BO3/pelt_A.py → workspace/
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
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs', 'pelt')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(GRAPHS_DIR, exist_ok=True)

GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']
MIN_SIZE = 3   # taille minimale segment PELT — contrainte physique
MIN_GAP  = 3   # gap minimal entre ruptures — contrainte physique

CLUSTER_PALETTE = {0:'#1D9E75', 1:'#D85A30', 2:'#7F77DD',
                   3:'#EF9F27', 4:'#D4537E', 5:'#3FA7D6'}
GREY = '#B4B2A9'


# ─── Calcul dynamique des paramètres PELT ─────────────────────────────────────

def compute_pelt_params(df: pd.DataFrame) -> dict:
    """
    Calcule tous les paramètres PELT depuis les données réelles.

    MIN_ANNONCES : p5 du nombre d'obs par (gouvernorat, annee, mois).
      → Exclut les périodes trop rares sans valeur arbitraire.

    MIN_SIGNAL   : p10 du nombre de points temporels distincts par gouvernorat.
      → Lance PELT uniquement sur des séries suffisamment longues.

    MIN_AMPLITUDE: p25 des variations absolues de prix lissés.
      → Seuil de base ; affiné par cluster dans seuil_amplitude_cluster().

    PENALTY_MULT : 1.0 + (std médian des signaux prix normalisés) × 0.15.
      → Plus le signal est bruité, plus la pénalité est conservative.
      → Borné dans [1.05, 1.35].
    """
    # MIN_ANNONCES
    grp_counts = df.groupby(['gouvernorat', 'annee', 'mois']).size()
    min_ann    = max(2, int(grp_counts.quantile(0.05)))

    # MIN_SIGNAL : points temporels distincts par gouvernorat
    pt_by_gov  = (df.groupby('gouvernorat')
                    .apply(lambda x: x[['annee','mois']].drop_duplicates().shape[0]))
    min_signal = max(4, int(pt_by_gov.quantile(0.10)))

    # MIN_AMPLITUDE : p25 des variations de prix lissés
    agg = (df.groupby(['gouvernorat','annee','mois'])['indice_prix_m2_regional']
             .mean().reset_index()
             .sort_values(['gouvernorat','annee','mois']))
    agg['lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                       .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    agg['var']   = agg.groupby('gouvernorat')['lisse'].pct_change().abs()
    variations   = agg['var'].dropna()
    min_amp      = round(float(variations.quantile(0.25)), 3) \
                   if len(variations) > 10 else 0.06
    min_amp      = max(0.04, min(0.15, min_amp))

    # PENALTY_MULT : calibré sur le bruit du signal normalisé
    prix_norm_stds = []
    for _, g in df.groupby('gouvernorat'):
        prix = g['indice_prix_m2_regional'].values
        if len(prix) > 3:
            prix_n = (prix - prix.mean()) / (prix.std() + 1e-8)
            prix_norm_stds.append(prix_n.std())
    med_std      = float(np.median(prix_norm_stds)) if prix_norm_stds else 1.0
    penalty_mult = round(1.0 + med_std * 0.15, 3)   # ×0.15 (conservateur)
    penalty_mult = max(1.05, min(1.35, penalty_mult))

    return {
        'min_annonces' : min_ann,
        'min_signal'   : min_signal,
        'min_amplitude': min_amp,
        'penalty_mult' : penalty_mult,
    }


# ─── Préparation série ────────────────────────────────────────────────────────

def preparer_serie(df: pd.DataFrame, min_annonces: int) -> pd.DataFrame:
    """
    Agrège par (gouvernorat, annee, mois).
    Colonnes utilisées : gouvernorat, annee, mois,
                         indice_prix_m2_regional, glissement_immo_trim.
    """
    cols_ok = [c for c in ['indice_prix_m2_regional', 'glissement_immo_trim']
               if c in df.columns]
    grp = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg = (grp[cols_ok].mean()
             .join(grp.size().rename('n'))
             .reset_index())
    agg = agg[agg['n'] >= min_annonces].sort_values(
        ['gouvernorat', 'annee', 'mois'])
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                            .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    return agg


# ─── Seuil adaptatif par cluster ──────────────────────────────────────────────

def seuil_amplitude_cluster(agg: pd.DataFrame,
                             govs_in_cluster: list,
                             min_amplitude_global: float) -> float:
    """
    Seuil calibré sur les gouvernorats du cluster.
    min_amplitude_global (calculé dynamiquement) sert de plancher.
    """
    sub = agg[agg['gouvernorat'].isin(govs_in_cluster)].copy()
    sub['variation'] = sub.groupby('gouvernorat')['prix_lisse'].pct_change().abs()
    variations = sub['variation'].dropna()
    if len(variations) < 5:
        return min_amplitude_global
    seuil = float(variations.quantile(0.30))
    return round(max(min_amplitude_global, min(0.18, seuil)), 3)


# ─── Signal multivarié 3D ─────────────────────────────────────────────────────

def construire_signal(sub: pd.DataFrame, zscore_prix: float = 0.0) -> tuple:
    """
    Signal 3D :
      dim 1 : prix_lisse normalisé  (indice_prix_m2_regional)
      dim 2 : glissement_immo_trim normalisé × 0.35
      dim 3 : zscore_prix du cluster_map × 0.20
              (constante par gouvernorat = ancrage inter-cluster, voulu)
    """
    prix    = sub['prix_lisse'].values
    prix_n  = (prix - prix.mean()) / (prix.std() + 1e-8)

    gliss_n = np.zeros_like(prix_n)
    if 'glissement_immo_trim' in sub.columns:
        gliss   = sub['glissement_immo_trim'].fillna(0).values
        gliss_n = (gliss - gliss.mean()) / (gliss.std() + 1e-8)

    z_channel = np.full_like(prix_n, zscore_prix * 0.20)
    return np.column_stack([prix_n, gliss_n * 0.35, z_channel]), prix


# ─── Filtrage ruptures ────────────────────────────────────────────────────────

def filtrer_ruptures(bps: list, prix: np.ndarray,
                     seuil_amp: float) -> list:
    """
    Filtre amplitude + gap minimal (MIN_GAP).
    En cas de chevauchement, garde la rupture avec la plus grande amplitude.
    """
    bps_valides = []
    for bp in bps:
        if bp == 0 or bp >= len(prix):
            continue
        avant     = prix[:bp].mean()
        apres     = prix[bp:].mean()
        amplitude = abs(apres - avant) / (avant + 1e-8)
        if amplitude < seuil_amp:
            continue
        if bps_valides and (bp - bps_valides[-1]) < MIN_GAP:
            bp_prev  = bps_valides[-1]
            amp_prev = (abs(prix[bp_prev:].mean() - prix[:bp_prev].mean())
                        / (prix[:bp_prev].mean() + 1e-8))
            if amplitude > amp_prev:
                bps_valides[-1] = bp
        else:
            bps_valides.append(bp)
    return bps_valides


# ─── Score rupture ────────────────────────────────────────────────────────────

def scorer_rupture(bp: int, prix: np.ndarray,
                   n_obs: int, spread_reel: float) -> float:
    """
    score = amplitude × √n_obs × (1 + |spread_réel|)
    n_obs = nombre de périodes (annee×mois) de la série agrégée.
    """
    avant     = prix[:bp].mean()
    apres     = prix[bp:].mean()
    amplitude = abs(apres - avant) / (avant + 1e-8)
    return round(amplitude * np.sqrt(n_obs) * (1 + abs(spread_reel)), 4)


# ─── Dashboard par groupe ─────────────────────────────────────────────────────

def plot_groupe(groupe: str, agg: pd.DataFrame,
                ruptures_gov: dict, cluster_map_grp: dict,
                params: dict, output_path: str) -> None:
    """
    Dashboard 3 lignes :
      Row 0 : Timeseries prix_lisse + ruptures colorées par cluster
      Row 1 : Heatmap intensité (gov × année) | Bar ruptures par cluster
      Row 2 : Top 12 ruptures par score
    """
    govs_with_rup = [g for g, r in ruptures_gov.items() if r['n'] > 0]
    n_gov_plot    = min(len(govs_with_rup), 8)

    fig = plt.figure(figsize=(16, 14), facecolor='white')
    gs  = GridSpec(3, 2, hspace=0.45, wspace=0.32,
                   top=0.93, bottom=0.05, left=0.07, right=0.97)
    TITRES = {'Residentiel':'Résidentiel',
              'Foncier':'Foncier', 'Commercial':'Commercial'}
    titre  = TITRES.get(groupe, groupe)

    # Row 0 : Timeseries
    ax0 = fig.add_subplot(gs[0, :])
    colors_used = set()
    for gov in govs_with_rup[:n_gov_plot]:
        sub = agg[agg['gouvernorat'] == gov].reset_index(drop=True)
        if sub.empty:
            continue
        dates = pd.to_datetime(
            sub['annee'].astype(int).astype(str) + '-' +
            sub['mois'].astype(int).astype(str).str.zfill(2))
        cid   = cluster_map_grp.get(gov, {}).get('cluster', 0)
        color = CLUSTER_PALETTE.get(cid, '#888')
        nom   = GOUVERNORAT_DEC.get(int(gov), str(gov))
        label = f"{nom} (C{cid})" if cid not in colors_used else None
        colors_used.add(cid)
        ax0.plot(dates, sub['prix_lisse'], color=color, linewidth=1.6,
                 alpha=0.82, label=label)
        for bp in ruptures_gov[gov].get('bps', []):
            if bp < len(dates):
                ax0.axvline(dates.iloc[bp], color=color, linestyle='--',
                            alpha=0.5, linewidth=1.1)
    ax0.set_title(
        f'PELT B — {titre} | '
        f'MIN_AMP={params["min_amplitude"]*100:.1f}% '
        f'PENALTY={params["penalty_mult"]:.3f} (calculés depuis données)',
        fontsize=11, fontweight='700', color='#1A1A18')
    ax0.set_xlabel('Date', fontsize=9)
    ax0.set_ylabel('indice_prix_m2_regional lissé (TND/m²)', fontsize=9)
    ax0.legend(fontsize=8, loc='upper left', ncol=2, framealpha=0.85)
    ax0.spines[['top','right']].set_visible(False)
    ax0.set_facecolor('#F9F9F7'); ax0.grid(True, alpha=0.22, linewidth=0.5)

    # Row 1 gauche : Heatmap intensité
    ax1      = fig.add_subplot(gs[1, 0])
    govs_all = sorted(agg['gouvernorat'].unique())
    annees   = sorted(agg['annee'].unique())
    matrix   = np.zeros((len(govs_all), len(annees)))
    for i, gov in enumerate(govs_all):
        for date_str in ruptures_gov.get(gov, {}).get('dates', []):
            try:
                yr = int(date_str.split('-')[0])
                if yr in annees:
                    matrix[i, annees.index(yr)] += 1
            except Exception:
                pass
    cmap_rup = LinearSegmentedColormap.from_list(
        'rup', ['#F9F9F7', '#EF9F27', '#D85A30', '#8B0000'])
    im = ax1.imshow(matrix, cmap=cmap_rup, aspect='auto',
                    vmin=0, vmax=max(matrix.max(), 1))
    plt.colorbar(im, ax=ax1, fraction=0.04, pad=0.03, label='Nb ruptures')
    ax1.set_xticks(range(len(annees)))
    ax1.set_xticklabels(annees, fontsize=8, rotation=45)
    ax1.set_yticks(range(len(govs_all)))
    ax1.set_yticklabels(
        [GOUVERNORAT_DEC.get(int(g), str(g)) for g in govs_all], fontsize=7)
    ax1.set_title('Intensité ruptures (gouvernorat × année)',
                  fontsize=10, fontweight='600')

    # Row 1 droite : Bar par cluster
    ax1b = fig.add_subplot(gs[1, 1])
    cluster_counts = {}
    for gov, r in ruptures_gov.items():
        cid = cluster_map_grp.get(gov, {}).get('cluster', -1)
        cluster_counts[cid] = cluster_counts.get(cid, 0) + r.get('n', 0)
    if cluster_counts:
        sc = sorted(cluster_counts.keys())
        ax1b.bar([f'C{c}' for c in sc],
                 [cluster_counts[c] for c in sc],
                 color=[CLUSTER_PALETTE.get(c, GREY) for c in sc],
                 edgecolor='white', linewidth=1.5)
        for i, c in enumerate(sc):
            v = cluster_counts[c]
            if v > 0:
                ax1b.text(i, v + 0.05, str(v), ha='center',
                          fontsize=10, fontweight='600', color='#1A1A18')
    ax1b.set_ylabel('Nb ruptures', fontsize=9)
    ax1b.set_title('Ruptures par cluster', fontsize=10, fontweight='600')
    ax1b.spines[['top','right']].set_visible(False)
    ax1b.set_facecolor('#F9F9F7'); ax1b.tick_params(colors='#888780')

    # Row 2 : Top 12 par score
    ax2 = fig.add_subplot(gs[2, :])
    top_scores = []
    for gov, r in ruptures_gov.items():
        cid = cluster_map_grp.get(gov, {}).get('cluster', 0)
        nom = GOUVERNORAT_DEC.get(int(gov), str(gov))
        for date_str, score in zip(r.get('dates', []), r.get('scores', [])):
            top_scores.append({
                'label'  : f"{nom}\n{date_str}",
                'score'  : score,
                'cluster': cid,
                'delta'  : r.get('delta_prix', 0),
            })
    top_scores.sort(key=lambda x: x['score'], reverse=True)
    top12 = top_scores[:12]
    if top12:
        ax2.barh(range(len(top12)), [t['score'] for t in top12],
                 color=[CLUSTER_PALETTE.get(t['cluster'], GREY) for t in top12],
                 edgecolor='white', linewidth=1.2)
        ax2.set_yticks(range(len(top12)))
        ax2.set_yticklabels([t['label'] for t in top12], fontsize=8)
        ax2.invert_yaxis()
        for i, t in enumerate(top12):
            ax2.text(t['score'] + 0.005, i,
                     f"Δ {t['delta']:+.0f} TND | score={t['score']:.3f}",
                     va='center', fontsize=7.5, color='#333330')
        ax2.set_xlabel('Score (amplitude × √n × |spread_réel|)', fontsize=9)
        ax2.set_title('Top ruptures les plus significatives',
                      fontsize=10, fontweight='600')
        ax2.spines[['top','right']].set_visible(False)
        ax2.set_facecolor('#F9F9F7')

    patches = [mpatches.Patch(color=CLUSTER_PALETTE.get(c, GREY),
                              label=f'Cluster {c}')
               for c in sorted(set(v.get('cluster', 0)
                                   for v in cluster_map_grp.values()))]
    fig.legend(handles=patches, loc='lower center', ncol=len(patches),
               fontsize=9, framealpha=0.85, bbox_to_anchor=(0.5, 0.005))

    fig.suptitle(
        f'PELT B — {titre} | Signal 3D · Seuil adaptatif par cluster\n'
        f'Params dynamiques : MIN_ANN={params["min_annonces"]} '
        f'MIN_SIG={params["min_signal"]} '
        f'MIN_AMP={params["min_amplitude"]*100:.1f}% '
        f'PEN={params["penalty_mult"]:.3f}',
        fontsize=12, fontweight='700', color='#1A1A18', y=0.98,
    )
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close(fig)
    print(f"  → Dashboard : {output_path}")


# ─── Pipeline PELT ────────────────────────────────────────────────────────────

def run_pelt(groupes: dict, cluster_map: dict) -> dict:
    """
    PELT par cluster. Tous les paramètres calculés depuis les données.
    GOV_NAMES = GOUVERNORAT_DEC de mappings_BO3 — aucune liste manuelle.
    """
    print()
    print("=" * 80)
    print("  PELT A | Paramètres 100% dynamiques "
          "(MIN_ANN, MIN_SIG, MIN_AMP, PENALTY×0.15)")
    print("  GOV_NAMES = GOUVERNORAT_DEC depuis mappings_BO3")
    print("=" * 80)

    resultats_global = {}
    nb_total         = 0
    rapport_rows     = []

    for groupe in GROUPES_ACTIFS:
        df = groupes.get(groupe)
        if df is None:
            print(f"\n  ✗ {groupe} absent"); continue

        print(f"\n{'─'*68}\n  {groupe.upper()} ({len(df):,} obs)")

        params = compute_pelt_params(df)
        print(f"  Paramètres calculés depuis les données :")
        print(f"    MIN_ANNONCES  = {params['min_annonces']}  "
              f"(p5 obs par période)")
        print(f"    MIN_SIGNAL    = {params['min_signal']}  "
              f"(p10 points temporels/gov)")
        print(f"    MIN_AMPLITUDE = {params['min_amplitude']*100:.1f}%  "
              f"(p25 variations prix)")
        print(f"    PENALTY_MULT  = {params['penalty_mult']:.3f}  "
              f"(1.0 + std_median × 0.15)")

        agg             = preparer_serie(df, params['min_annonces'])
        cluster_map_grp = cluster_map.get(groupe, {})
        ruptures_gov    = {}

        clusters_govs: dict = {}
        for gov_id, info in cluster_map_grp.items():
            clusters_govs.setdefault(info['cluster'], []).append(gov_id)
        if not clusters_govs:
            for gov in agg['gouvernorat'].unique():
                clusters_govs.setdefault(0, []).append(int(gov))

        resultats_cluster: dict = {}

        for cluster_id, govs_cluster in sorted(clusters_govs.items()):
            noms_cluster = [GOUVERNORAT_DEC.get(g, str(g))
                            for g in govs_cluster]
            print(f"\n  ● Cluster {cluster_id} "
                  f"({len(govs_cluster)} gov) : {noms_cluster}")

            seuil_amp = seuil_amplitude_cluster(
                agg, govs_cluster, params['min_amplitude'])
            print(f"    Seuil amplitude cluster : {seuil_amp*100:.1f}%")

            ruptures_cluster = []

            for gov in sorted(govs_cluster):
                sub = agg[agg['gouvernorat'] == gov].reset_index(drop=True)
                if len(sub) < params['min_signal']:
                    continue

                gov_info  = cluster_map_grp.get(gov, {})
                zscore_p  = gov_info.get('zscore_prix', 0.0)
                spread_r  = gov_info.get('spread_reel', 0.0)
                n_obs_gov = len(sub)
                nom       = GOUVERNORAT_DEC.get(int(gov), str(gov))

                try:
                    signal, prix = construire_signal(sub, zscore_p)
                    n = len(prix)
                    cluster_factor = max(0.9, min(1.3, len(govs_cluster) / 3))
                    pen = (params['penalty_mult'] * cluster_factor
                           * np.log(n) * max(signal.std(), 0.5))

                    algo = rpt.Pelt(model="rbf", min_size=MIN_SIZE, jump=1)
                    algo.fit(signal)
                    bps_raw = [b for b in algo.predict(pen=pen) if 0 < b < n]
                    bps     = filtrer_ruptures(bps_raw, prix, seuil_amp)

                    if bps:
                        dates  = [f"{int(sub.iloc[bp]['annee'])}-"
                                  f"{int(sub.iloc[bp]['mois']):02d}"
                                  for bp in bps]
                        scores = [scorer_rupture(bp, prix, n_obs_gov, spread_r)
                                  for bp in bps]
                        delta  = abs(prix[bps[-1]:].mean() - prix[:bps[0]].mean())

                        ruptures_gov[gov] = {
                            'n': len(bps), 'dates': dates, 'bps': bps,
                            'scores': scores,
                            'delta_prix': round(delta, 0),
                            'cluster': cluster_id,
                        }
                        ruptures_cluster.append({
                            'gouvernorat': gov, 'nom': nom,
                            'n': len(bps), 'dates': dates, 'scores': scores,
                            'delta_prix': round(delta, 0),
                            'seuil_amp': seuil_amp,
                        })
                        for d, sc in zip(dates, scores):
                            rapport_rows.append({
                                'groupe'                : groupe,
                                'gouvernorat'           : gov,
                                'nom'                   : nom,
                                'cluster'               : cluster_id,
                                'date_rupture'          : d,
                                'score'                 : sc,
                                'delta_prix'            : round(delta, 0),
                                'seuil_amp'             : seuil_amp,
                                'zscore_prix'           : round(zscore_p, 4),
                                'min_amplitude_calcule' : params['min_amplitude'],
                                'penalty_mult_calcule'  : params['penalty_mult'],
                            })
                        nb_total += len(bps)
                        print(f"    ✓ {nom:<16} → {len(bps)} rupture(s) | "
                              f"Δ ≈ {delta:5.0f} TND | "
                              f"score_max={max(scores):.3f}")
                    else:
                        ruptures_gov[gov] = {
                            'n':0,'dates':[],'bps':[],'scores':[],
                            'delta_prix':0,'cluster':cluster_id,
                        }

                except Exception:
                    import traceback; traceback.print_exc()
                    ruptures_gov[gov] = {
                        'n':0,'dates':[],'bps':[],'scores':[],
                        'delta_prix':0,'cluster':cluster_id,
                    }

            n_cl = sum(r['n'] for r in ruptures_cluster)
            print(f"    → Total cluster {cluster_id} : {n_cl} ruptures")
            resultats_cluster[cluster_id] = ruptures_cluster

        n_grp = sum(r.get('n', 0) for r in ruptures_gov.values())
        print(f"\n  ✓ {groupe} total : {n_grp} ruptures validées")
        resultats_global[groupe] = resultats_cluster

        plot_path = os.path.join(GRAPHS_DIR, f'pelt_A_{groupe.lower()}.png')
        try:
            plot_groupe(groupe, agg, ruptures_gov,
                        cluster_map_grp, params, plot_path)
        except Exception:
            import traceback; traceback.print_exc()

    pkl_path = os.path.join(MODELS_DIR, 'pelt_A.pkl')
    with open(pkl_path, 'wb') as f:
        pickle.dump(resultats_global, f)

    csv_path = os.path.join(MODELS_DIR, 'pelt_A_rapport.csv')
    if rapport_rows:
        (pd.DataFrame(rapport_rows)
           .sort_values(['groupe','cluster','score'],
                        ascending=[True,True,False])
           .to_csv(csv_path, index=False))

    print()
    print("=" * 80)
    print(f"  ✓ Total ruptures : {nb_total}")
    print(f"  → pelt_A.pkl         : {pkl_path}")
    print(f"  → pelt_A_rapport.csv : {csv_path}")
    print(f"  → Graphiques         : {GRAPHS_DIR}")
    print("=" * 80)

    return {
        'n_ruptures': nb_total, 'pkl': pkl_path,
        'csv': csv_path, 'details': resultats_global,
    }


# ─── Point d'entrée ──────────────────────────────────────────────────────────

if __name__ == '__main__':
    cluster_map_path = os.path.join(MODELS_DIR, 'cluster_map.pkl')
    if not os.path.exists(cluster_map_path):
        print(f"⚠  cluster_map.pkl introuvable : {cluster_map_path}")
        print("   → Exécutez d'abord : python kmeans_A.py")
        exit(1)

    with open(cluster_map_path, 'rb') as f:
        cluster_map = pickle.load(f)
    print(f"✔  cluster_map chargé")
    for grp, mp in cluster_map.items():
        clusters = sorted(set(v['cluster'] for v in mp.values()))
        noms_cl  = {
            c: [GOUVERNORAT_DEC.get(g, str(g))
                for g, v in mp.items() if v['cluster'] == c]
            for c in clusters
        }
        print(f"   {grp:<14} : {len(mp)} gov | clusters={noms_cl}")

    dossier = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     '..', '..', 'BO3'))
    print(f"\nDossier : {dossier}\n")

    groupes = {}
    for nom in GROUPES_ACTIFS:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f"  ✔ {nom:<14} : {len(groupes[nom]):,} lignes")
        else:
            print(f"  ✗ {nom} manquant")

    if groupes:
        run_pelt(groupes, cluster_map)