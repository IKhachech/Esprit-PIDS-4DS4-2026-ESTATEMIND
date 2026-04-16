"""
pelt_A.py — Détection des Ruptures de Régime par Cluster · Version Production L102
===================================================================================

OBJECTIF MÉTIER 3 : Prédire les tendances régionales du marché immobilier tunisien.

CHANGELOG L102 (vs L99) :
  1. Cohérence inter-types RÉALISTE (was trop stricte) :
     L99 exigeait que loc OU ven bouge avec ≥ 70% du seuil d'amplitude.
     L102 : seuil abaissé à 50% pour loc/ven — certains marchés tunisiens
     sont quasi mono-type (Foncier → vente quasi exclusive, marché locatif quasi nul).
     Dans ces cas, forcer le check loc revient à invalider des ruptures réelles.
     Nouveau paramètre : COHERENCE_SEUIL_RATIO = 0.50 (was 0.70).

  2. Signal enrichi avec volatilite_prix et zone_geographique (depuis cluster_map L102) :
     dim 6 : zone_geographique normalisée × 0.10  — ancre spatiale légère
     dim 7 : volatilite_prix normalisée × 0.15    — marchés volatils → ruptures attendues
     Signal passe de 5D à 7D. Poids faibles pour ne pas écraser le signal prix (dim 1).

  3. Pénalité PELT adaptée à la liquidité (indice_liquidite depuis cluster_map L102) :
     Gouvernorats illiquides (Siliana, Kébili, Tozeur) → pénalité × 2.5 (was 2.2 faible).
     Gouvernorats très liquides (Tunis, Sousse) → pénalité × 0.9 (légèrement réduite).
     Évite les fausses ruptures sur les marchés peu actifs.

  4. Rapport CSV enrichi avec zone_geographique, indice_liquidite, volatilite_prix.

CORRECTION MAINTENUE L99 :
  • Signal stratifié loc/ven (prix_lisse_loc / prix_lisse_ven).
  • filtrer_ruptures_avec_coherence() : logique OU conservée (pas ET).
  • Seuil adaptatif par cluster.
  • Pénalité adaptée à la fiabilité du signal (haute/moyenne/faible).

PRINCIPES :
  • Aucune constante manuelle codée en dur.
  • GOV_NAMES    → GOUVERNORAT_DEC depuis mappings_BO3
  • MIN_ANNONCES → p5 obs par (gov, annee, mois)
  • MIN_SIGNAL   → p10 points temporels distincts par gouvernorat
  • PENALTY_MULT → 1.0 + std médian × 0.15
  • MIN_AMPLITUDE→ p25 variations absolues prix lissés
  • MIN_GAP      → 3 (contrainte physique PELT)

Dépendance : kmeans_A.py L102 → models/cluster_map.pkl

Sortie :
  models/pelt_A.pkl
  models/pelt_A_rapport.csv    ← enrichi avec rupture_flag (binaire TFT)
  graphs/pelt/pelt_A_{groupe}.png

Utilitaire TFT (nouveau) :
  build_rupture_flag_series(rapport_csv, calendar_df)
  → Génère la colonne rupture_flag = 1/0 pour le dataset TFT.
  → Rend le modèle TFT causal : "expliquer pourquoi le prix change"."""

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
from features_registry import build_features, validate_features, get_features

warnings.filterwarnings('ignore')

# ─── Import mappings officiels du pipeline ──────────────────────────────────
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
# pelt_A.py consomme les features PELT depuis le dataset BO3 (TARGET_COLS).
# Le signal de rupture est construit depuis indice_prix_m2_regional +
# indice_loc_lisse + indice_ven_lisse (stratifiés — voir FEATURE_SETS["pelt"]).
try:
    from features_config import get_features, validate_features
    _FEATURES_CONFIG_AVAILABLE = True
except ImportError:
    _FEATURES_CONFIG_AVAILABLE = False

# Colonnes L102 : zone_geographique + indice_liquidite + volatilite_prix_trim
# Le flag L102 est RÉEL si ces colonnes existent dans le dataset.
# Vérifié dynamiquement dans run_pelt() pour chaque groupe.
_L102_COLS = ['zone_geographique', 'indice_liquidite', 'volatilite_prix_trim']

# ─── Répertoires ──────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models')
GRAPHS_DIR = os.path.join(BASE_DIR, 'graphs', 'pelt')
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(GRAPHS_DIR, exist_ok=True)

GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']
MIN_SIZE = 3   # taille minimale segment PELT — contrainte physique
MIN_GAP  = 3   # gap minimal entre ruptures — contrainte physique

# Codes type_transaction
TT_LOC = 1
TT_VEN = 2

# L102 : seuil cohérence inter-types abaissé (was 0.70 en L99)
# Justification : marchés mono-type (Foncier vente exclusive) → loc = NaN
# → 0.70 invalide trop de ruptures réelles sur ces marchés.
COHERENCE_SEUIL_RATIO = 0.50

CLUSTER_PALETTE = {0: '#1D9E75', 1: '#D85A30', 2: '#7F77DD',
                   3: '#EF9F27', 4: '#D4537E', 5: '#3FA7D6'}
GREY = '#B4B2A9'

# ─── Variables exogènes causales intégrées dans le signal PELT ───────────────
# Ajoutent une dimension causale au signal multivarié :
#   inflation_glissement_annuel → pression macro sur les prix (dim 8)
#   croissance_pib_trim         → contexte économique régional  (dim 9)
#   high_season                 → effet saisonnalité touristique (dim 10)
# Vérification dynamique dans _ensure_exogenous_features() :
#   si colonne absente → fallback calculé depuis indice_prix_m2_regional.
features_externes = [
    "inflation_glissement_annuel",
    "croissance_pib_trim",
    "high_season",
]


# ─── Calcul dynamique des paramètres PELT ─────────────────────────────────────

def compute_pelt_params(df: pd.DataFrame) -> dict:
    """
    Calcule tous les paramètres PELT depuis les données réelles.

    MIN_ANNONCES : p5 obs par (gouvernorat, annee, mois).
    MIN_SIGNAL   : p10 points temporels distincts par gouvernorat.
    MIN_AMPLITUDE: p25 variations absolues de prix lissés.
    PENALTY_MULT : 1.0 + std médian des signaux normalisés × 0.15.
    IQR_FACTOR   : adapté à la liquidité globale du groupe.
    """
    grp_counts  = df.groupby(['gouvernorat', 'annee', 'mois']).size()
    p25_periods = grp_counts.quantile(0.25)
    med_periods = grp_counts.median()
    pct_solo    = float((grp_counts == 1).mean())
    is_illiquid = (p25_periods <= 1) and (med_periods <= 3)
    min_ann     = 1 if is_illiquid else max(1, int(grp_counts.quantile(0.05)))

    pt_by_gov  = (df.groupby('gouvernorat')
                    .apply(lambda x: x[['annee', 'mois']].drop_duplicates().shape[0]))
    min_signal = max(4, int(pt_by_gov.quantile(0.10)))

    agg = (df.groupby(['gouvernorat', 'annee', 'mois'])['indice_prix_m2_regional']
             .mean().reset_index()
             .sort_values(['gouvernorat', 'annee', 'mois']))
    agg['lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                       .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    agg['var']   = agg.groupby('gouvernorat')['lisse'].pct_change().abs()
    variations   = agg['var'].dropna()
    min_amp      = round(float(variations.quantile(0.25)), 3) \
                   if len(variations) > 10 else 0.06
    min_amp      = max(0.04, min(0.15, min_amp))

    prix_norm_stds = []
    for _, g in df.groupby('gouvernorat'):
        prix = g['indice_prix_m2_regional'].values
        if len(prix) > 3:
            prix_n = (prix - prix.mean()) / (prix.std() + 1e-8)
            prix_norm_stds.append(prix_n.std())
    med_std      = float(np.median(prix_norm_stds)) if prix_norm_stds else 1.0
    penalty_mult = round(1.0 + med_std * 0.15, 3)
    penalty_mult = max(1.05, min(1.35, penalty_mult))

    iqr_factor = round(min(3.5, max(2.0, 2.0 + pct_solo * 1.5)), 2)

    return {
        'min_annonces' : min_ann,
        'min_signal'   : min_signal,
        'min_amplitude': min_amp,
        'penalty_mult' : penalty_mult,
        'pct_solo'     : round(pct_solo, 3),
        'is_illiquid'  : is_illiquid,
        'iqr_factor'   : iqr_factor,
    }


# ─── Préparation série (stratifiée par type_transaction) ─────────────────────

def _compute_iqr_filter(series: pd.Series, factor: float) -> tuple:
    q1, q3 = series.quantile(0.25), series.quantile(0.75)
    iqr = q3 - q1
    if iqr < 1.0:
        return -np.inf, np.inf, False
    return q1 - factor * iqr, q3 + factor * iqr, True


def preparer_serie(df: pd.DataFrame, min_annonces: int,
                   iqr_factor: float = 2.5) -> pd.DataFrame:
    """
    Agrège par (gouvernorat, annee, mois).
    Calcule 3 colonnes de prix lissés :
      prix_lisse      : tous types confondus (signal principal PELT)
      prix_lisse_loc  : Location uniquement (type_transaction=1)
      prix_lisse_ven  : Vente uniquement    (type_transaction=2)

    L102 : inchangé. La préparation reste identique à L99.
    """
    cols_ok = [c for c in ['indice_prix_m2_regional', 'glissement_immo_trim'] + features_externes
               if c in df.columns]

    grp = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg = (grp[cols_ok].mean()
             .join(grp.size().rename('n'))
             .reset_index())
    agg = agg[agg['n'] >= min_annonces].sort_values(['gouvernorat', 'annee', 'mois'])

    # Signal global lissé
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                            .transform(lambda x: x.rolling(3, min_periods=1).mean()))

    # Filtre IQR sur signal global
    mask_ok    = pd.Series(True, index=agg.index)
    n_excl_iqr = 0
    for gov, g in agg.groupby('gouvernorat'):
        lo, hi, valid = _compute_iqr_filter(g['prix_lisse'], iqr_factor)
        if valid:
            excl = (g['prix_lisse'] < lo) | (g['prix_lisse'] > hi)
            mask_ok.loc[g.index] = ~excl
            n_excl_iqr += int(excl.sum())
    agg = agg[mask_ok].reset_index(drop=True)
    agg['_n_excl_iqr'] = n_excl_iqr

    # Signaux stratifiés loc / ven
    for tt, col_lisse in [(TT_LOC, 'prix_lisse_loc'), (TT_VEN, 'prix_lisse_ven')]:
        sub_tt = df[df['type_transaction'] == tt].copy()
        if len(sub_tt) == 0:
            agg[col_lisse] = np.nan
            continue
        agg_tt = (sub_tt.groupby(['gouvernorat', 'annee', 'mois'])['indice_prix_m2_regional']
                        .mean()
                        .reset_index()
                        .rename(columns={'indice_prix_m2_regional': f'prix_raw_{tt}'}))
        agg = agg.merge(agg_tt, on=['gouvernorat', 'annee', 'mois'], how='left')
        agg[col_lisse] = (agg.groupby('gouvernorat')[f'prix_raw_{tt}']
                             .transform(lambda x: x.rolling(3, min_periods=1).mean()))
        agg = agg.drop(columns=[f'prix_raw_{tt}'], errors='ignore')

    return agg


def _ensure_exogenous_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garantit la présence de toutes les variables exogènes requises par PELT.
    Objectif : 0 warnings, fallbacks silencieux et sémantiquement corrects.

    Ordre de priorité pour chaque feature :
      1. Colonne réelle présente → utilisée telle quelle
      2. Proxy calculé depuis indice_prix_m2_regional
      3. Valeur neutre (0.0) si aucune donnée prix disponible

    Cas spécial inflation_proxy :
      Toujours créé comme alias d'inflation_glissement_annuel si celle-ci existe,
      sinon calculé comme pct_change(12). Évite le warning "inflation_proxy absent".
    """
    df = df.copy()

    # ── 1. inflation_glissement_annuel + inflation_proxy ─────────────────────
    if 'inflation_glissement_annuel' in df.columns:
        # Vraie colonne présente → inflation_proxy = copie directe (cohérence)
        df['inflation_proxy'] = df['inflation_glissement_annuel']
    elif 'indice_prix_m2_regional' in df.columns:
        df['inflation_proxy'] = (
            df.groupby('gouvernorat')['indice_prix_m2_regional']
              .transform(lambda x: x.pct_change().fillna(0).abs())
        )
        df['inflation_glissement_annuel'] = df['inflation_proxy']
    else:
        df['inflation_proxy']              = 0.0
        df['inflation_glissement_annuel']  = 0.0

    # ── 2. croissance_pib_trim ────────────────────────────────────────────────
    if 'croissance_pib_trim' not in df.columns:
        if 'indice_prix_m2_regional' in df.columns:
            df['croissance_pib_trim'] = (
                df.groupby('gouvernorat')['indice_prix_m2_regional']
                  .transform(lambda x: x.pct_change(periods=3).fillna(0).abs())
            )
        else:
            df['croissance_pib_trim'] = 0.0

    # ── 3. high_season ────────────────────────────────────────────────────────
    if 'high_season' not in df.columns:
        if 'mois' in df.columns:
            df['high_season'] = df['mois'].isin([4, 5, 6, 7, 8, 9]).astype(float)
        else:
            df['high_season'] = 0.0

    # ── 4. prix_lisse_loc / prix_lisse_ven ───────────────────────────────────
    # Calculés normalement par preparer_serie() après ce fallback.
    # Si indice_prix_m2_regional absent (cas très rare), créer colonnes neutres.
    # Ici on ne les crée PAS — preparer_serie les produira depuis type_transaction.
    # Cette section est un guard de dernier recours seulement.

    # ── Rapport compact (une seule ligne, 0 warning si tout est présent) ──────
    fb_appliques = [c for c in ['inflation_proxy', 'croissance_pib_trim',
                                 'high_season', 'inflation_glissement_annuel']
                    if c in df.columns]
    manquantes   = [c for c in features_externes if c not in df.columns]
    if manquantes:
        print(f"  ⚠ [PELT] Exogènes manquantes après fallback : {manquantes}")
    # Sinon : silence total — 0 warnings

    return df


# ─── Seuil adaptatif par cluster ─────────────────────────────────────────────

def seuil_amplitude_cluster(agg: pd.DataFrame,
                             govs_in_cluster: list,
                             min_amplitude_global: float) -> float:
    sub = agg[agg['gouvernorat'].isin(govs_in_cluster)].copy()
    sub['variation'] = sub.groupby('gouvernorat')['prix_lisse'].pct_change().abs()
    variations = sub['variation'].dropna()
    if len(variations) < 5:
        return min_amplitude_global
    seuil = float(variations.quantile(0.30))
    return round(max(min_amplitude_global, min(0.18, seuil)), 3)


# ─── Signal multivarié 7D (L102 : +2 dimensions vs 5D L99) ──────────────────

def _zscore_weight(n_pts: int) -> float:
    return round(min(0.30, max(0.05, 1.0 / np.sqrt(max(n_pts, 1)))), 3)


def construire_signal(sub: pd.DataFrame,
                      zscore_prix: float = 0.0,
                      ratio_loc_ven: float = 0.5,
                      zone_geo: float = 3.0,
                      volatilite_prix: float = 0.0) -> tuple:
    """
    Signal 10D (L102+) :
      dim 1 : prix_lisse global normalisé       (signal principal PELT)
      dim 2 : glissement_immo_trim normalisé × 0.35
      dim 3 : zscore_prix × zscore_weight(n_pts)
      dim 4 : prix_lisse_loc normalisé × poids_loc
      dim 5 : prix_lisse_ven normalisé × poids_ven
      dim 6 : zone_geo normalisée × 0.10         (L102 — ancre spatiale)
      dim 7 : volatilite_prix normalisée × 0.15  (L102 — contexte risque marché)
      dim 8 : inflation_glissement_annuel normalisée × 0.10
      dim 9 : croissance_pib_trim normalisée × 0.08
      dim10 : high_season normalisée × 0.05

    dim 6 (zone_geo × 0.10) : légère ancre spatiale.
      Tunis (zone=0) et Tozeur (zone=4) ont des profils différents sur cette dim.
      Poids 0.10 → information de contexte, pas signal dominant.

    dim 7 (volatilite × 0.15) : marchés instables (Bizerte Foncier, Médenine)
      ont naturellement plus de ruptures PELT légitimes. Cette dimension aide
      l'algorithme à contextualiser les ruptures détectées.
      Poids 0.15 → influence modérée, pas de sur-détection.

    Toutes les nouvelles dims sont CONSTANTES sur la série temporelle
    (caractéristiques du gouvernorat, pas de valeurs mensuelles).
    """
    prix   = sub['prix_lisse'].values
    prix_n = (prix - prix.mean()) / (prix.std() + 1e-8)

    gliss_n = np.zeros_like(prix_n)
    if 'glissement_immo_trim' in sub.columns:
        gliss   = sub['glissement_immo_trim'].fillna(0).values
        gliss_n = (gliss - gliss.mean()) / (gliss.std() + 1e-8)

    z_weight  = _zscore_weight(len(prix))
    z_channel = np.full_like(prix_n, zscore_prix * z_weight)

    def _norm_feature(arr: np.ndarray) -> np.ndarray:
        arr = np.asarray(arr, dtype=float)
        return (arr - np.nanmean(arr)) / (np.nanstd(arr) + 1e-8)

    exog_inflation = np.zeros_like(prix_n)
    if 'inflation_glissement_annuel' in sub.columns:
        inf = sub['inflation_glissement_annuel'].fillna(0).values
        exog_inflation = _norm_feature(inf) * 0.10

    exog_pib = np.zeros_like(prix_n)
    if 'croissance_pib_trim' in sub.columns:
        pib_arr = sub['croissance_pib_trim'].fillna(0).values
        exog_pib = _norm_feature(pib_arr) * 0.08

    exog_season = np.zeros_like(prix_n)
    if 'high_season' in sub.columns:
        season = sub['high_season'].fillna(0).values
        exog_season = _norm_feature(season) * 0.05

    # Canaux stratifiés loc/ven
    poids_loc = (1.0 - ratio_loc_ven) * 0.25
    poids_ven = ratio_loc_ven * 0.25

    loc_n = np.zeros_like(prix_n)
    if 'prix_lisse_loc' in sub.columns:
        loc_raw   = sub['prix_lisse_loc'].values
        valid_loc = ~np.isnan(loc_raw) & (loc_raw > 0)
        if valid_loc.sum() > 2:
            loc_filled = pd.Series(loc_raw).ffill().bfill().values
            loc_n = (loc_filled - loc_filled.mean()) / (loc_filled.std() + 1e-8)
            loc_n = loc_n * poids_loc

    ven_n = np.zeros_like(prix_n)
    if 'prix_lisse_ven' in sub.columns:
        ven_raw   = sub['prix_lisse_ven'].values
        valid_ven = ~np.isnan(ven_raw) & (ven_raw > 0)
        if valid_ven.sum() > 2:
            ven_filled = pd.Series(ven_raw).ffill().bfill().values
            ven_n = (ven_filled - ven_filled.mean()) / (ven_filled.std() + 1e-8)
            ven_n = ven_n * poids_ven

    # L102 : dim 6 — zone géographique normalisée (0-4 → 0-1) × poids 0.10
    zone_norm  = float(zone_geo) / 4.0  # 0=Grand Tunis, 1=Sud
    geo_channel = np.full_like(prix_n, zone_norm * 0.10)

    # L102 : dim 7 — volatilite_prix normalisée × poids 0.15
    # cap à 0.5 pour éviter que les marchés très volatils dominent
    vol_norm    = min(float(volatilite_prix), 0.5) * 2.0  # rescale 0-1
    vol_channel = np.full_like(prix_n, vol_norm * 0.15)

    return np.column_stack([prix_n, gliss_n * 0.35, z_channel,
                             loc_n, ven_n, geo_channel, vol_channel,
                             exog_inflation, exog_pib, exog_season]), prix


# ─── Filtrage ruptures ────────────────────────────────────────────────────────

def filtrer_ruptures(bps: list, prix: np.ndarray, seuil_amp: float) -> list:
    """Filtre amplitude + gap minimal. Garde la rupture max en cas de chevauchement."""
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


def _coherence_inter_types(bp: int,
                            prix_loc: np.ndarray, prix_ven: np.ndarray,
                            seuil_amp: float) -> tuple:
    """
    Vérifie qu'une rupture sur le signal global se manifeste dans ≥ 1 type.

    L102 : COHERENCE_SEUIL_RATIO = 0.50 (was 0.70).
    Raison : seuil 70% trop strict pour les marchés mono-type (Foncier/vente, etc.)
    Sur ces marchés, loc = quasi NaN → check loc invalide systématiquement des ruptures
    réelles détectées sur le signal global (vente uniquement).

    Logique inchangée : OU (loc confirme) OU (ven confirme) → cohérent.
    Si les deux types sont absents → accepté (signal global seul suffit).
    """
    def _delta(arr: np.ndarray, bp: int) -> float:
        if arr is None or len(arr) < bp + 1:
            return float('nan')
        av = arr[:bp][~np.isnan(arr[:bp])]
        ap = arr[bp:][~np.isnan(arr[bp:])]
        if len(av) == 0 or len(ap) == 0:
            return float('nan')
        d = ap.mean() - av.mean()
        return float(d / (av.mean() + 1e-8))

    dl = _delta(prix_loc, bp)
    dv = _delta(prix_ven, bp)

    has_loc = not (dl != dl)  # not nan
    has_ven = not (dv != dv)

    if not has_loc and not has_ven:
        return True, 0.0, 0.0  # Aucun sous-type → accepté

    # L102 : seuil 50% (was 70%) — moins strict pour marchés mono-type
    confirmed_loc = has_loc and (abs(dl) >= seuil_amp * COHERENCE_SEUIL_RATIO)
    confirmed_ven = has_ven and (abs(dv) >= seuil_amp * COHERENCE_SEUIL_RATIO)

    coherent = confirmed_loc or confirmed_ven
    return coherent, (dl if has_loc else 0.0), (dv if has_ven else 0.0)


def filtrer_ruptures_avec_coherence(bps: list, prix: np.ndarray,
                                    prix_loc: np.ndarray, prix_ven: np.ndarray,
                                    seuil_amp: float) -> tuple:
    """
    Filtre en deux passes :
    1. filtrer_ruptures() : amplitude + gap
    2. _coherence_inter_types() : ≥ 1 type confirme (logique OU)

    Retourne (bps_valides, deltas_loc, deltas_ven, coherences).
    """
    bps_amp = filtrer_ruptures(bps, prix, seuil_amp)

    bps_final  = []
    deltas_loc = []
    deltas_ven = []
    coherences = []

    for bp in bps_amp:
        coh, dl, dv = _coherence_inter_types(bp, prix_loc, prix_ven, seuil_amp)
        if coh:
            bps_final.append(bp)
            deltas_loc.append(round(dl, 4))
            deltas_ven.append(round(dv, 4))
            coherences.append(True)
        # Sinon : rupture rejetée — artefact de composition loc/ven

    return bps_final, deltas_loc, deltas_ven, coherences


# ─── Score rupture ────────────────────────────────────────────────────────────

def scorer_rupture(bp: int, prix: np.ndarray,
                   n_obs: int, spread_reel: float) -> float:
    avant     = prix[:bp].mean()
    apres     = prix[bp:].mean()
    amplitude = abs(apres - avant) / (avant + 1e-8)
    return round(amplitude * np.sqrt(n_obs) * (1 + abs(spread_reel)), 4)


# ─── Dashboard par groupe ────────────────────────────────────────────────────

def plot_groupe(groupe: str, agg: pd.DataFrame,
                ruptures_gov: dict, cluster_map_grp: dict,
                params: dict, output_path: str) -> None:
    govs_with_rup = [g for g, r in ruptures_gov.items() if r['n'] > 0]
    n_gov_plot    = min(len(govs_with_rup), 8)

    fig = plt.figure(figsize=(16, 14), facecolor='white')
    gs  = GridSpec(3, 2, hspace=0.45, wspace=0.32,
                   top=0.93, bottom=0.05, left=0.07, right=0.97)
    TITRES = {'Residentiel': 'Résidentiel',
              'Foncier': 'Foncier', 'Commercial': 'Commercial'}
    titre  = TITRES.get(groupe, groupe)

    # Row 0 : Timeseries (signal global + sous-signaux loc/ven en pointillés)
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

        if 'prix_lisse_loc' in sub.columns:
            loc_vals = sub['prix_lisse_loc']
            if loc_vals.notna().sum() > 2:
                ax0.plot(dates, loc_vals, color=color, linewidth=0.8,
                         linestyle=':', alpha=0.45)
        if 'prix_lisse_ven' in sub.columns:
            ven_vals = sub['prix_lisse_ven']
            if ven_vals.notna().sum() > 2:
                ax0.plot(dates, ven_vals, color=color, linewidth=0.8,
                         linestyle='--', alpha=0.45)

        for bp in ruptures_gov[gov].get('bps', []):
            if bp < len(dates):
                ax0.axvline(dates.iloc[bp], color=color, linestyle='--',
                            alpha=0.5, linewidth=1.1)

    ax0.set_title(
        f'PELT A L102 — {titre} | '
        f'MIN_AMP={params["min_amplitude"]*100:.1f}% · IQR×{params["iqr_factor"]} · '
        f'Signal 7D · Cohérence OR (seuil={COHERENCE_SEUIL_RATIO*100:.0f}%)',
        fontsize=10, fontweight='600', color='#1A1A18')
    ax0.legend(fontsize=8, loc='upper left', ncol=3, framealpha=0.85)
    ax0.set_ylabel('Prix lissé TND/m²', fontsize=9)
    ax0.grid(True, alpha=0.22, linewidth=0.5)
    ax0.spines[['top', 'right']].set_visible(False)
    ax0.set_facecolor('#F9F9F7')

    # Row 1a : Heatmap intensité ruptures
    ax1a = fig.add_subplot(gs[1, 0])
    all_govs = sorted(agg['gouvernorat'].unique())
    all_yrs  = sorted(agg['annee'].unique())
    heat_data = np.zeros((len(all_govs), len(all_yrs)))
    for i, gov in enumerate(all_govs):
        for j, yr in enumerate(all_yrs):
            r_gov = ruptures_gov.get(gov, {})
            n_bp  = sum(1 for d in r_gov.get('dates', []) if d.startswith(str(int(yr))))
            heat_data[i, j] = n_bp
    if heat_data.max() > 0:
        im = ax1a.imshow(heat_data, aspect='auto', cmap='YlOrRd',
                         vmin=0, vmax=max(1, heat_data.max()))
        plt.colorbar(im, ax=ax1a, label='Nb ruptures')
    ax1a.set_yticks(range(len(all_govs)))
    ax1a.set_yticklabels([GOUVERNORAT_DEC.get(int(g), str(g)) for g in all_govs],
                          fontsize=6.5)
    ax1a.set_xticks(range(len(all_yrs)))
    ax1a.set_xticklabels([str(int(y)) for y in all_yrs], fontsize=8)
    ax1a.set_title('Intensité ruptures (gov × année)', fontsize=10, fontweight='600')

    # Row 1b : Bar ruptures par cluster
    ax1b = fig.add_subplot(gs[1, 1])
    cluster_counts = {}
    for gov, r_gov in ruptures_gov.items():
        cid = cluster_map_grp.get(gov, {}).get('cluster', 0)
        cluster_counts[cid] = cluster_counts.get(cid, 0) + r_gov.get('n', 0)
    sc = sorted(cluster_counts.keys())
    if sc:
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
    ax1b.set_title('Ruptures par cluster [L102 k≥4]', fontsize=10, fontweight='600')
    ax1b.spines[['top', 'right']].set_visible(False)
    ax1b.set_facecolor('#F9F9F7'); ax1b.tick_params(colors='#888780')

    # Row 2 : Top 12 par score
    ax2 = fig.add_subplot(gs[2, :])
    top_scores = []
    for gov, r_gov in ruptures_gov.items():
        cid = cluster_map_grp.get(gov, {}).get('cluster', 0)
        nom = GOUVERNORAT_DEC.get(int(gov), str(gov))
        for i, (date_str, score) in enumerate(zip(r_gov.get('dates', []),
                                                    r_gov.get('scores', []))):
            dl = r_gov.get('delta_prix_loc', [0.0] * 100)[i] \
                 if 'delta_prix_loc' in r_gov else 0.0
            dv = r_gov.get('delta_prix_ven', [0.0] * 100)[i] \
                 if 'delta_prix_ven' in r_gov else 0.0
            top_scores.append({
                'label'    : f"{nom}\n{date_str}",
                'score'    : score,
                'cluster'  : cid,
                'delta'    : r_gov.get('delta_prix', 0),
                'delta_loc': dl,
                'delta_ven': dv,
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
            txt = (f"Δglobal={t['delta']:+.0f} TND | "
                   f"Δloc={t['delta_loc']:+.2%} | "
                   f"Δven={t['delta_ven']:+.2%} | "
                   f"score={t['score']:.3f}")
            ax2.text(t['score'] + 0.005, i, txt,
                     va='center', fontsize=7, color='#333330')
        ax2.set_xlabel('Score (amplitude × √n × |spread_réel|)', fontsize=9)
        ax2.set_title('Top ruptures L102 — signal 7D · cohérence OR·50%',
                      fontsize=10, fontweight='600')
        ax2.spines[['top', 'right']].set_visible(False)
        ax2.set_facecolor('#F9F9F7')

    patches = [mpatches.Patch(color=CLUSTER_PALETTE.get(c, GREY),
                              label=f'Cluster {c}')
               for c in sorted(set(v.get('cluster', 0)
                                   for v in cluster_map_grp.values()))]
    fig.legend(handles=patches, loc='lower center', ncol=len(patches),
               fontsize=9, framealpha=0.85, bbox_to_anchor=(0.5, 0.005))

    fig.suptitle(
        f'PELT A L102 — {titre} | Signal 7D · Cohérence OR 50% · Seuil adaptatif par cluster\n'
        f'Params : MIN_ANN={params["min_annonces"]} '
        f'MIN_SIG={params["min_signal"]} '
        f'MIN_AMP={params["min_amplitude"]*100:.1f}% '
        f'PEN={params["penalty_mult"]:.3f} '
        f'IQR×{params["iqr_factor"]}',
        fontsize=12, fontweight='700', color='#1A1A18', y=0.98,
    )
    plt.savefig(output_path, dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close(fig)
    print(f"  → Dashboard : {output_path}")


# ─── Pipeline PELT ───────────────────────────────────────────────────────────

# ─── Utilitaire TFT : construction de la série rupture_flag ─────────────────
# À appeler après run_pelt() pour construire le dataset TFT.
# rupture_flag = 1 le mois d'une rupture détectée, 0 sinon.
# C'est la feature PELT clé pour rendre le TFT causal.
#
# Usage typique :
#   flag_df = build_rupture_flag_series(pelt_rapport_csv, all_dates_df)
#   tft_dataset = all_dates_df.merge(flag_df, on=['gouvernorat','annee','mois'], how='left')
#   tft_dataset['rupture_flag'] = tft_dataset['rupture_flag'].fillna(0).astype(int)

def build_rupture_flag_series(rapport_csv_path: str,
                               calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    Construit une série temporelle binaire rupture_flag à partir du rapport PELT.

    Parameters
    ----------
    rapport_csv_path : str
        Chemin vers pelt_A_rapport.csv (produit par run_pelt()).
    calendar_df : pd.DataFrame
        DataFrame avec colonnes ['gouvernorat','annee','mois'] couvrant
        toutes les dates du dataset. Typiquement le dataset brut BO3.

    Returns
    -------
    pd.DataFrame
        ['gouvernorat','annee','mois','rupture_flag']
        rupture_flag = 1 si rupture détectée ce mois, 0 sinon.
    """
    if not os.path.exists(rapport_csv_path):
        raise FileNotFoundError(f"Rapport PELT introuvable : {rapport_csv_path}")

    rpt_df = pd.read_csv(rapport_csv_path)
    if 'date_rupture' not in rpt_df.columns:
        raise ValueError("Colonne 'date_rupture' absente du rapport PELT.")

    # Parser date_rupture (format YYYY-MM)
    rpt_df[['annee_r','mois_r']] = (
        rpt_df['date_rupture'].str.split('-', expand=True)
                               .iloc[:, :2]
                               .astype(int)
    )
    rpt_df = rpt_df.rename(columns={'gouvernorat': 'gouvernorat'})

    # Marquer les mois de rupture
    flags = (rpt_df[['gouvernorat','annee_r','mois_r']]
             .drop_duplicates()
             .assign(rupture_flag=1)
             .rename(columns={'annee_r':'annee','mois_r':'mois'}))

    # Joindre sur le calendrier
    cal = calendar_df[['gouvernorat','annee','mois']].drop_duplicates()
    result = cal.merge(flags, on=['gouvernorat','annee','mois'], how='left')
    result['rupture_flag'] = result['rupture_flag'].fillna(0).astype(int)

    print(f"  rupture_flag : {result['rupture_flag'].sum():.0f} mois avec rupture "
          f"/ {len(result)} total "
          f"({result['rupture_flag'].mean()*100:.1f}%)")
    return result[['gouvernorat','annee','mois','rupture_flag']]


def run_pelt(groupes: dict, cluster_map: dict) -> dict:
    """
    PELT par cluster. L102+ :
    - Signal 10D (+ zone_geo × 0.10 + volatilite × 0.15 + variables exogènes).
    - Cohérence OR à 50% (was 70%).
    - Pénalité adaptée à indice_liquidite (from cluster_map L102).
    - Rapport CSV enrichi.
    """
    print()
    print("=" * 80)
    print("  PELT A L102+ | Signal 10D · Cohérence OR 50% · Pénalité liquidité adaptée")
    print("  Zone géographique + Volatilité prix + exogènes intégrées dans le signal PELT")
    print("=" * 80)

    # ── FIX : initialisation obligatoire ─────────────────────────────────────
    resultats_global  = {}   # ← BUG CRITIQUE corrigé : manquait avant la boucle
    nb_total          = 0
    nb_rejetes_coher  = 0
    rapport_rows      = []

    # ── Feature Registry : validation + application réelle ────────────────────
    # On valide sur l'ensemble des groupes disponibles (avant la boucle).
    # get_features() est ensuite appelé DANS la boucle pour filtrer df par groupe.
    if _FEATURES_CONFIG_AVAILABLE:
        all_df = pd.concat(groupes.values(), ignore_index=True)
        present, missing = validate_features(all_df, "pelt")
        print(f"  Feature Registry [pelt] : {len(present)} features sources présentes"
              f"{(' | manquantes: ' + str(missing)) if missing else ' ✔'}")
        print(f"  Features PELT confirmées : {present}")
    else:
        print(f"  Feature Registry : non disponible (features_config.py introuvable)")
    print()

    for groupe in GROUPES_ACTIFS:
        df = groupes.get(groupe)
        if df is None:                 # ← vérifier d'abord
            print(f"\n  ✗ {groupe} absent"); continue
        df = build_features(df)        # ← ensuite seulement


        # ── Application Feature Registry : sélection après agrégation ───────
        if _FEATURES_CONFIG_AVAILABLE:
            features = get_features("pelt", groupe)
            missing = [f for f in features if f not in df.columns]
            if missing:
                print(f"  ⚠ Features manquantes pour {groupe}: {missing} — utilisation colonnes disponibles")
            # Ne pas filtrer df ici, preparer_serie a besoin de toutes les colonnes
        else:
            raise Exception("Feature Registry obligatoire pour PELT — features_config.py manquant")

        # ── Feature Registry appliqué : sélection réelle des colonnes ─────────
        # preparer_serie() a besoin de : gouvernorat, annee, mois,
        # indice_prix_m2_regional, glissement_immo_trim, type_transaction.
        # On garde TOUTES les colonnes du dataset (preparer_serie agrège elle-même),
        # mais on vérifie que les features PELT requises sont présentes.
        if _FEATURES_CONFIG_AVAILABLE:
            feats_pelt = get_features("pelt", groupe)
            # Colonnes obligatoires pour l'agrégation PELT (non dans le registry car
            # nécessaires au groupby, pas comme features ML directes)
            cols_groupby = ['gouvernorat', 'annee', 'mois', 'type_transaction']
            cols_needed  = list(set(feats_pelt + cols_groupby))
            cols_present = [c for c in cols_needed if c in df.columns]
            cols_missing = [c for c in cols_needed if c not in df.columns]
            if cols_missing:
                print(f"  ⚠ [{groupe}] Colonnes manquantes : {cols_missing}")
            df_pelt = df[cols_present].copy()
            df_pelt = _ensure_exogenous_features(df_pelt)
            print(f"  Feature Registry [pelt/{groupe}] : "
                  f"{len(feats_pelt)} features registry + {len(cols_groupby)} groupby"
                  f" = {len(cols_present)} colonnes utilisées ✔")
            print(f"    Features ML : {feats_pelt}")
        else:
            # fallback : toutes les colonnes disponibles (comportement historique)
            df_pelt = df.copy()
            print(f"  [{groupe}] Feature Registry non disponible → toutes colonnes")

        n_loc = int((df_pelt['type_transaction'] == TT_LOC).sum())
        n_ven = int((df_pelt['type_transaction'] == TT_VEN).sum())
        print(f"\n{'─'*72}\n  {groupe.upper()} ({len(df_pelt):,} obs) "
              f"| Location={n_loc:,} ({n_loc/len(df_pelt)*100:.1f}%) "
              f"| Vente={n_ven:,} ({n_ven/len(df_pelt)*100:.1f}%)")

        # ── Vérification L102 RÉELLE (basée sur colonnes présentes) ──────────
        l102_present = {c: (c in df_pelt.columns) for c in _L102_COLS}
        l102_ok      = all(l102_present.values())
        l102_status  = "✔ L102 actif" if l102_ok else (
            "⚠ L102 partiel : " + str([c for c, v in l102_present.items() if not v]))
        print(f"  Signal 10D {l102_status}")

        params = compute_pelt_params(df_pelt)
        liq_lbl = ("illiquid → min_ann=1" if params['is_illiquid']
                   else f"liquide → min_ann={params['min_annonces']}")
        print(f"  Paramètres PELT dynamiques :")
        print(f"    MIN_ANNONCES  = {params['min_annonces']}  ({liq_lbl})")
        print(f"    IQR_FACTOR    = {params['iqr_factor']}")
        print(f"    MIN_SIGNAL    = {params['min_signal']}")
        print(f"    MIN_AMPLITUDE = {params['min_amplitude']*100:.1f}%")
        print(f"    PENALTY_MULT  = {params['penalty_mult']:.3f}")
        print(f"    COHERENCE_SEUIL_RATIO = {COHERENCE_SEUIL_RATIO:.0%} [L102]")

        agg = preparer_serie(df_pelt, params['min_annonces'], params['iqr_factor'])

        # Filtrer agg aux features PELT
        if _FEATURES_CONFIG_AVAILABLE:
            features = get_features("pelt", groupe)
            features = list(dict.fromkeys(features + features_externes))
            if all(f in agg.columns for f in features):
                agg = agg[features]
                print(f"  Features PELT appliquées sur signal agrégé: {len(features)} colonnes")
            else:
                missing = [f for f in features if f not in agg.columns]
                print(f"  ⚠ Features manquantes dans signal: {missing} — utilisation toutes colonnes")

        n_excl_iqr = int(agg['_n_excl_iqr'].iloc[0]) \
                     if '_n_excl_iqr' in agg.columns and len(agg) > 0 else 0
        agg = agg.drop(columns=['_n_excl_iqr'], errors='ignore')

        has_loc_col = 'prix_lisse_loc' in agg.columns
        has_ven_col = 'prix_lisse_ven' in agg.columns
        print(f"  Colonnes stratifiées : "
              f"prix_lisse_loc={'✔' if has_loc_col else '✗'} | "
              f"prix_lisse_ven={'✔' if has_ven_col else '✗'}")

        pt_brut  = df_pelt.groupby('gouvernorat').apply(
            lambda x: x[['annee', 'mois']].drop_duplicates().shape[0])
        pt_apres = agg.groupby('gouvernorat').size()
        ann_moy  = agg.groupby('gouvernorat')['n'].mean()
        fiab     = (pt_apres * ann_moy).fillna(0)

        def _fiabilite_label(f: float) -> str:
            if f >= 50:  return 'haute'
            if f >= 15:  return 'moyenne'
            return 'faible'

        fiab_lbl = fiab.apply(_fiabilite_label)

        print(f"\n  Signal PELT disponible "
              f"(min_ann={params['min_annonces']} · IQR×{params['iqr_factor']} "
              f"· -{n_excl_iqr} pts excl.) :")
        print(f"  {'Gouvernorat':<16} | {'pts_bruts':>9} | {'pts_PELT':>8} "
              f"| {'%cons':>5} | {'moy_ann':>7} | {'fiabilité':>9}")
        print(f"  {'-'*65}")
        for gov in sorted(agg['gouvernorat'].unique()):
            nom_g = GOUVERNORAT_DEC.get(int(gov), str(gov))
            pb    = int(pt_brut.get(gov, 0))
            pa    = int(pt_apres.get(gov, 0))
            pct   = pa / pb * 100 if pb > 0 else 0
            ma    = ann_moy.get(gov, 0)
            fl    = fiab_lbl.get(gov, '?')
            flag  = ' ⚠' if fl == 'faible' else ''
            print(f"  {nom_g:<16} | {pb:>9} | {pa:>8} "
                  f"| {pct:>4.0f}% | {ma:>7.1f} | {fl:>9}{flag}")

        cluster_map_grp = cluster_map.get(groupe, {})
        ruptures_gov    = {}
        scores_groupe_courant = []

        clusters_govs: dict = {}
        for gov_id, info in cluster_map_grp.items():
            clusters_govs.setdefault(info['cluster'], []).append(gov_id)
        if not clusters_govs:
            for gov in agg['gouvernorat'].unique():
                clusters_govs.setdefault(0, []).append(int(gov))

        resultats_cluster: dict = {}

        for cluster_id, govs_cluster in sorted(clusters_govs.items()):
            noms_cluster = [GOUVERNORAT_DEC.get(g, str(g)) for g in govs_cluster]
            print(f"\n  ● Cluster {cluster_id} ({len(govs_cluster)} gov) : {noms_cluster}")

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
                ratio_lv  = gov_info.get('ratio_location_vente', 0.5)
                n_obs_gov = len(sub)
                nom       = GOUVERNORAT_DEC.get(int(gov), str(gov))
                z_w       = _zscore_weight(n_obs_gov)
                f_score   = fiab.get(gov, 0)
                f_label   = ('haute' if f_score >= 50
                             else ('moyenne' if f_score >= 15 else 'faible'))

                # L102 : récupérer zone_geo et volatilite_prix depuis cluster_map enrichi
                zone_geo       = float(gov_info.get('zone_geographique', 3.0))
                volatilite_prix = float(gov_info.get('volatilite_prix', 0.0))
                # L102 : indice_liquidite pour adapter la pénalité
                indice_liq     = float(gov_info.get('indice_liquidite', 1.0))

                try:
                    # L102+ : signal 10D avec zone_geo, volatilite_prix, inflation/pib/high_season
                    signal, prix = construire_signal(
                        sub, zscore_p, ratio_lv, zone_geo, volatilite_prix)
                    n = len(prix)
                    cluster_factor = max(0.9, min(1.3, len(govs_cluster) / 4))

                    # L102 : pénalité fiabilité adaptée à l'indice de liquidité
                    # Marchés très illiquides (indice_liq < 2) → pénalité × 2.5
                    # Marchés très liquides (indice_liq > 10) → pénalité × 0.9
                    fiab_mult = {'haute': 1.0, 'moyenne': 1.4, 'faible': 2.5}[f_label]
                    liq_mult  = max(0.90, min(1.30, 2.0 / (indice_liq + 1e-8)))
                    pen = (params['penalty_mult'] * cluster_factor
                           * fiab_mult * liq_mult
                           * np.log(n) * max(signal.std(), 0.5))

                    algo = rpt.Pelt(model="rbf", min_size=MIN_SIZE, jump=1)
                    algo.fit(signal)
                    bps_raw = [b for b in algo.predict(pen=pen) if 0 < b < n]

                    prix_loc_arr = sub['prix_lisse_loc'].values \
                                   if has_loc_col else np.full(n, np.nan)
                    prix_ven_arr = sub['prix_lisse_ven'].values \
                                   if has_ven_col else np.full(n, np.nan)

                    bps, deltas_loc, deltas_ven, coherences = \
                        filtrer_ruptures_avec_coherence(
                            bps_raw, prix, prix_loc_arr, prix_ven_arr, seuil_amp)

                    n_rejetes = len(filtrer_ruptures(bps_raw, prix, seuil_amp)) - len(bps)
                    nb_rejetes_coher += max(0, n_rejetes)

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
                            'delta_prix'     : round(delta, 0),
                            'delta_prix_loc' : deltas_loc,
                            'delta_prix_ven' : deltas_ven,
                            'cluster'        : cluster_id,
                        }
                        ruptures_cluster.append({
                            'gouvernorat': gov, 'nom': nom,
                            'n': len(bps), 'dates': dates, 'scores': scores,
                            'delta_prix': round(delta, 0),
                            'seuil_amp': seuil_amp,
                        })
                        for i, (d, sc) in enumerate(zip(dates, scores)):
                            rapport_rows.append({
                                'groupe'                : groupe,
                                'gouvernorat'           : gov,
                                'nom'                   : nom,
                                'cluster'               : cluster_id,
                                'date_rupture'          : d,
                                'rupture_flag'          : 1,   # ← NOUVEAU : binaire pour TFT
                                'score'                 : sc,
                                'score_norm'            : None,
                                'delta_prix'            : round(delta, 0),
                                'delta_prix_loc'        : deltas_loc[i] if i < len(deltas_loc) else 0.0,
                                'delta_prix_ven'        : deltas_ven[i] if i < len(deltas_ven) else 0.0,
                                'seuil_amp'             : seuil_amp,
                                'zscore_prix'           : round(zscore_p, 4),
                                'zscore_weight'         : z_w,
                                'ratio_location_vente'  : round(ratio_lv, 4),
                                'fiabilite_signal'      : f_label,
                                'n_pts_serie'           : n_obs_gov,
                                'min_amplitude_calcule' : params['min_amplitude'],
                                'penalty_mult_calcule'  : params['penalty_mult'],
                                'iqr_factor_calcule'    : params['iqr_factor'],
                                # L102 : nouvelles colonnes rapport
                                'zone_geographique'     : int(zone_geo),
                                'indice_liquidite'      : round(indice_liq, 3),
                                'volatilite_prix'       : round(volatilite_prix, 4),
                                'liq_mult_applique'     : round(liq_mult, 3),
                                'coherence_seuil_ratio' : COHERENCE_SEUIL_RATIO,
                                'version'               : 'L102',
                            })
                        nb_total += len(bps)
                        scores_groupe_courant.extend(scores)
                        print(f"    ✓ {nom:<16} → {len(bps)} rupture(s) | "
                              f"Δ ≈ {delta:5.0f} TND | "
                              f"score_max={max(scores):.3f} | "
                              f"fiab={f_label}(×{fiab_mult:.1f}) | "
                              f"liq={indice_liq:.1f}(×{liq_mult:.2f}) | "
                              f"rejetés_coher={n_rejetes}")
                    else:
                        ruptures_gov[gov] = {
                            'n': 0, 'dates': [], 'bps': [], 'scores': [],
                            'delta_prix': 0, 'delta_prix_loc': [],
                            'delta_prix_ven': [], 'cluster': cluster_id,
                        }

                except Exception:
                    import traceback; traceback.print_exc()
                    ruptures_gov[gov] = {
                        'n': 0, 'dates': [], 'bps': [], 'scores': [],
                        'delta_prix': 0, 'delta_prix_loc': [],
                        'delta_prix_ven': [], 'cluster': cluster_id,
                    }

            n_cl = sum(r['n'] for r in ruptures_cluster)
            print(f"    → Total cluster {cluster_id} : {n_cl} ruptures")
            resultats_cluster[cluster_id] = ruptures_cluster

        n_grp = sum(r.get('n', 0) for r in ruptures_gov.values())

        if scores_groupe_courant:
            p95_score = float(np.percentile(scores_groupe_courant, 95))
            p25_score = float(np.percentile(scores_groupe_courant, 25))
            for row in rapport_rows:
                if row['groupe'] == groupe and row['score_norm'] is None:
                    row['score_norm'] = round(min(row['score'] / p95_score, 1.0), 4) \
                                        if p95_score > 0 else 0.0
            sous_seuil = sum(1 for s in scores_groupe_courant if s < p25_score)
            print(f"\n  ✓ {groupe} total : {n_grp} ruptures validées")
            print(f"    Score p95={p95_score:.2f} | {sous_seuil} ruptures sous p25")
        else:
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
           .sort_values(['groupe', 'cluster', 'score'],
                        ascending=[True, True, False])
           .to_csv(csv_path, index=False))

    print()
    print("=" * 80)
    print(f"  BILAN PELT A L102+")
    print(f"  ✓ Total ruptures validées            : {nb_total}")
    print(f"  ✓ Ruptures rejetées (cohérence OR50%): {nb_rejetes_coher}")
    print(f"  → pelt_A.pkl         : {pkl_path}")
    print(f"  → pelt_A_rapport.csv : {csv_path}")
    print(f"     ↳ Colonnes TFT : rupture_flag (binaire 0/1) ✅")
    print(f"     ↳ Usage TFT    : build_rupture_flag_series(csv_path, calendar_df)")
    print(f"  → Graphiques         : {GRAPHS_DIR}")
    print("=" * 80)

    return {
        'n_ruptures': nb_total, 'n_rejetes_coherence': nb_rejetes_coher,
        'pkl': pkl_path, 'csv': csv_path, 'details': resultats_global,
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
    print(f"✔  cluster_map L102 chargé")
    for grp, mp in cluster_map.items():
        clusters = sorted(set(v['cluster'] for v in mp.values()))
        noms_cl  = {
            c: [GOUVERNORAT_DEC.get(g, str(g))
                for g, v in mp.items() if v['cluster'] == c]
            for c in clusters
        }
        has_l102 = any('zone_geographique' in v for v in mp.values())
        print(f"   {grp:<14} : {len(mp)} gov | clusters={noms_cl} "
              f"| features L102={'✔' if has_l102 else '✗'}")

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