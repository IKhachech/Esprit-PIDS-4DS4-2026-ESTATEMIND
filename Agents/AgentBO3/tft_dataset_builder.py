"""
tft_dataset_builder.py — Préparation Dataset TFT · Version Production L99+
===========================================================================

ARCHITECTURE PIPELINE (dataset unique — pas de datasets dérivés)
─────────────────────────────────────────────────────────────────
  pipeline_BO3.py  →  Excel BO3 (22 colonnes, TARGET_COLS)
                              │
                       features_config.py   ← Feature Registry centralisé
                              │
          ┌───────────────────┼──────────────┬──────────────┐
      kmeans_A.py         pelt_A.py      lstm_A.py     tft_dataset_builder.py
      X = df[FEATURE_SETS["kmeans"]]   ...            ← CE FICHIER (exception TFT)

POURQUOI TFT EST UNE EXCEPTION :
  Le TFT exige une structure spécifique (time_idx, group_ids, 4 groupes de features,
  features post-pipeline rupture_flag + cluster_size) → module dédié justifié.
  Les AUTRES modèles lisent directement les Excel BO3.

OBJECTIF : Construire le dataset multi-séries structuré pour PyTorch Forecasting,
en exploitant TOUS les artefacts du pipeline :
  - Excel BO3 (22 colonnes TARGET_COLS)  → features principales
  - cluster_map.pkl  (kmeans_A)          → cluster, cluster_size (statiques)
  - pelt_A_rapport.csv (pelt_A)          → rupture_flag (feature causale 🔥)

STRUCTURE DU DATASET TFT (depuis features_config.FEATURE_SETS["tft"])
──────────────────────────────────────────────────────────────────────
  Target        : prix_m2 (= indice_prix_m2_regional lissé)
  Static cat    : cluster, zone_geo, gouvernorat, groupe
  Static real   : cluster_size, score_attractivite, potentiel_emergent
  Known real    : mois, annee, saison, mois_sin, mois_cos, inflation, pib
  Unknown real  : prix_m2_loc, prix_m2_ven, ratio_loc_ven, volatilite_rolling,
                  nb_annonces_proxy, rupture_flag 🔥, score_rupture, post_rupture

USAGE
─────
  from tft_dataset_builder import build_tft_dataset, get_tft_column_groups
  tft_df    = build_tft_dataset(groupes, cluster_map, pelt_df)
  col_groups = get_tft_column_groups(tft_df)
  tft_df.to_csv('models/tft_dataset.csv', index=False)
"""

import os
import numpy as np
import pandas as pd
import pickle
import warnings
warnings.filterwarnings('ignore')

# ─── Feature Registry ─────────────────────────────────────────────────────────
# tft_dataset_builder.py est le SEUL module qui crée un dataset "dérivé" —
# car le TFT exige une structure spécifique (time_idx, series_id, groupes de features).
# C'est l'exception documentée à la règle "pas de datasets dérivés".
# Tous les autres modèles (kmeans, pelt, lstm) lisent directement les Excel BO3.
try:
    from features_config import FEATURE_SETS, get_features
    _TFT_FEATURE_GROUPS = FEATURE_SETS["tft"]  # dict structuré TFT
    _FEATURES_CONFIG_AVAILABLE = True
except ImportError:
    _FEATURES_CONFIG_AVAILABLE = False
    _TFT_FEATURE_GROUPS = None


# ─── Codes et mappings ─────────────────────────────────────────────────────────
TT_LOC = 1
TT_VEN = 2

# Saison déduite du mois (marché immobilier tunisien)
# Été   (juin-août)       → transactions élevées
# Hiver (déc-fév)        → marché ralenti
# Inter (reste)           → transition
def _mois_to_saison(m: int) -> int:
    if m in (6, 7, 8):   return 1   # Été
    if m in (12, 1, 2):  return 2   # Hiver
    return 3                         # Inter-saison


# ─── 1. Chargement artefacts ──────────────────────────────────────────────────

def charger_artefacts(models_dir: str) -> tuple:
    """
    Charge cluster_map.pkl et pelt_A_rapport.csv.
    Retourne (cluster_map, pelt_df).
    """
    cm_path   = os.path.join(models_dir, 'cluster_map.pkl')
    pelt_path = os.path.join(models_dir, 'pelt_A_rapport.csv')

    cluster_map = {}
    pelt_df     = pd.DataFrame()

    if os.path.exists(cm_path):
        with open(cm_path, 'rb') as f:
            cluster_map = pickle.load(f)
        n_govs = sum(len(v) for v in cluster_map.values())
        print(f"  ✔ cluster_map.pkl : {n_govs} gouvernorats")
        # Vérification cluster_size
        has_size = any('cluster_size' in list(v.values())[0]
                       for v in cluster_map.values() if v)
        print(f"    cluster_size = {'✔ présent' if has_size else '✗ absent (kmeans_A ancien)'}")
    else:
        print(f"  ✗ cluster_map.pkl introuvable : {cm_path}")

    if os.path.exists(pelt_path):
        pelt_df = pd.read_csv(pelt_path)
        n_rupt  = len(pelt_df)
        has_flag = 'rupture_flag' in pelt_df.columns
        print(f"  ✔ pelt_A_rapport.csv : {n_rupt} ruptures | "
              f"rupture_flag={'✔' if has_flag else '✗ (ancienne version)'}")
        if not has_flag:
            pelt_df['rupture_flag'] = 1   # rétrocompatibilité
    else:
        print(f"  ✗ pelt_A_rapport.csv introuvable : {pelt_path}")

    return cluster_map, pelt_df


# ─── 2. Features statiques par gouvernorat ────────────────────────────────────

def _build_static_features(cluster_map: dict, groupe: str) -> pd.DataFrame:
    """
    Extrait les features statiques depuis cluster_map pour un groupe.
    Retourne un DataFrame indexé par gouvernorat.
    """
    cm_grp = cluster_map.get(groupe, {})
    rows   = []
    for gov_id, info in cm_grp.items():
        rows.append({
            'gouvernorat'     : int(gov_id),
            'cluster'         : int(info.get('cluster', 0)),
            'cluster_size'    : int(info.get('cluster_size', 1)),   # ← insight métier
            'zone_geo'        : int(info.get('zone_geographique', 3)),
            'prix_mean_ref'   : float(info.get('prix_mean', 0.0)),
            'zscore_ref'      : float(info.get('zscore_prix', 0.0)),
            'ratio_lv_ref'    : float(info.get('ratio_location_vente', 0.5)),
            'nom'             : info.get('nom', str(gov_id)),
        })
    return pd.DataFrame(rows)


# ─── 3. Features PELT (rupture_flag, score_norm, post_rupture) ───────────────

def _build_pelt_features(pelt_df: pd.DataFrame, groupe: str,
                          calendar: pd.DataFrame) -> pd.DataFrame:
    """
    Construit les features PELT sur le calendrier complet :
      rupture_flag     = 1 le mois de la rupture, 0 sinon
      score_rupture    = score_norm de la rupture (0 si pas de rupture)
      post_rupture     = 1 après la première rupture détectée, 0 avant
    """
    if pelt_df.empty:
        cal = calendar[['gouvernorat','annee','mois']].drop_duplicates().copy()
        cal['rupture_flag']   = 0
        cal['score_rupture']  = 0.0
        cal['post_rupture']   = 0
        return cal

    pelt_grp = pelt_df[pelt_df['groupe'] == groupe].copy()

    # Parser date_rupture → annee, mois
    if 'date_rupture' in pelt_grp.columns:
        parts = pelt_grp['date_rupture'].str.split('-', expand=True)
        pelt_grp['annee_r'] = parts[0].astype(int)
        pelt_grp['mois_r']  = parts[1].astype(int)

    # Agréger : plusieurs ruptures le même mois → max score
    flags = (pelt_grp.groupby(['gouvernorat','annee_r','mois_r'])
             .agg(rupture_flag=('rupture_flag', 'max'),
                  score_rupture=('score_norm', 'max'))
             .reset_index()
             .rename(columns={'annee_r':'annee','mois_r':'mois'}))

    cal = calendar[['gouvernorat','annee','mois']].drop_duplicates().copy()
    cal = cal.merge(flags, on=['gouvernorat','annee','mois'], how='left')
    cal['rupture_flag']  = cal['rupture_flag'].fillna(0).astype(int)
    cal['score_rupture'] = cal['score_rupture'].fillna(0.0)

    # post_rupture : flag cumulatif → 1 dès qu'une rupture a eu lieu
    cal = cal.sort_values(['gouvernorat','annee','mois']).reset_index(drop=True)
    cal['post_rupture'] = (cal.groupby('gouvernorat')['rupture_flag']
                             .cumsum()
                             .clip(upper=1)
                             .astype(int))

    n_flags = int(cal['rupture_flag'].sum())
    print(f"    rupture_flag : {n_flags} mois avec rupture / {len(cal)} total "
          f"({n_flags/len(cal)*100:.1f}%)")
    return cal


# ─── 4a. Robustesse features critiques ───────────────────────────────────────

# Features critiques : nom_colonne → (nom_renommé_dans_agg, colonne_source_proxy)
# Chaque entrée définit le comportement SI la colonne est absente du DataFrame brut.
_CRITICAL_FEATURES = {
    # colonne_brute          : (nom_agg_tft,       source_proxy,             périodes_proxy)
    'inflation_glissement_annuel': ('inflation_annuel', 'indice_prix_m2_regional', 12),
    'croissance_pib_trim'        : ('croissance_pib',   'indice_prix_m2_regional',  3),
    'high_season'                : ('high_season',       None,                      None),
}


def _handle_critical_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garantit la présence des features critiques avant agrégation.

    Pour chaque feature critique :
      ── SI la colonne existe dans df :
           → remplir les NaN par la moyenne de la colonne
           → logger le nombre de NaN corrigés
      ── SINON :
           → créer un proxy calculé depuis indice_prix_m2_regional
           → logger la création du proxy
      ── CAS SPÉCIAL high_season (pas de proxy numérique) :
           → créer colonne à 0 si absente (hiver neutre)

    Ne modifie jamais la logique de détection en aval.
    Retourne une copie défensive du DataFrame.
    """
    df = df.copy()
    print("  [features_critiques] Vérification robustesse features critiques...")

    # ── inflation_glissement_annuel ───────────────────────────────────────────
    if 'inflation_glissement_annuel' in df.columns:
        n_nan = int(df['inflation_glissement_annuel'].isna().sum())
        if n_nan > 0:
            mean_val = df['inflation_glissement_annuel'].mean()
            df['inflation_glissement_annuel'] = (
                df['inflation_glissement_annuel'].fillna(mean_val)
            )
            print(f"    ✔ inflation_glissement_annuel : {n_nan} NaN → fillna(mean={mean_val:.4f})")
        else:
            print(f"    ✔ inflation_glissement_annuel : présente, aucun NaN")
    else:
        # Fallback : variation prix sur 12 mois glissants comme proxy inflation
        if 'indice_prix_m2_regional' in df.columns:
            df = df.sort_values(['gouvernorat', 'annee', 'mois']).reset_index(drop=True)
            df['inflation_glissement_annuel'] = (
                df.groupby('gouvernorat')['indice_prix_m2_regional']
                  .transform(lambda x: x.pct_change(12).fillna(0))
            )
            print("    ⚠ inflation_glissement_annuel : ABSENTE → proxy créé "
                  "(pct_change(12) sur indice_prix_m2_regional)")
        else:
            df['inflation_glissement_annuel'] = 0.0
            print("    ⚠ inflation_glissement_annuel : ABSENTE + source proxy absente → 0.0")

    # ── croissance_pib_trim ───────────────────────────────────────────────────
    if 'croissance_pib_trim' in df.columns:
        n_nan = int(df['croissance_pib_trim'].isna().sum())
        if n_nan > 0:
            mean_val = df['croissance_pib_trim'].mean()
            df['croissance_pib_trim'] = df['croissance_pib_trim'].fillna(mean_val)
            print(f"    ✔ croissance_pib_trim         : {n_nan} NaN → fillna(mean={mean_val:.4f})")
        else:
            print(f"    ✔ croissance_pib_trim         : présente, aucun NaN")
    else:
        # Fallback : variation trimestrielle prix comme proxy PIB local
        if 'indice_prix_m2_regional' in df.columns:
            df = df.sort_values(['gouvernorat', 'annee', 'mois']).reset_index(drop=True)
            df['croissance_pib_trim'] = (
                df.groupby('gouvernorat')['indice_prix_m2_regional']
                  .transform(lambda x: x.pct_change(3).fillna(0))
            )
            print("    ⚠ croissance_pib_trim         : ABSENTE → proxy créé "
                  "(pct_change(3) sur indice_prix_m2_regional)")
        else:
            df['croissance_pib_trim'] = 0.0
            print("    ⚠ croissance_pib_trim         : ABSENTE + source proxy absente → 0.0")

    # ── high_season ───────────────────────────────────────────────────────────
    if 'high_season' in df.columns:
        n_nan = int(df['high_season'].isna().sum())
        if n_nan > 0:
            mean_val = df['high_season'].mean()
            df['high_season'] = df['high_season'].fillna(mean_val)
            print(f"    ✔ high_season                 : {n_nan} NaN → fillna(mean={mean_val:.4f})")
        else:
            print(f"    ✔ high_season                 : présente, aucun NaN")
    else:
        # Pas de proxy numérique pertinent : on crée via logique calendaire
        if 'mois' in df.columns:
            df['high_season'] = df['mois'].apply(lambda m: 1 if m in (6, 7, 8) else 0)
            print("    ⚠ high_season                 : ABSENTE → proxy calendaire "
                  "(mois 6/7/8 = haute saison)")
        else:
            df['high_season'] = 0
            print("    ⚠ high_season                 : ABSENTE + mois absent → 0")

    return df


# ─── 4. Agrégation mensuelle + features time-varying ─────────────────────────

def _agreger_mensuel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège par (gouvernorat, annee, mois) et calcule toutes les
    features time-varying observed.
    """
    # Signal global (TARGET)
    agg = (df.groupby(['gouvernorat','annee','mois'])
             .agg(
                 prix_m2             = ('indice_prix_m2_regional', 'mean'),
                 score_attractivite  = ('score_attractivite', 'mean'),
                 nb_infra            = ('nb_infra', 'mean'),
                 nb_commerce         = ('nb_commerce', 'mean'),
                 glissement_immo     = ('glissement_immo_trim', 'mean'),
                 inflation_annuel    = ('inflation_glissement_annuel', 'mean'),
                 croissance_pib      = ('croissance_pib_trim', 'mean'),
                 high_season         = ('high_season', 'mean'),
                 n_obs               = ('indice_prix_m2_regional', 'count'),
             )
             .reset_index())

    # Sous-signaux stratifiés par type_transaction
    for tt, col in [(TT_LOC, 'prix_m2_loc'), (TT_VEN, 'prix_m2_ven')]:
        sub = (df[df['type_transaction'] == tt]
               .groupby(['gouvernorat','annee','mois'])['indice_prix_m2_regional']
               .mean()
               .reset_index()
               .rename(columns={'indice_prix_m2_regional': col}))
        agg = agg.merge(sub, on=['gouvernorat','annee','mois'], how='left')

    # ratio_location_vente mensuel
    n_total = df.groupby(['gouvernorat','annee','mois'])['type_transaction'].count()
    n_ven   = (df[df['type_transaction'] == TT_VEN]
               .groupby(['gouvernorat','annee','mois'])['type_transaction'].count())
    ratio   = (n_ven / n_total).reset_index()
    ratio.columns = ['gouvernorat','annee','mois','ratio_loc_ven']
    agg = agg.merge(ratio, on=['gouvernorat','annee','mois'], how='left')
    agg['ratio_loc_ven'] = agg['ratio_loc_ven'].fillna(0.5)

    # nb_annonces_proxy : n_obs cumulé normalisé log → proxy liquidité
    agg = agg.sort_values(['gouvernorat','annee','mois']).reset_index(drop=True)
    agg['nb_annonces_proxy'] = (
        agg.groupby('gouvernorat')['n_obs']
           .transform(lambda x: np.log1p(x.cumsum())))

    # Volatilité glissante (3 mois)
    agg['volatilite_rolling'] = (
        agg.groupby('gouvernorat')['prix_m2']
           .transform(lambda x: x.rolling(3, min_periods=1).std().fillna(0.0)))

    # Interpolation NaN sur sous-signaux
    for col in ['prix_m2_loc', 'prix_m2_ven']:
        agg[col] = (agg.groupby('gouvernorat')[col]
                       .transform(lambda x: x.interpolate(method='linear',
                                                           limit_direction='both')))
        agg[col] = agg[col].fillna(agg['prix_m2'])   # fallback prix global

    return agg


# ─── 5. Construction dataset TFT complet ─────────────────────────────────────

def build_tft_dataset(groupes: dict,
                       cluster_map: dict,
                       pelt_df: pd.DataFrame,
                       groupes_actifs: list = None) -> pd.DataFrame:
    """
    Construit le dataset TFT multi-séries + causal.

    Parameters
    ----------
    groupes       : dict {groupe → DataFrame brut BO3}
    cluster_map   : dict chargé depuis cluster_map.pkl
    pelt_df       : DataFrame chargé depuis pelt_A_rapport.csv
    groupes_actifs: liste des groupes à inclure (défaut = tous)

    Returns
    -------
    pd.DataFrame  — dataset TFT structuré avec toutes les features.
    """
    if groupes_actifs is None:
        groupes_actifs = list(groupes.keys())

    all_dfs = []

    for groupe in groupes_actifs:
        df = groupes.get(groupe)
        if df is None:
            print(f"  ✗ {groupe} absent"); continue

        print(f"\n  ── {groupe.upper()} ──")

        # ── Robustesse features critiques (AVANT agrégation) ─────────────────
        # Garantit que inflation, PIB et high_season existent toujours dans df.
        # Si absentes → proxy calculé depuis indice_prix_m2_regional.
        # Si présentes avec NaN → fillna(mean). Ne modifie jamais la logique aval.
        df = _handle_critical_features(df)

        # ── Agrégation mensuelle ──────────────────────────────────────────────
        agg = _agreger_mensuel(df)

        # ── Features temporelles connues à l'avance ───────────────────────────
        agg['saison']           = agg['mois'].apply(_mois_to_saison)
        agg['annee_norm']       = (agg['annee'] - agg['annee'].min()) / \
                                   max(agg['annee'].max() - agg['annee'].min(), 1)
        agg['mois_sin']         = np.sin(2 * np.pi * agg['mois'] / 12)
        agg['mois_cos']         = np.cos(2 * np.pi * agg['mois'] / 12)

        # ── Features statiques (cluster_map) ─────────────────────────────────
        static_df = _build_static_features(cluster_map, groupe)
        agg = agg.merge(static_df, on='gouvernorat', how='left')

        # Fallback si gouvernorat non présent dans cluster_map
        agg['cluster']      = agg['cluster'].fillna(0).astype(int)
        agg['cluster_size'] = agg['cluster_size'].fillna(1).astype(int)
        agg['zone_geo']     = agg['zone_geo'].fillna(3).astype(int)

        # ── Features PELT causales ────────────────────────────────────────────
        pelt_feats = _build_pelt_features(pelt_df, groupe, agg)
        agg = agg.merge(pelt_feats, on=['gouvernorat','annee','mois'], how='left')
        agg['rupture_flag']  = agg['rupture_flag'].fillna(0).astype(int)
        agg['score_rupture'] = agg['score_rupture'].fillna(0.0)
        agg['post_rupture']  = agg['post_rupture'].fillna(0).astype(int)

        # ── Label groupe ──────────────────────────────────────────────────────
        agg['groupe'] = groupe

        # ── Rapport features ──────────────────────────────────────────────────
        n_gov    = agg['gouvernorat'].nunique()
        n_rows   = len(agg)
        n_rupt   = int(agg['rupture_flag'].sum())
        has_sz   = 'cluster_size' in agg.columns
        print(f"    {n_gov} gouvernorats | {n_rows} lignes mensuelles")
        print(f"    rupture_flag : {n_rupt} mois avec rupture")
        print(f"    cluster_size : {'✔' if has_sz else '✗'}")
        if has_sz:
            atypiques = agg[agg['cluster_size'] <= 2]['nom'].unique()
            if len(atypiques):
                print(f"    Clusters atypiques (size≤2) : {list(atypiques)}")

        all_dfs.append(agg)

    if not all_dfs:
        raise ValueError("Aucun groupe disponible pour construire le dataset TFT.")

    tft_df = pd.concat(all_dfs, ignore_index=True)
    tft_df = tft_df.sort_values(['groupe','gouvernorat','annee','mois']).reset_index(drop=True)

    return tft_df


# ─── 6. Colonnes structurées pour TFT ────────────────────────────────────────

def get_tft_column_groups(tft_df: pd.DataFrame) -> dict:
    """
    Retourne le dictionnaire des groupes de colonnes pour configurer le TFT.

    Lit depuis features_config.FEATURE_SETS["tft"] si disponible,
    sinon utilise la définition locale de fallback.

    Usage avec PyTorch Forecasting :
      TimeSeriesDataSet(
          data=tft_df,
          target="prix_m2",
          time_idx="time_idx",
          group_ids=["gouvernorat","groupe"],
          static_categoricals=col_groups["static_cat"],
          static_reals=col_groups["static_real"],
          time_varying_known_reals=col_groups["known_real"],
          time_varying_unknown_reals=col_groups["unknown_real"],
      )
    """
    cols = set(tft_df.columns)

    # ── Lire depuis le Feature Registry si disponible ─────────────────────────
    if _FEATURES_CONFIG_AVAILABLE and _TFT_FEATURE_GROUPS is not None:
        reg = _TFT_FEATURE_GROUPS
        # Les features du Registry sont définies sur les noms TARGET_COLS (sources).
        # Le dataset TFT utilise des noms renommés (prix_m2, prix_m2_loc, etc.).
        # On construit les groupes en ajoutant les features TFT-spécifiques.
        static_cat  = [c for c in reg.get('static_cat',  []) if c in cols]
        static_real = [c for c in reg.get('static_real', []) if c in cols]
        # Ajouter les features statiques post-pipeline (depuis cluster_map)
        for extra in ['cluster', 'cluster_size', 'zscore_ref', 'ratio_lv_ref', 'prix_mean_ref']:
            if extra in cols and extra not in static_cat and extra not in static_real:
                static_real.append(extra)

        known_real = [c for c in reg.get('known_real', []) if c in cols]
        # Ajouter les features temporelles enrichies par tft_dataset_builder
        for extra in ['saison', 'annee_norm', 'mois_sin', 'mois_cos']:
            if extra in cols and extra not in known_real:
                known_real.append(extra)

        unknown_real = [
            'prix_m2',            # TARGET (renommé depuis indice_prix_m2_regional)
            'prix_m2_loc',        # (renommé depuis indice_loc_lisse)
            'prix_m2_ven',        # (renommé depuis indice_ven_lisse)
            'ratio_loc_ven',      # (calculé par tft_dataset_builder)
            'volatilite_rolling', # (calculé par tft_dataset_builder)
            'nb_annonces_proxy',  # (calculé par tft_dataset_builder)
            'glissement_immo',    # (renommé depuis glissement_immo_trim)
            'inflation_annuel',   # (renommé depuis inflation_glissement_annuel)
            'croissance_pib',     # (renommé depuis croissance_pib_trim)
            'rupture_flag',       # ← feature PELT causale 🔥 (POST_PIPELINE)
            'score_rupture',      # ← intensité rupture (POST_PIPELINE)
            'post_rupture',       # ← régime post-rupture (POST_PIPELINE)
        ]
        # Ajouter features du Registry présentes sous leur nom original
        for feat in reg.get('unknown_real', []):
            if feat in cols and feat not in unknown_real:
                unknown_real.append(feat)

        unknown_real = [c for c in unknown_real if c in cols]
        source = "features_config.FEATURE_SETS['tft']"

    else:
        # ── Fallback local (si features_config.py non disponible) ─────────────
        static_cat  = [c for c in ['cluster', 'zone_geo', 'groupe'] if c in cols]
        static_real = [c for c in ['cluster_size', 'zscore_ref', 'ratio_lv_ref',
                                    'prix_mean_ref', 'score_attractivite',
                                    'potentiel_emergent'] if c in cols]
        known_real  = [c for c in ['mois', 'annee', 'saison', 'annee_norm',
                                    'mois_sin', 'mois_cos',
                                    'inflation_annuel', 'croissance_pib',
                                    'high_season'] if c in cols]
        unknown_real = [c for c in [
            'prix_m2', 'prix_m2_loc', 'prix_m2_ven', 'ratio_loc_ven',
            'volatilite_rolling', 'nb_annonces_proxy', 'glissement_immo',
            'rupture_flag', 'score_rupture', 'post_rupture',
        ] if c in cols]
        source = "fallback local (features_config.py introuvable)"

    print(f"  get_tft_column_groups : source = {source}")
    return {
        'target'      : 'prix_m2',
        'group_ids'   : ['gouvernorat', 'groupe'],
        'static_cat'  : static_cat,
        'static_real' : static_real,
        'known_real'  : known_real,
        'unknown_real': unknown_real,
    }


# ─── 7. Résumé dataset ────────────────────────────────────────────────────────

def print_dataset_summary(tft_df: pd.DataFrame, col_groups: dict) -> None:
    """Affiche un résumé complet du dataset TFT."""
    print("\n" + "=" * 72)
    print("  DATASET TFT — RÉSUMÉ")
    print("=" * 72)
    print(f"  Lignes totales     : {len(tft_df):,}")
    print(f"  Gouvernorats       : {tft_df['gouvernorat'].nunique()}")
    print(f"  Groupes            : {tft_df['groupe'].nunique()} "
          f"({list(tft_df['groupe'].unique())})")
    print(f"  Période            : {tft_df['annee'].min():.0f}-{tft_df['mois'].min():02.0f}"
          f" → {tft_df['annee'].max():.0f}-{tft_df['mois'].max():02.0f}")
    print(f"\n  TARGET             : {col_groups['target']}")
    print(f"  Static cat         : {col_groups['static_cat']}")
    print(f"  Static real        : {col_groups['static_real']}")
    print(f"  Known real         : {col_groups['known_real']}")
    print(f"  Unknown real       : {col_groups['unknown_real']}")
    print(f"\n  rupture_flag       : {int(tft_df['rupture_flag'].sum())} mois "
          f"({tft_df['rupture_flag'].mean()*100:.1f}%)")
    if 'cluster_size' in tft_df.columns:
        atypiques = tft_df[tft_df['cluster_size'] <= 2][['nom','cluster','cluster_size']].drop_duplicates()
        print(f"  Clusters atypiques : {len(atypiques)} gouvernorats (cluster_size ≤ 2)")
        if len(atypiques):
            print(f"    → {list(atypiques['nom'].values)}")
    print("=" * 72)


# ─── Point d'entrée ──────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys

    BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
    MODELS_DIR = os.path.join(BASE_DIR, 'models')
    DOSSIER_BO3 = os.path.normpath(
        os.path.join(BASE_DIR, '..', '..', 'BO3'))

    print("=" * 72)
    print("  TFT DATASET BUILDER — L99+")
    print("=" * 72)

    # Charger les artefacts
    print("\n  Chargement artefacts...")
    cluster_map, pelt_df = charger_artefacts(MODELS_DIR)

    # Charger les données brutes BO3
    GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']
    groupes = {}
    print("\n  Chargement données BO3...")
    for nom in GROUPES_ACTIFS:
        f = os.path.join(DOSSIER_BO3, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f"  ✔ {nom:<14} : {len(groupes[nom]):,} lignes")
        else:
            print(f"  ✗ {nom} manquant : {f}")

    if not groupes:
        print("  ✗ Aucune donnée disponible. Vérifiez le dossier BO3.")
        sys.exit(1)

    # Construire le dataset TFT
    print("\n  Construction dataset TFT...")
    tft_df = build_tft_dataset(groupes, cluster_map, pelt_df)
    col_groups = get_tft_column_groups(tft_df)
    print_dataset_summary(tft_df, col_groups)

    # Sauvegarder
    out_csv = os.path.join(MODELS_DIR, 'tft_dataset.csv')
    out_pkl = os.path.join(MODELS_DIR, 'tft_dataset.pkl')

    tft_df.to_csv(out_csv, index=False)
    with open(out_pkl, 'wb') as f:
        pickle.dump({'df': tft_df, 'col_groups': col_groups}, f)

    print(f"\n  → tft_dataset.csv : {out_csv}")
    print(f"  → tft_dataset.pkl : {out_pkl}")
    print(f"     (contient df + col_groups pour configurer TimeSeriesDataSet)")
    print("=" * 72)
