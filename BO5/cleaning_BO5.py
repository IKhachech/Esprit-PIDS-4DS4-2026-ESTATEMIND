"""
cleaning_BO5.py — ETL et nettoyage pour l'Objectif 5 : Rentabilité Régionale.

Même logique BO2/BO3 :
  [1] Chargement 4 datasets *_BO5.xlsx
  [2] Suppression 4 colonnes nulles (= 0 partout)
  [3] Construction ville_encoded (encodage fréquentiel par gouvernorat × prix_m2 rank)
  [4] Nettoyage seuils prix_m2 + variation_prix_m2
  [5] Isolation Forest outliers
  [6] Validation TARGET + sélection 15 colonnes finales
"""

import os, warnings
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

warnings.filterwarnings('ignore')

from mappings_BO5 import (
    COLS_NULLES, FINAL_COLS_BO5, GOUVERNORAT_DEC,
    SEUILS_PRIX_M2, SEUILS_VARIATION,
)


def _log(msg):
    print(f"  {msg}")


# ================================================================
# ÉTAPE 1 — CHARGEMENT
# ================================================================

def load_datasets(paths):
    print("\n" + "=" * 65)
    print("   ETAPE 1 — CHARGEMENT DATASETS BO5")
    print("=" * 65)
    groupes = {}
    total = 0
    for nom, path in paths.items():
        if not os.path.exists(path):
            _log(f"[WARN] Absent : {path}")
            continue
        df = pd.read_excel(path)
        groupes[nom] = df
        total += len(df)
        _log(f"✔ {nom:<15}: {len(df):>6} lignes × {len(df.columns)} colonnes")
    _log(f"Total chargé     : {total:,} lignes")
    return groupes


# ================================================================
# ÉTAPE 2 — SUPPRESSION COLONNES NULLES
# ================================================================

def drop_null_columns(groupes):
    print("\n" + "=" * 65)
    print("   ETAPE 2 — SUPPRESSION COLONNES NULLES")
    print("=" * 65)
    for nom, df in groupes.items():
        cols = [c for c in COLS_NULLES if c in df.columns]
        groupes[nom] = df.drop(columns=cols)
        _log(f"{nom:<15}: -{len(cols)} colonnes nulles → {groupes[nom].shape[1]} colonnes")
    _log(f"Supprimées : {', '.join(COLS_NULLES)}")
    return groupes


# ================================================================
# ÉTAPE 3 — CONSTRUCTION ville_encoded (NOUVEAU BO5)
# ================================================================
#
# Logique identique BO2/BO3 mais adaptée à BO5 :
#   BO2/BO3 : ville_encoded = encodage fréquentiel de la ville textuelle
#   BO5     : pas de colonne ville → on crée un proxy sous-zone
#             ville_encoded = gouvernorat × 1000 + rank_décile_prix_m2_dans_gouvernorat
#             → distingue les tranches de prix dans un même gouvernorat
#             → range 1000-24009 (cohérent avec BO2/BO3 range 0-24001)
#

def encode_ville(groupes):
    print("\n" + "=" * 65)
    print("   ETAPE 3 — CONSTRUCTION ville_encoded")
    print("=" * 65)
    _log("Logique : gouvernorat × 1000 + décile_prix_m2 (proxy sous-zone)")
    _log("Cohérent avec BO2/BO3 — range 1000-24009")

    for nom, df in groupes.items():
        # Décile prix_m2 dans chaque gouvernorat (0-9)
        df['prix_decile'] = df.groupby('gouvernorat')['prix_m2'].transform(
            lambda x: pd.qcut(x.rank(method='first'), q=10,
                              labels=False, duplicates='drop')
        ).fillna(0).astype(int)

        # ville_encoded = gouvernorat × 1000 + décile
        df['ville_encoded'] = df['gouvernorat'] * 1000 + df['prix_decile']
        df = df.drop(columns=['prix_decile'])

        groupes[nom] = df
        nuniq = df['ville_encoded'].nunique()
        vrange = f"{df['ville_encoded'].min()}-{df['ville_encoded'].max()}"
        _log(f"✔ {nom:<15}: ville_encoded nuniq={nuniq} range={vrange}")

    return groupes


# ================================================================
# ÉTAPE 4 — NETTOYAGE SEUILS
# ================================================================

def clean_seuils(groupes):
    print("\n" + "=" * 65)
    print("   ETAPE 4 — NETTOYAGE SEUILS")
    print("=" * 65)
    for nom, df in groupes.items():
        avant = len(df)
        p_min, p_max = SEUILS_PRIX_M2.get(nom, (5, 10000))
        df = df[(df['prix_m2'] >= p_min) & (df['prix_m2'] <= p_max)]
        v_min, v_max = SEUILS_VARIATION
        df = df[(df['variation_prix_m2'] >= v_min) & (df['variation_prix_m2'] <= v_max)]
        df = df[(df['indice_rentabilite_regionale'] >= 0) &
                (df['indice_rentabilite_regionale'] <= 100)]
        groupes[nom] = df.reset_index(drop=True)
        _log(f"{nom:<15}: {avant:>6} → {len(df):>6} ({avant - len(df):>4} supprimés)")
    return groupes


# ================================================================
# ÉTAPE 5 — ISOLATION FOREST
# ================================================================

def isolation_forest(groupes, contamination=0.02):
    print("\n" + "=" * 65)
    print("   ETAPE 5 — ISOLATION FOREST (contamination=2%)")
    print("=" * 65)
    features_if = ['prix_m2', 'variation_prix_m2', 'nb_amenities_total',
                   'densite_routes_km', 'ville_encoded']
    for nom, df in groupes.items():
        if len(df) < 50:
            _log(f"{nom:<15}: trop peu ({len(df)}) — IF ignoré")
            continue
        cols = [c for c in features_if if c in df.columns]
        X = df[cols].fillna(df[cols].median())
        avant = len(df)
        clf = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
        mask = clf.fit_predict(X) == 1
        groupes[nom] = df[mask].reset_index(drop=True)
        _log(f"{nom:<15}: {avant:>6} → {len(groupes[nom]):>6} ({avant - len(groupes[nom]):>4} outliers)")
    return groupes


# ================================================================
# ÉTAPE 6 — VALIDATION + 15 COLONNES FINALES
# ================================================================

def validate_and_finalize(groupes):
    print("\n" + "=" * 65)
    print("   ETAPE 6 — VALIDATION + 15 COLONNES FINALES")
    print("=" * 65)
    for nom, df in groupes.items():
        # NaN TARGET
        df = df.dropna(subset=['indice_rentabilite_regionale'])

        # Sélection 15 colonnes finales disponibles
        cols = [c for c in FINAL_COLS_BO5 if c in df.columns]
        df = df[cols].copy()

        # NaN restants → médiane
        nan_tot = df.isna().sum().sum()
        if nan_tot > 0:
            for col in df.select_dtypes(include='number').columns:
                df[col] = df[col].fillna(df[col].median())

        groupes[nom] = df.reset_index(drop=True)
        t_mean = df['indice_rentabilite_regionale'].mean()
        t_std  = df['indice_rentabilite_regionale'].std()
        _log(f"✔ {nom:<13}: {len(df):>6} lignes | {len(df.columns)} cols | "
             f"TARGET mean={t_mean:.2f} std={t_std:.2f} | NaN={df.isna().sum().sum()}")

    return groupes


# ================================================================
# RAPPORT NETTOYAGE
# ================================================================

def print_cleaning_report(groupes, n_avant):
    print("\n" + "=" * 65)
    print("   RAPPORT NETTOYAGE BO5")
    print("=" * 65)
    total_avant = sum(n_avant.values())
    total_apres = sum(len(df) for df in groupes.values())
    print(f"\n  {'Groupe':<15} {'Avant':>8} {'Après':>8} {'Supprimés':>10} {'TARGET mean':>12}")
    print(f"  {'-'*56}")
    for nom, df in groupes.items():
        avant = n_avant.get(nom, 0)
        apres = len(df)
        t     = df['indice_rentabilite_regionale'].mean()
        print(f"  {nom:<15} {avant:>8,} {apres:>8,} {avant-apres:>10,} {t:>12.2f}")
    print(f"  {'-'*56}")
    print(f"  {'TOTAL':<15} {total_avant:>8,} {total_apres:>8,} {total_avant-total_apres:>10,}")
    print(f"\n  Colonnes finales ({len(FINAL_COLS_BO5)}) :")
    for i, col in enumerate(FINAL_COLS_BO5, 1):
        note = ''
        if col == 'indice_rentabilite_regionale': note = ' ⭐ TARGET'
        if col == 'ville_encoded':                note = ' ← nouveau BO5'
        print(f"    {i:>2}. {col}{note}")
