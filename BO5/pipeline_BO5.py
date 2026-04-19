"""
pipeline_BO5.py — Orchestrateur principal pour l'Objectif 5 : Rentabilité Régionale.

Workflow (même structure que BO2/BO3) :
  [0] ENCODAGES         — Résumé des constantes
  [1] CHARGEMENT        — 4 fichiers *_BO5.xlsx
  [2] COLONNES NULLES   — Suppression 4 colonnes = 0 partout
  [3] ENRICHISSEMENT OSM— Remplacement fallback par vraies valeurs signaux ← NOUVEAU
  [4] ville_encoded     — Encodage fréquentiel sous-zone (BO2/BO3)
  [5] SEUILS            — Nettoyage prix_m2 + variation
  [6] ISOLATION FOREST  — Outliers multivariés (contamination=2%)
  [7] VALIDATION        — NaN TARGET + 14 colonnes finales
  [8] MONTE CARLO       — N=1000 simulations par ligne
  [9] EXPORT            — 4 fichiers Excel + encoding_mappings_BO5.json

Usage :
  python pipeline_BO5.py
  → Génère : residentiel_BO5.xlsx, foncier_BO5.xlsx,
             commercial_BO5.xlsx,  divers_BO5.xlsx
"""

import os as _os
import warnings
warnings.filterwarnings('ignore')

from mappings_BO5        import print_encoding_summary, FICHIERS_BO5
import cleaning_BO5      as clng
import modeling_BO5      as mdlg
import external_data_BO5 as ext


def _find_file(*candidates):
    for c in candidates:
        if _os.path.exists(c):
            return c
    return candidates[-1]


# ── Fichiers sources BO5 ────────────────────────────────────────
PATHS_BO5 = {
    'residentiel': _find_file('residentiel_BO5.xlsx', '../residentiel_BO5.xlsx'),
    'foncier':     _find_file('foncier_BO5.xlsx',     '../foncier_BO5.xlsx'),
    'commercial':  _find_file('commercial_BO5.xlsx',  '../commercial_BO5.xlsx'),
    'divers':      _find_file('divers_BO5.xlsx',      '../divers_BO5.xlsx'),
}

# ── Fichier signaux OSM ──────────────────────────────────────────
SIGNAUX_PATH = _find_file(
    'signaux_immobilier_tunisie.xlsx',
    '../goolgeMaps/signaux_immobilier_tunisie.xlsx',
    '../signaux_immobilier_tunisie.xlsx',
)

OUTPUT_DIR = _find_file('BO5CORRECTED', '.', '../BO5CORRECTED')
_os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"  Fichiers sources :")
for nom, path in PATHS_BO5.items():
    print(f"    {nom:<15}: {path}  ({'✔' if _os.path.exists(path) else '✗'})")
print(f"  Signaux OSM    : {SIGNAUX_PATH}  ({'✔' if _os.path.exists(SIGNAUX_PATH) else '✗ (valeurs hardcodées)'})")
print(f"  Output dir     : {OUTPUT_DIR}")


def run_pipeline():

    print_encoding_summary()

    # ── PHASE 1 : Chargement ──────────────────────────────────
    groupes = clng.load_datasets(PATHS_BO5)
    if not groupes:
        print("\n  [ERREUR] Aucun fichier *_BO5.xlsx trouvé.")
        return
    n_avant = {nom: len(df) for nom, df in groupes.items()}

    # ── PHASE 2 : Nettoyage + Enrichissement ──────────────────

    # Étape 2 : Suppression colonnes nulles
    groupes = clng.drop_null_columns(groupes)

    # Étape 3 : Enrichissement OSM réel depuis signaux ← NOUVEAU
    groupes = ext.enrich_with_real_osm(groupes, SIGNAUX_PATH)

    # Étape 4 : ville_encoded
    groupes = clng.encode_ville(groupes)

    # Étape 5 : Seuils
    groupes = clng.clean_seuils(groupes)

    # Étape 6 : Isolation Forest
    groupes = clng.isolation_forest(groupes, contamination=0.02)

    # Étape 7 : Validation + 14 colonnes finales
    groupes = clng.validate_and_finalize(groupes)

    clng.print_cleaning_report(groupes, n_avant)

    # ── PHASE 3 : Monte Carlo + Export ────────────────────────

    # Étape 8 : Monte Carlo
    groupes = mdlg.run_monte_carlo(groupes, n_simulations=1000, seed=42)

    # Étape 9 : Export
    mdlg.export_datasets(groupes, output_dir=OUTPUT_DIR)
    mdlg.print_final_report(groupes)


if __name__ == '__main__':
    run_pipeline()
