"""
pipeline_BO3.py — Orchestrateur principal pour l'Objectif 3 : Tendances Régionales.

Workflow (comme BO2 mais adapté aux séries temporelles) :
  [0] EXTERNAL DATA   — BCT + INS + Signaux + Satellite + Google Maps
  [1] ETL             — Fusion 8 sources immobilières
  [2] DEDUP           — 2 passes de déduplication
  [3] STANDARDISATION — Dates, prix, surface, gouvernorat, prix_m2
  [4] SEGMENTATION    — 4 groupes (Residentiel, Foncier, Commercial, Divers)
  [5] NETTOYAGE       — Seuils + Isolation Forest
  [6] ENRICHISSEMENT  — Google Maps, Satellite, INS/BCT
  [7] TARGET          — indice_prix_m2_regional (gov × annee × mois)
  [8] EXPORT          — 4 fichiers Excel + encoding_mappings_BO3.json

Usage :
  python pipeline_BO3.py
  → Génère : residentiel_BO3.xlsx, foncier_BO3.xlsx,
             commercial_BO3.xlsx, divers_BO3.xlsx
"""

import os as _os
import warnings
warnings.filterwarnings('ignore')

from mappings_BO3       import print_encoding_summary, FICHIERS_ML
import external_data_BO3 as ext
import cleaning_BO3      as clng
import modeling_BO3      as mdlg


# ================================================================
# PATHS — auto-détection
# ================================================================

def _find_file(*candidates: str) -> str:
    for c in candidates:
        if _os.path.exists(c):
            return c
    return candidates[-1]

SATELLITE_DIR = _find_file(
    'Tunisia_Satellite_scraper/tunisia_satellite_data',
    '../Tunisia_Satellite_scraper/tunisia_satellite_data',
)
BCT_PATH = _find_file(
    'bct_dataset_20260219_1332.xlsx',
    'bct/bct_dataset_20260219_1332.xlsx',
    '../bct/bct_dataset_20260219_1332.xlsx',
)
INS_PATH = _find_file(
    'ins_dataset_20260219_1307.xlsx',
    'Ins/ins_dataset_20260219_1307.xlsx',
    '../Ins/ins_dataset_20260219_1307.xlsx',
)
SIGNAUX_PATH = _find_file(
    'signaux_immobilier_tunisie.xlsx',
    'goolgeMaps/signaux_immobilier_tunisie.xlsx',
    '../goolgeMaps/signaux_immobilier_tunisie.xlsx',
)
GOOGLE_JSON = _find_file(
    'raw_data.json',
    'goolgeMaps/raw_data.json',
    '../goolgeMaps/raw_data.json',
)

# Répertoire de sortie
OUTPUT_DIR = _find_file('.', '../BO3CORRECTED', '.')

print(f"  BCT path      : {BCT_PATH}  ({'✔' if _os.path.exists(BCT_PATH) else '✗'})")
print(f"  INS path      : {INS_PATH}  ({'✔' if _os.path.exists(INS_PATH) else '✗'})")
print(f"  Signaux path  : {SIGNAUX_PATH}  ({'✔' if _os.path.exists(SIGNAUX_PATH) else '✗'})")
print(f"  Satellite dir : {SATELLITE_DIR}  ({'✔' if _os.path.exists(SATELLITE_DIR) else '✗'})")
print(f"  Google JSON   : {GOOGLE_JSON}  ({'✔' if _os.path.exists(GOOGLE_JSON) else '✗'})")

SOURCES_IMMO = [
    ('Facebook Marketplace', '../marketplace/data/facebook_marketplace_2185_annonces_20260216_222412.csv', 'csv', None),
    ('Mubawab',              '../mubawab/mubawab_annonces.csv',                                            'csv', None),
    ('Mubawab Partial',      '../mubawab2/mubawab_partial_120.xlsx',                                       'xlsx', None),
    ('Tayara',               '../tayara/tayara_complete.csv',                                              'csv', None),
    ('Tunisie Annonces',     '../tunisie_annance_scraper/ta_properties.csv',                               'csv', None),
    ('Century21',            '../scrapping/century21_data2.csv',                                           'csv', ';'),
    ('HomeInTunisia',        '../scrapping/homeintunisia_data2.csv',                                       'csv', ';'),
    ('BnB',                  '../bnb/bnb_properties.csv',                                                  'csv', None),
]


# ================================================================
# PIPELINE
# ================================================================

def run_pipeline() -> None:

    # ════════════════════════════════════════════════════════
    # PHASE 0 — DONNÉES EXTERNES
    # ════════════════════════════════════════════════════════
    external = ext.load_all(
        bct_path    = BCT_PATH,
        ins_path    = INS_PATH,
        sig_path    = SIGNAUX_PATH,
        sat_dir     = SATELLITE_DIR,
        google_path = GOOGLE_JSON,
    )
    bct = external['bct']

    # ════════════════════════════════════════════════════════
    # PHASE 1 — ENCODAGES
    # ════════════════════════════════════════════════════════
    print_encoding_summary()

    # ════════════════════════════════════════════════════════
    # PHASE 2 — ETL + NETTOYAGE
    # ════════════════════════════════════════════════════════

    # Étape 1 : ETL
    df, n0 = clng.load_sources(SOURCES_IMMO)
    n_sources = len(SOURCES_IMMO)

    # Étape 2 : Déduplication
    df, n_dedup = clng.deduplicate(df)

    # Étape 3 : Standardisation (dates, prix, gouvernorat, prix_m2)
    df = clng.standardize(df)

    # Étape 4 : Segmentation
    df = clng.segment(df)

    # Étape 4b : Encodage ville
    df = clng.encode_ville(df, min_freq=30)

    # Étape 5 : Nettoyage + Isolation Forest (même que BO2)
    groupes_clean = clng.clean_groups(df)

    # Étape 5b : Gestion valeurs manquantes (même que BO2 — impute surface_m2)
    groupes_clean = clng.handle_missing(groupes_clean)

    # Étape 5c : Encodage gouvernorat + suppression Unknown
    df, groupes_clean = clng.encode_categorical(df, groupes_clean)

    # ════════════════════════════════════════════════════════
    # PHASE 3 — ENRICHISSEMENT + TARGET + EXPORT
    # ════════════════════════════════════════════════════════

    # Étape 6 : Enrichissement multi-sources
    groupes_clean = mdlg.enrich_external(groupes_clean, external)

    # Étape 7 : TARGET — indice_prix_m2_regional
    groupes_clean = mdlg.compute_regional_index(groupes_clean)

    # Étape 8 : Export Excel + JSON
    mdlg.export_datasets(groupes_clean, output_dir=OUTPUT_DIR)

    # Rapport final
    mdlg.print_final_report(groupes_clean, n_sources, n0, n_dedup, bct)


if __name__ == '__main__':
    run_pipeline()
