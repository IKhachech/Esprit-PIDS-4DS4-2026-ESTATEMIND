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

Structure attendue sur disque :
  agent_scraper/          ← racine du projet
  ├── BO3/                ← ce fichier est ici
  │   └── pipeline_BO3.py
  ├── bct/
  ├── bnb/
  ├── goolgeMaps/
  ├── Ins/
  ├── marketplace/
  ├── mubawab/
  ├── mubawab2/
  ├── scrapping/
  ├── tayara/
  ├── Tunisia_Satellite_scraper/
  └── tunisie_annance_scraper/

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

# ─── Feature Registry centralisé ──────────────────────────────────────────────
try:
    from features_config import print_feature_registry as _print_registry
    _REGISTRY_AVAILABLE = True
except ImportError:
    _REGISTRY_AVAILABLE = False
    def _print_registry(): pass

# ─── Chemins ───────────────────────────────────────────────────────────────────
# BASE_DIR  = .../agent_scraper/BO3
# DATA_ROOT = .../agent_scraper          ← les datasets sont ICI (un niveau au-dessus de BO3)
BASE_DIR  = _os.path.dirname(_os.path.abspath(__file__))
DATA_ROOT = _os.environ.get('BO3_DATA_ROOT', _os.path.abspath(_os.path.join(BASE_DIR, '..')))


def _find_file(*candidates: str) -> str:
    """
    Cherche un fichier parmi plusieurs candidats.
    Teste dans l'ordre :
      1. Chemin absolu direct
      2. Relatif au répertoire courant
      3. Relatif à DATA_ROOT (agent_scraper/)
      4. Relatif à BASE_DIR  (agent_scraper/BO3/)
    Retourne le dernier candidat si rien trouvé.
    """
    for c in candidates:
        if _os.path.isabs(c) and _os.path.exists(c):
            return c
        if _os.path.exists(c):
            return c
        alt = _os.path.join(DATA_ROOT, c)
        if _os.path.exists(alt):
            return alt
        alt2 = _os.path.join(BASE_DIR, c)
        if _os.path.exists(alt2):
            return alt2
    return candidates[-1]


# ─── Fichiers externes ─────────────────────────────────────────────────────────
# Tous relatifs à DATA_ROOT = agent_scraper/
SATELLITE_DIR = _find_file(
    'Tunisia_Satellite_scraper/tunisia_satellite_data',
)
BCT_PATH = _find_file(
    'bct/bct_dataset_20260219_1332.xlsx',
)
INS_PATH = _find_file(
    'Ins/ins_dataset_20260219_1307.xlsx',
)
SIGNAUX_PATH = _find_file(
    'goolgeMaps/signaux_immobilier_tunisie.xlsx',
)
GOOGLE_JSON = _find_file(
    'goolgeMaps/raw_data.json',
)

# Répertoire de sortie = agent_scraper/BO3/ (même dossier que ce fichier)
OUTPUT_DIR = BASE_DIR

# ─── Diagnostic chemins ────────────────────────────────────────────────────────
print(f"  DATA_ROOT      : {DATA_ROOT}")
print(f"  BCT path      : {BCT_PATH}  ({'✔' if _os.path.exists(BCT_PATH) else '✗'})")
print(f"  INS path      : {INS_PATH}  ({'✔' if _os.path.exists(INS_PATH) else '✗'})")
print(f"  Signaux path  : {SIGNAUX_PATH}  ({'✔' if _os.path.exists(SIGNAUX_PATH) else '✗'})")
print(f"  Satellite dir : {SATELLITE_DIR}  ({'✔' if _os.path.exists(SATELLITE_DIR) else '✗'})")
print(f"  Google JSON   : {GOOGLE_JSON}  ({'✔' if _os.path.exists(GOOGLE_JSON) else '✗'})")

# ─── Sources immobilières ──────────────────────────────────────────────────────
# Tous les fichiers CSV/XLSX cherchés dans DATA_ROOT = agent_scraper/
SOURCES_IMMO = [
    ('Facebook Marketplace', _find_file('marketplace/data/facebook_marketplace_2185_annonces_20260216_222412.csv'), 'csv', None),
    ('Mubawab',              _find_file('mubawab/mubawab_annonces.csv'),                                            'csv', None),
    ('Mubawab Partial',      _find_file('mubawab2/mubawab_partial_120.xlsx'),                                       'xlsx', None),
    ('Tayara',               _find_file('tayara/tayara_complete.csv'),                                              'csv', None),
    ('Tunisie Annonces',     _find_file('tunisie_annance_scraper/ta_properties.csv'),                               'csv', None),
    ('Century21',            _find_file('scrapping/century21_data2.csv'),                                           'csv', ';'),
    ('HomeInTunisia',        _find_file('scrapping/homeintunisia_data2.csv'),                                       'csv', ';'),
    ('BnB',                  _find_file('bnb/bnb_properties.csv'),                                                  'csv', None),
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
    # PHASE 1 — ENCODAGES + FEATURE REGISTRY
    # ════════════════════════════════════════════════════════
    print_encoding_summary()
    if _REGISTRY_AVAILABLE:
        _print_registry()
    else:
        print("\n  [INFO] features_config.py non trouvé → Feature Registry non affiché")

    # ════════════════════════════════════════════════════════
    # PHASE 2 — ETL + NETTOYAGE
    # ════════════════════════════════════════════════════════

    # Étape 1 : ETL
    df, n0 = clng.load_sources(SOURCES_IMMO)
    n_sources = len(SOURCES_IMMO)

    if df.empty:
        print("\nAucune source immobilière chargée — pipeline interrompu")
        return

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

    # Étape 7 : TARGET robuste (MODIFIÉ — modeling_BO3 v2)
    #   ① Lissage (winsorisation 10-90 + rolling median)
    #   ② Imputation hiérarchique petits gouvernorats
    #   ③ potentiel_emergent finalisé sur indice lissé
    #   ④ Outliers MAD par gov × type
    #   ⑤ indice_loc_lisse + indice_ven_lisse exportés
    #   external passé en paramètre pour score_attractivite → potentiel_emergent
    groupes_clean = mdlg.compute_regional_index(groupes_clean, external)

    # Étape 6b : Variables discriminantes (NOUVEAU — modeling_BO3 v2)
    #   Calcule : zone_geographique, potentiel_emergent, indice_liquidite,
    #             volatilite_prix_trim — utilisées par kmeans, pelt, lstm
    groupes_clean = mdlg.add_discriminant_features(groupes_clean)

    # Étape 8 : Export Excel + JSON
    mdlg.export_datasets(groupes_clean, output_dir=OUTPUT_DIR)

    # Rapport final
    mdlg.print_final_report(groupes_clean, n_sources, n0, n_dedup, bct)


if __name__ == '__main__':
    run_pipeline()