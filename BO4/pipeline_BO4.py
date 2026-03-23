"""
pipeline_BO4.py — Orchestrateur principal pour l'Objectif 4 : Conformité Juridique.

Workflow (même structure que BO2/BO3) :
  [0] SOURCES         — Détection automatique des fichiers
  [1] ETL             — Chargement contrats + textes JORT
  [2] DÉDUP           — Suppression doublons exacts
  [3] VALIDATION      — Détection type/clauses/risques depuis texte arabe
  [4] ENCODAGE        — Catégorielles → entiers
  [5] CONSTRUCTION    — 3 datasets (contrats / JORT RAG / règles)
  [6] EXPORT          — 3 fichiers Excel + encoding_mappings_BO4.json

Usage :
  python pipeline_BO4.py
  → Génère : dataset_BO4_contrats_final.xlsx
             dataset_BO4_jort_chunks.xlsx
             dataset_BO4_rules.xlsx
             encoding_mappings_BO4.json
"""

import os as _os
import warnings
warnings.filterwarnings('ignore')

from mappings_BO4 import print_encoding_summary
import cleaning_BO4 as clng
import modeling_BO4  as mdlg


# ================================================================
# PATHS — auto-détection (même logique BO2/BO3)
# ================================================================

def _find_file(*candidates: str) -> str:
    for c in candidates:
        if _os.path.exists(c):
            return c
    return candidates[-1]


# Contrats annotés (généré par le processus de collecte)
CONTRATS_PATH = _find_file(
    'dataset_BO4_contrats_final.csv',
    'dataset_contrats_BO4_final.csv',
    'BO4/dataset_BO4_contrats_final.csv',
    'BO4/dataset_contrats_BO4_final.csv',
    '../BO4/dataset_BO4_contrats_final.csv',
    '../BO4/dataset_contrats_BO4_final.csv',
    '../dataset_BO4_contrats_final.csv',
    '../dataset_contrats_BO4_final.csv',
)

# Textes JORT (lois, décrets)
JORT_PATH = _find_file(
    'dataset_conformite_immobilier_tunisien.csv',
    'BO4/dataset_conformite_immobilier_tunisien.csv',
    '../BO4/dataset_conformite_immobilier_tunisien.csv',
)

# Répertoire de sortie
OUTPUT_DIR = _find_file('BO4CORRECTED', '.', '../BO4CORRECTED')
_os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"  Contrats path : {CONTRATS_PATH}  ({'✔' if _os.path.exists(CONTRATS_PATH) else '✗'})")
print(f"  JORT path     : {JORT_PATH}  ({'✔' if _os.path.exists(JORT_PATH) else '✗'})")
print(f"  Output dir    : {OUTPUT_DIR}")


# ================================================================
# PIPELINE
# ================================================================

def run_pipeline() -> None:

    # ════════════════════════════════════════════════════════
    # PHASE 0 — ENCODAGES
    # ════════════════════════════════════════════════════════
    print_encoding_summary()

    # ════════════════════════════════════════════════════════
    # PHASE 1 — ETL : chargement sources
    # ════════════════════════════════════════════════════════

    # Étape 1a : Contrats annotés
    df_contrats = clng.load_contrats(CONTRATS_PATH)
    n_contrats  = len(df_contrats)

    if n_contrats == 0:
        print("\n" + "=" * 65)
        print("  [ERREUR] Fichier contrats introuvable.")
        print(f"  Chemin recherché : {CONTRATS_PATH}")
        print("  Vérifiez que 'dataset_BO4_contrats_final.csv'")
        print("  ou 'dataset_contrats_BO4_final.csv' est dans")
        print("  le même dossier que pipeline_BO4.py")
        print("=" * 65)
        return

    # Étape 1b : Textes JORT
    df_jort = clng.load_jort(JORT_PATH, max_rows=6000)
    n_jort  = len(df_jort)

    # ════════════════════════════════════════════════════════
    # PHASE 2 — NETTOYAGE CONTRATS
    # ════════════════════════════════════════════════════════

    # Étape 2 : Déduplication
    df_contrats, n_avant = clng.deduplicate(df_contrats)

    # Étape 3 : Validation + enrichissement automatique
    df_contrats = clng.validate_and_enrich(df_contrats)

    # Étape 4 : Encodage catégoriel
    df_contrats = clng.encode_categorical(df_contrats)

    # Étape 4b : Valeurs manquantes
    df_contrats = clng.handle_missing(df_contrats)

    # ════════════════════════════════════════════════════════
    # PHASE 3 — CONSTRUCTION DATASETS + EXPORT
    # ════════════════════════════════════════════════════════

    # Étape 5a : Dataset contrats (NLP + ML alertes)
    df_final_contrats = mdlg.build_contrats_dataset(df_contrats)

    # Étape 5b : Dataset JORT chunks (RAG)
    df_final_rag = mdlg.build_rag_dataset(df_jort)

    # Étape 5c : Dataset règles juridiques (Rule-based)
    df_final_rules = mdlg.build_rules_dataset()

    # Étape 6 : Export Excel + JSON
    mdlg.export_datasets(df_final_contrats, df_final_rag, df_final_rules,
                         output_dir=OUTPUT_DIR)

    # Rapport final
    mdlg.print_final_report(df_final_contrats, df_final_rag, df_final_rules)


if __name__ == '__main__':
    run_pipeline()
