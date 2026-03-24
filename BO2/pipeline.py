"""
pipeline.py — Main orchestrator for the Tunisia Real Estate Pipeline v8.

What changed vs v7:
  - external_data.py added : loads BCT, INS, Signaux Google Maps
  - MARKET_CONTEXT  : computed from real BCT taux directeur + INS PIB/inflation
  - NEGO_BASE       : computed from real INS glissements prix immobilier
  - market_tension  : enriched with score_attractivite + nb_immo_direct (signaux)
  - temp_high_season: derived from actual listing seasonality in dataset
  - image_quality_score: enriched with nb_immo_direct from signaux (not hardcoded per source)
  - prix / surface_m2 / nb_pieces: NEVER imputed — NaN kept, illogical rows DROPPED
  - taux_directeur column added to output (from BCT)
  - score_attractivite column added to output (from signaux)

Workflow:
  [0] EXTERNAL DATA   — load BCT + INS + Signaux, build market_context + nego_rates
  [1] ENCODING/MAPPING — print encoding summary, build geo coords
  [2] DATA CLEANING    — ETL, dedup, coerce, standardize, segment,
                         clean (DROP invalid), handle missing (no imputation for prix/surf),
                         encode, geocode
  [3] POST-CLEANING   — market features, temporal, vision, NLP, multimodal, target, export
"""

import warnings
warnings.filterwarnings('ignore')

from mappings import print_encoding_summary
import external_data as ext
import cleaning      as clng
import modeling      as mdlg


# ================================================================
# PATHS — auto-detects whether external files are in subdirectory or same folder
# ================================================================
import os as _os

def _find_file(*candidates: str) -> str:
    """Returns the first path that exists, or the last candidate as fallback."""
    for c in candidates:
        if _os.path.exists(c):
            return c
    return candidates[-1]

SATELLITE_DIR = _find_file(
    'Tunisia_Satellite_scraper/tunisia_satellite_data',
    '../Tunisia_Satellite_scraper/tunisia_satellite_data',
)

# BCT — tries same-folder, subfolder, and parent-folder variants
BCT_PATH = _find_file(
    'bct_dataset_20260219_1332.xlsx',
    'bct/bct_dataset_20260219_1332.xlsx',
    '../bct/bct_dataset_20260219_1332.xlsx',
)

# INS
INS_PATH = _find_file(
    'ins_dataset_20260219_1307.xlsx',
    'Ins/ins_dataset_20260219_1307.xlsx',
    '../Ins/ins_dataset_20260219_1307.xlsx',
)

# Signaux Google Maps
SIGNAUX_PATH = _find_file(
    'signaux_immobilier_tunisie.xlsx',
    'goolgeMaps/signaux_immobilier_tunisie.xlsx',
    '../goolgeMaps/signaux_immobilier_tunisie.xlsx',
    'googleMaps/signaux_immobilier_tunisie.xlsx',
)

print(f"  BCT path     : {BCT_PATH}  ({'✔ found' if _os.path.exists(BCT_PATH) else '✗ missing'})")
print(f"  INS path     : {INS_PATH}  ({'✔ found' if _os.path.exists(INS_PATH) else '✗ missing'})")
print(f"  Signaux path : {SIGNAUX_PATH}  ({'✔ found' if _os.path.exists(SIGNAUX_PATH) else '✗ missing'})")

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

CURRENT_QUARTER = (2026, 1)


# ================================================================
# PIPELINE
# ================================================================

def run_pipeline() -> None:

    # ════════════════════════════════════════════════════════════════
    # PHASE 0 — EXTERNAL DATA (BCT + INS + SIGNAUX)
    # ════════════════════════════════════════════════════════════════

    external = ext.load_all(
        bct_path        = BCT_PATH,
        ins_path        = INS_PATH,
        sig_path        = SIGNAUX_PATH,
        current_quarter = CURRENT_QUARTER,
    )

    market_context = external['market_context']   # {(year,quarter): (taux, cycle)}
    nego_rates     = external['nego_rates']        # {type_bien: taux_nego}
    gov_features   = external['gov_features']      # {gouvernorat: features_dict}
    bct            = external['bct']               # {taux_directeur, taux_moyen, tre, date}


    # ════════════════════════════════════════════════════════════════
    # PHASE 1 — ENCODING / MAPPING
    # ════════════════════════════════════════════════════════════════

    print_encoding_summary()
    gouvernorat_coords = clng.build_gouvernorat_coords(SATELLITE_DIR)


    # ════════════════════════════════════════════════════════════════
    # PHASE 2 — DATA CLEANING
    # ════════════════════════════════════════════════════════════════

    # Step 1: ETL
    df, n0 = clng.load_sources(SOURCES_IMMO)
    n_sources = len(SOURCES_IMMO)

    # Step 2: Deduplication
    df, n_apres_dedup = clng.deduplicate(df)

    # Step 2b: Type coercion
    df = clng.coerce_types(df)

    # Step 3: Standardization (type_bien, type_transaction, nb_pieces from description)
    df = clng.standardize(df)

    # Step 4: Segmentation
    df = clng.segment(df)

    # Step 5: Per-group cleaning — DROP invalid rows, no imputation for prix/surface
    groupes_clean = clng.clean_groups(df)

    # Step 5b: Handle missing — only type_categorise imputed, prix/surface kept as NaN
    groupes_clean = clng.handle_missing(groupes_clean)

    # Step 5d: Categorical encoding
    df, groupes_clean = clng.encode_categorical(df, groupes_clean)

    # Step 5e: Ville encoding (frequency-based)
    # Computes ville_encoded from the actual distribution in the data.
    # Returns rank_map for reference (can be saved for inference).
    df, groupes_clean, ville_rank_map, ville_rank_map_full = clng.encode_ville(df, groupes_clean, min_freq=30)
    _ville_n_top = len(ville_rank_map)  # number of frequent villes

    # Step 6: Geocoding
    df = clng.geocode(df, gouvernorat_coords)

    # Step 6b: Reverse geocoding correction
    df, groupes_clean = clng.reverse_geocode_correction(df, groupes_clean)

    # ── Rebuild df from groupes_clean ────────────────────────────────
    # After steps 5–6b, groupes_clean contains only clean rows.
    # df still holds all 39k rows (needed for geocoding lookups above).
    # From here on, all modeling steps (7–11) must work on clean rows only.
    # We rebuild df as the union of all clean groups, re-attaching
    # the geocoding columns (lat, lon, gouvernorat) from the original df.
    import pandas as _pd
    df_clean = _pd.concat(groupes_clean.values(), ignore_index=False)
    # Propagate ALL columns computed on df that are not yet in groupes_clean
    # This includes: lat, lon, gouvernorat, _gouvernorat_str, ville_encoded
    # and any modeling columns added after step 6b (market_tension, cycle_marche, etc.)
    _valid_idx = [i for i in df_clean.index if i in df.index]
    for _col in df.columns:
        if _col not in df_clean.columns and _valid_idx:
            df_clean.loc[_valid_idx, _col] = df.loc[_valid_idx, _col].values
    df = df_clean
    clng.log(f"  df reconstruit depuis groupes_clean : {len(df):,} lignes propres")
    clng.log(f"  Colonnes disponibles               : {len(df.columns)}")
    clng.log(f"  prix NaN dans df reconstruit        : {df['prix'].isna().sum()}")
    del df_clean, _pd


    # ════════════════════════════════════════════════════════════════
    # PHASE 3 — POST-CLEANING MODELING
    # ════════════════════════════════════════════════════════════════

    # Save encoding mappings
    mdlg.save_encoding_mappings(ville_rank_map=ville_rank_map_full, n_top=_ville_n_top)

    # Step 5c: Distribution visualization
    mdlg.plot_distributions(groupes_clean)

    # Step 7: Market features (enriched with real signaux data)
    df = mdlg.compute_market_features(df, SATELLITE_DIR, gov_features)

    # Step 8: Temporal features (cycle_marche from real BCT+INS)
    df = mdlg.compute_temporal_features(df, market_context)

    # Step 9: Vision features (image quality enriched by signaux)
    df, img_embed_cols = mdlg.run_vision_features(df, gov_features)

    # Step 10: NLP text embedding
    df, _embeddings = mdlg.run_text_embedding(df)
    mdlg.run_image_embedding_save(df, img_embed_cols)

    # Step 10b: Multimodal fusion
    df = mdlg.run_multimodal_fusion(df)

    # Step 11: Target estimation (real nego rates, NaN kept if prix absent)
    df = mdlg.compute_target(df, nego_rates)

    # Step 11b: Sample weights — corrige déséquilibres géographique + temporel
    # geo_weight  : sous-représentés (Béja, Gafsa...) → weight > 1
    # temp_weight : cycles rares (stabilization, decline) → weight > 1
    # Utilisation : model.fit(X, y, sample_weight=df['sample_weight'])
    clng.section("ETAPE 11b — SAMPLE WEIGHTS")
    df = mdlg.compute_sample_weights(df)

    # ── Garantie : toutes les colonnes clés sont présentes et sans NaN ──────
    # score_attractivite, market_tension, cycle_marche, text_embedding_score
    # sont calculées sur df reconstruit. Si des NaN subsistent (index mismatch
    # ou ville inconnue), on remplace par des valeurs cohérentes :
    #   - score_attractivite : 0.5 (attractivité neutre si gouvernorat inconnu dans signaux)
    #   - market_tension     : médiane globale (marché moyen)
    #   - cycle_marche       : 2 (growth — valeur la plus fréquente 2025-2026)
    #   - text_embedding_score : médiane (score moyen si description manquante)
    import pandas as _pd
    _med_tension = df['market_tension'].median() if 'market_tension' in df.columns else 0.5
    _med_text    = df['text_embedding_score'].median() if 'text_embedding_score' in df.columns else 0.5
    # ── Règles de validation : NaN ET zéros invalides ──────────────
    # score_attractivite = 0 : impossible (min signaux = 0.23) → erreur mapping
    # market_tension = 0     : impossible si gouvernorat connu → erreur calcul
    # cycle_marche = 0       : encodage 'Unknown' → jamais valide dans output final
    # text_embedding_score   : NaN seulement (0 est un score valide théoriquement)
    _med_tension = df['market_tension'].replace(0, _pd.NA).median() if 'market_tension' in df.columns else 0.5
    _med_score   = df['score_attractivite'].replace(0, _pd.NA).median() if 'score_attractivite' in df.columns else 0.5
    _med_text    = df['text_embedding_score'].median() if 'text_embedding_score' in df.columns else 0.5

    _fill_rules = {
        # (fill_for_nan, fill_for_zero, zero_is_invalid)
        'score_attractivite':   (_med_score,   _med_score,   True),
        'market_tension':       (_med_tension, _med_tension, True),
        'cycle_marche':         (2,            2,            True),
        'text_embedding_score': (_med_text,    None,         False),
    }
    for _col, (_fill_nan, _fill_zero, _zero_invalid) in _fill_rules.items():
        if _col not in df.columns:
            clng.log(f"  [WARN] {_col} absent de df")
            continue
        _n_nan  = df[_col].isna().sum()
        _n_zero = (df[_col] == 0).sum() if _zero_invalid else 0
        if _n_nan > 0:
            df[_col] = df[_col].fillna(_fill_nan)
            clng.log(f"  [{_col}] {_n_nan} NaN → {round(float(_fill_nan),3)}")
        if _zero_invalid and _n_zero > 0:
            df.loc[df[_col] == 0, _col] = _fill_zero
            clng.log(f"  [{_col}] {_n_zero} zéros invalides → {round(float(_fill_zero),3)}")
        if _n_nan == 0 and _n_zero == 0:
            clng.log(f"  [{_col}] ✔ aucun NaN ni zéro invalide")

    del _pd, _med_tension, _med_text, _fill_rules

    # Step 12: Re-segmentation + Excel export
    groupes_clean = mdlg.resegment_and_export(df, groupes_clean, img_embed_cols)

    # Final report
    mdlg.print_final_report(groupes_clean, n_sources, n0, n_apres_dedup, bct, nego_rates)


if __name__ == '__main__':
    run_pipeline()
