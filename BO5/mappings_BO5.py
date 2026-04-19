"""
mappings_BO5.py — Constantes, encodages et colonnes pour l'Objectif 5 : Rentabilité Régionale.

TARGET : indice_rentabilite_regionale [0-100]
  = rendement_locatif × 0.45 + score_macro × 0.30 + attractivité × 0.25

Colonnes finales : 15 exactement
  Supprimées (= 0 partout) : ratio_amenities_commerce, ratio_amenities_sante,
                              nb_stations_transport, nb_commerce
  Ajoutée                  : ville_encoded (proxy sous-zone, même logique BO2/BO3)
"""

import warnings
warnings.filterwarnings('ignore')

# ================================================================
# COLONNES NULLES — supprimées (valeur unique = 0 dans tous les datasets)
# ================================================================

COLS_NULLES = [
    'ratio_amenities_commerce',
    'ratio_amenities_sante',
    'nb_stations_transport',
    'nb_commerce',
]

# ================================================================
# COLONNES FINALES — 15 exactement (identiques pour les 4 datasets)
# ================================================================
#
# Choix basé sur corrélation |r| avec TARGET :
#   |r|=0.78  densite_routes_km         ← très fort
#   |r|=0.77  nb_amenities_total        ← très fort
#   |r|=0.73  surface_landuse_resid.    ← fort
#   |r|=0.64  densite_population        ← fort
#   |r|=0.57  gouvernorat               ← géo
#   |r|=0.48  prix_m2                   ← marché
#   |r|=0.37  variation_prix_m2         ← marché
#   |r|=0.31  nb_buildings_residentiel  ← contexte
#   |r|=0.06  croissance_pib_trim       ← macro
#   |r|=0.04  inflation_glissement_ann. ← macro
#   |r|=0.01  note_google_moyenne       ← attractivité
#   |r|=0.007 high_season               ← saisonnier
#   ville_encoded                       ← demandé, même logique BO2/BO3
#   indice_rentabilite_regionale        ← TARGET ⭐

FINAL_COLS_BO5 = [
    # ── Géographie (1) ───────────────────────────────────────
    'gouvernorat',                    # encodé 1-24 — identifiant géo

    # ── Features marché (2) ──────────────────────────────────
    'prix_m2',                        # |r|=0.872 avec TARGET — feature principale
    'variation_prix_m2',              # |r|=0.490 — dynamique du marché

    # ── Features attractivité (4) ────────────────────────────
    'note_google_moyenne',            # |r|=0.291 — attractivité réelle scrappée
    'densite_population',             # |r|=0.079 — démographie
    'densite_routes_km',              # |r|=0.953 — meilleure feature OSM
    'nb_buildings_residentiel',       # |r|=0.692 — stock immobilier

    # ── TARGET ⭐ ─────────────────────────────────────────────
    'indice_rentabilite_regionale',   # score 0-100
]

# Colonnes supprimées et pourquoi :
# ville_encoded           → r=1.00 avec gouvernorat (doublon parfait)
# surface_landuse_resid.  → r=0.976 avec densite_routes_km (quasi-identique)
# nb_amenities_total      → r=0.885 avec densite_pop + |r|=0.009 avec TARGET
# inflation               → |r|=0.033 avec TARGET + r=0.84 avec PIB
# croissance_pib_trim     → |r|=0.023 avec TARGET (quasi nul)
# high_season             → |r|=0.009 avec TARGET (pas de signal)

# 14 colonnes finales (identiques 4 datasets)

# ================================================================
# ENCODAGE GOUVERNORATS (identique BO2/BO3)
# ================================================================

GOUVERNORAT_ENC = {
    'Tunis': 1, 'Ariana': 2, 'Ben Arous': 3, 'Manouba': 4,
    'Nabeul': 5, 'Zaghouan': 6, 'Bizerte': 7, 'Béja': 8,
    'Jendouba': 9, 'Le Kef': 10, 'Siliana': 11, 'Sousse': 12,
    'Monastir': 13, 'Mahdia': 14, 'Sfax': 15, 'Kairouan': 16,
    'Kasserine': 17, 'Sidi Bouzid': 18, 'Gabès': 19,
    'Medenine': 20, 'Tataouine': 21, 'Gafsa': 22,
    'Tozeur': 23, 'Kébili': 24,
}
GOUVERNORAT_DEC = {v: k for k, v in GOUVERNORAT_ENC.items()}

# ================================================================
# SEUILS DE NETTOYAGE
# ================================================================

SEUILS_PRIX_M2 = {
    'residentiel': (10,   8000),
    'foncier':     (5,    3000),
    'commercial':  (20,  15000),
    'divers':      (5,   10000),
}
SEUILS_VARIATION = (-50, 200)

# ================================================================
# FICHIERS DE SORTIE
# ================================================================

FICHIERS_BO5 = {
    'residentiel': ('residentiel_BO5.xlsx', '1565C0'),
    'foncier':     ('foncier_BO5.xlsx',     '2E7D32'),
    'commercial':  ('commercial_BO5.xlsx',  'E65100'),
    'divers':      ('divers_BO5.xlsx',      '6A1B9A'),
}

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


def print_encoding_summary():
    print("\n" + "=" * 65)
    print("   ENCODAGES — OBJECTIF 5 : RENTABILITÉ RÉGIONALE")
    print("=" * 65)
    print(f"  gouvernorats encodés       : {len(GOUVERNORAT_ENC)}")
    print(f"  colonnes nulles supprimées : {len(COLS_NULLES)}")
    print(f"    → {', '.join(COLS_NULLES)}")
    print(f"  colonnes finales           : {len(FINAL_COLS_BO5)} (7 features + 1 TARGET)")
    print(f"  Supprimées (redondantes)   : ville_encoded, surface_landuse, nb_amenities,")
    print(f"                               inflation, croissance_pib, high_season")
    print()
    print("  Colonnes retenues :")
    print("    Géo (1)       : gouvernorat")
    print("    Marché (2)    : prix_m2, variation_prix_m2")
    print("    Attract. (4)  : note_google, densite_pop, densite_routes, nb_buildings")
    print("    TARGET (1)    : indice_rentabilite_regionale ⭐")
    print("=" * 65)
