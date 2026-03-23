"""
external_data_BO5.py — Données OSM réelles depuis signaux_immobilier_tunisie.xlsx.

Résout 3 problèmes détectés dans les données BO5 :
  1. nb_amenities_total = 423 (fallback) → remplacé par vrais comptages signaux
  2. note_google_moyenne erronée (3.9-4.4) → remplacée par vraies moyennes scrappées
  3. prix_m2 aberrants (Béja=5.4, Sidi Bouzid=6.7) → corrigés par médiane régionale
"""

import os, warnings
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')


# ================================================================
# MAPPING code_gouv INS → gouvernorat_enc BO5 (1-24)
# ================================================================

CODE_INS_TO_BO5 = {
    11:1,  12:2,  13:3,  14:4,    # Grand Tunis
    21:5,  22:6,  23:7,            # Nord-Est
    31:8,  32:9,  33:10, 34:11,   # Nord-Ouest
    41:12, 42:13, 43:14,           # Centre-Est (Sousse, Monastir, Mahdia)
    51:15, 52:16, 53:17,           # Centre-Ouest
    61:18,                         # Sfax
    71:19, 72:20, 73:21, 74:22,   # Sud-Est (Gabès, Gafsa, Tozeur, Kébili)
    82:23, 83:24,                  # Sud (Médenine, Tataouine)
}

# ================================================================
# VALEURS OSM RÉELLES extraites de signaux_immobilier_tunisie.xlsx
# Comptage réel de lieux Google Maps scrappés par gouvernorat
# ================================================================

NB_AMENITIES_REELS = {
    1:361, 2:366, 3:312, 4:318, 5:318, 6:225, 7:286,
    8:200, 9:185, 10:185, 11:180, 12:362, 13:264, 14:267,
    15:209, 16:226, 17:304, 18:339, 19:279, 20:225,
    21:199, 22:165, 23:263, 24:229,
}

# Notes Google calculées depuis les vraies notes scrappées
NOTE_GOOGLE_REELS = {
    1:3.60, 2:3.29, 3:3.12, 4:3.10, 5:3.28, 6:3.09, 7:3.20,
    8:2.52, 9:2.91, 10:2.68, 11:2.85, 12:3.52, 13:3.28, 14:3.20,
    15:2.54, 16:2.50, 17:3.06, 18:3.15, 19:2.93, 20:3.19,
    21:3.29, 22:2.84, 23:2.74, 24:3.03,
}

# Population réelle INS par gouvernorat (habitants)
POPULATION_REELS = {
    1:1056247, 2:587200,  3:703022,  4:391136,
    5:787920,  6:185888,  7:568219,  8:308553,
    9:422717,  10:258591, 11:237217, 12:674535,
    13:560605, 14:414412, 15:436130, 16:438628,
    17:570559, 18:955421, 19:374300, 20:383914,
    21:108852, 22:165509, 23:490931, 24:157447,
}

# densite_population — encodée 1-4 (même que BO3)
# 1=très faible (<200k), 2=faible (200-400k), 3=moyenne (400-700k), 4=forte (>700k)
DENSITE_POP_REELS = {}
for gov, pop in POPULATION_REELS.items():
    if pop < 200000:   DENSITE_POP_REELS[gov] = 1
    elif pop < 400000: DENSITE_POP_REELS[gov] = 2
    elif pop < 700000: DENSITE_POP_REELS[gov] = 3
    else:              DENSITE_POP_REELS[gov] = 4

# ================================================================
# PRIX_M2 MÉDIANS RÉGIONAUX — pour corriger les aberrants
# (gouvernorats avec n < 10 annonces = prix non fiable)
# ================================================================

# Source : médiane calculée depuis les annonces existantes par région
PRIX_M2_MEDIAN_REGIONAL = {
    # Grand Tunis : médiane 559 TND/m²
    1:559, 2:559, 3:559, 4:559,
    # Nord-Est : médiane 200 TND/m²
    5:200, 6:200, 7:200,
    # Nord-Ouest : médiane calculée = 280 TND/m² (Béja/Jendouba/Kef/Siliana)
    # (les 5.4 et 6.7 sont des erreurs de scrapping — terrain foncier mélangé)
    8:280, 9:280, 10:280, 11:280,
    # Centre-Est : médiane 469 TND/m²
    12:469, 13:469, 14:469,
    # Centre : médiane 600 TND/m²
    15:600, 16:600, 17:600,
    # Sud : médiane 250 TND/m²
    18:600, 19:250, 20:250, 21:250, 22:250, 23:250, 24:250,
}

# Seuil n_annonces minimum — si < ce seuil → corriger prix_m2
MIN_N_FIABLE = 10

# ================================================================
# FONCTION PRINCIPALE
# ================================================================

def load_signaux(path: str) -> dict:
    """
    Charge signaux_immobilier_tunisie.xlsx et retourne
    les vraies valeurs OSM par gouvernorat.
    """
    if not os.path.exists(path):
        print(f"  [WARN] signaux absent : {path} — valeurs hardcodées utilisées")
        return _get_hardcoded()

    df = pd.read_excel(path)
    df['gov_bo5'] = df['code_gouv'].map(CODE_INS_TO_BO5)
    df = df.dropna(subset=['gov_bo5'])
    df['gov_bo5'] = df['gov_bo5'].astype(int)

    # nb_amenities = comptage réel lieux scrappés
    nb_am = df.groupby('gov_bo5').size().to_dict()

    # note_google = moyenne réelle des notes scrappées
    df['_note'] = pd.to_numeric(df['note_google'], errors='coerce')
    note = df.groupby('gov_bo5')['_note'].mean().round(2).to_dict()

    # population réelle
    pop = df.groupby('gov_bo5')['population'].first().to_dict()

    # densite_population encodée 1-4
    densite = {}
    for gov, p in pop.items():
        p = float(p) if pd.notna(p) else 0
        densite[gov] = (1 if p < 200000 else 2 if p < 400000
                        else 3 if p < 700000 else 4)

    print(f"  ✔ signaux chargé : {len(df)} lieux | {len(nb_am)} gouvernorats")
    return {
        'nb_amenities':     nb_am,
        'note_google':      note,
        'population':       pop,
        'densite_pop':      densite,
    }


def _get_hardcoded() -> dict:
    """Retourne les valeurs extraites et hardcodées depuis le fichier signaux."""
    return {
        'nb_amenities': NB_AMENITIES_REELS,
        'note_google':  NOTE_GOOGLE_REELS,
        'population':   POPULATION_REELS,
        'densite_pop':  DENSITE_POP_REELS,
    }


def enrich_with_real_osm(groupes: dict, signaux_path: str) -> dict:
    """
    Remplace les valeurs fallback OSM par les vraies valeurs des signaux.
    Corrige aussi les prix_m2 aberrants (n < MIN_N_FIABLE annonces).
    """
    print("\n" + "=" * 65)
    print("   ETAPE EXTERNE — ENRICHISSEMENT OSM RÉEL (signaux)")
    print("=" * 65)

    osm = load_signaux(signaux_path)

    for nom, df in groupes.items():
        n_avant = len(df)

        # ── Correction 1 : nb_amenities_total ──────────────────
        if 'nb_amenities_total' in df.columns:
            nb_old_fallback = (df['nb_amenities_total'] == 423).sum()
            df['nb_amenities_total'] = df['gouvernorat'].map(
                osm['nb_amenities']
            ).fillna(df['nb_amenities_total'])
            print(f"  {nom:<15} nb_amenities : {nb_old_fallback} fallback → valeurs réelles signaux")

        # ── Correction 2 : note_google_moyenne ─────────────────
        if 'note_google_moyenne' in df.columns:
            df['note_google_moyenne'] = df['gouvernorat'].map(
                osm['note_google']
            ).fillna(df['note_google_moyenne']).round(2)
            print(f"  {nom:<15} note_google  : recalculée depuis vraies notes scrappées")

        # ── Correction 3 : densite_population ──────────────────
        if 'densite_population' in df.columns:
            df['densite_population'] = df['gouvernorat'].map(
                osm['densite_pop']
            ).fillna(df['densite_population']).astype(int)

        # ── Correction 4 : prix_m2 aberrants ───────────────────
        # Compter n annonces par gouvernorat dans ce groupe
        n_par_gov = df.groupby('gouvernorat').size()
        govs_faibles = n_par_gov[n_par_gov < MIN_N_FIABLE].index.tolist()
        if govs_faibles:
            masque = df['gouvernorat'].isin(govs_faibles)
            n_corriges = masque.sum()
            df.loc[masque, 'prix_m2'] = df.loc[masque, 'gouvernorat'].map(
                PRIX_M2_MEDIAN_REGIONAL
            ).fillna(df.loc[masque, 'prix_m2'])
            print(f"  {nom:<15} prix_m2      : {n_corriges} lignes corrigées "
                  f"(gouvernorats {govs_faibles} avaient n < {MIN_N_FIABLE})")
        else:
            print(f"  {nom:<15} prix_m2      : aucune correction nécessaire")

        groupes[nom] = df

    print()
    print("  Valeurs OSM finales par gouvernorat :")
    print(f"  {'Gov':>4} {'nb_amenities':>13} {'note_google':>12} {'densite_pop':>12}")
    for gov in sorted(osm['nb_amenities'].keys()):
        print(f"  {gov:>4} {osm['nb_amenities'].get(gov,0):>13} "
              f"{osm['note_google'].get(gov,0):>12.2f} "
              f"{osm['densite_pop'].get(gov,0):>12}")

    return groupes
