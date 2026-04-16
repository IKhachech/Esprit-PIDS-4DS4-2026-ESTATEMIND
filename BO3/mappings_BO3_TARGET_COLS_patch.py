"""
mappings_BO3_TARGET_COLS_patch.py
==================================
PATCH UNIQUE à appliquer dans mappings_BO3.py :
Remplacer l'ancien TARGET_COLS par celui-ci (16 → 22 colonnes).

Dans mappings_BO3.py, chercher "TARGET_COLS = [" et remplacer tout le bloc.
"""

TARGET_COLS = [
    # ── Géographie ──────────────────────────────────────────────
    'gouvernorat',              # code 1-24
    'ville_encoded',            # code gov*1000+rang
    # ── Temporel ────────────────────────────────────────────────
    'annee',
    'mois',
    'high_season',              # 1=mars-mai/sept-nov
    # ── Transaction ─────────────────────────────────────────────
    'type_transaction',         # 1=Location / 2=Vente
    # ── Zone géographique (③ NOUVEAU) ────────────────────────────
    'zone_geographique',        # 0=Grand Tunis 1=Littoral Nord 2=Sahel 3=Centre 4=Sud
    # ── Marché local ────────────────────────────────────────────
    'score_attractivite',       # 0-1
    'nb_infra',
    'nb_commerce',
    'indice_liquidite',         # ③ NOUVEAU : ln(n_ann/n_mois) — liquide vs illiquide
    'volatilite_prix_trim',     # ③ NOUVEAU : σ trimestriel normalisé
    'potentiel_emergent',       # ③ NOUVEAU : score zones émergentes 0-1
    # ── Macro-économique ─────────────────────────────────────────
    'inflation_glissement_annuel',
    'croissance_pib_trim',
    'glissement_immo_trim',
    # ── Corrections déséquilibre ─────────────────────────────────
    'sample_weight_temporal',
    'sample_weight_geo',
    'arima_eligible',
    # ── TARGET principal (lissé ①②④) ─────────────────────────────
    'indice_prix_m2_regional',  # mean(prix_m2) lissé × gov × annee × mois
    # ── Targets stratifiés (⑤ NOUVEAU) ───────────────────────────
    'indice_loc_lisse',         # indice Location lissé par gov × mois
    'indice_ven_lisse',         # indice Vente lissé par gov × mois
]

# ── Mise à jour print_encoding_summary ───────────────────────────────────
# Dans mappings_BO3.py, mettre à jour aussi la ligne :
#   print(f"  TARGET_COLS      : {len(TARGET_COLS)} colonnes")
# Elle affichera automatiquement 22 au lieu de 16.
