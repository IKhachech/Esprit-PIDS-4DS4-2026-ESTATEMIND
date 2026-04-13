"""
features.py — Construction vecteur de features pour l'inférence
Compatible avec dataset_residentiel.csv et dataset_terrain.csv
"""
import math
import pandas as pd
import numpy as np

HAS_FEATURES = [
    'has_ascenseur','has_meuble','has_terrasse','has_piscine',
    'has_vue_mer','has_garage','has_securite','has_climatisation',
    'has_chauffage','has_cuisine_equip','has_double_vitrage',
    'has_porte_blindee','has_concierge','has_jardin','has_standing',
    'has_vue_montagne','has_cheminee',
]

HAS_WEIGHTS = {
    'has_piscine':3,'has_vue_mer':3,'has_jardin':2,'has_garage':2,
    'has_standing':2,'has_concierge':2,'has_cheminee':2,
    'has_ascenseur':1,'has_meuble':1,'has_terrasse':1,'has_securite':1,
    'has_climatisation':1,'has_cuisine_equip':1,'has_chauffage':1,
    'has_double_vitrage':1,'has_porte_blindee':1,'has_vue_montagne':1,
}

CYCLE_MULT = {0:1.0, 1:0.90, 2:1.05, 3:1.10, 4:0.95, 5:0.85}


def build_feature_vector(inp) -> pd.DataFrame:
    """
    Construit un DataFrame 1 ligne avec toutes les features
    depuis un ValuationInput.
    """
    # ── Localisation ──────────────────────────────────────────────
    lat      = float(inp.lat or 36.8)
    lon      = float(inp.lon or 10.2)
    micro_lat  = round(lat, 2)
    micro_lon  = round(lon, 2)
    micro_zone = round(micro_lat * 1000 + micro_lon, 1)
    ville_enc  = float(inp.ville_enc or 0.5)

    # ── Bien ─────────────────────────────────────────────────────
    surf       = float(inp.surface_m2 or 100.0)
    surf_log   = math.log1p(surf)
    nb_pieces  = float(inp.nb_pieces or 3.0)
    nb_chambres= float(inp.nb_chambres or max(nb_pieces - 1, 1))

    # ── Marché ───────────────────────────────────────────────────
    score_attr = float(inp.score_attractivite or 0.5)
    market_ten = float(inp.market_tension or 0.5)
    cycle      = int(inp.cycle_marche or 2)
    cf         = CYCLE_MULT.get(cycle, 1.0)

    # ── Type bien / transaction ───────────────────────────────────
    type_bien_f = int(inp.type_bien or 1)
    type_tx_f   = int(inp.type_transaction or 2)

    # ── Score équipements composite ───────────────────────────────
    total_w  = sum(HAS_WEIGHTS.values())
    score_eq = sum(
        getattr(inp, c, 0) * HAS_WEIGHTS.get(c, 1)
        for c in HAS_FEATURES
    ) / max(total_w, 1)

    row = {
        # Localisation
        'gouvernorat_enc':      int(inp.gouvernorat),
        'ville_enc':            ville_enc,
        'lat':                  lat,
        'lon':                  lon,
        'micro_zone':           micro_zone,
        'micro_lat':            micro_lat,
        'micro_lon':            micro_lon,
        # Bien
        'surface_m2':           surf,
        'surface_log':          surf_log,
        'nb_pieces':            nb_pieces,
        'nb_pieces_f':          nb_pieces,
        'nb_chambres':          nb_chambres,
        'nb_chambres_f':        nb_chambres,
        'type_bien_f':          type_bien_f,
        'type_transaction_f':   type_tx_f,
        # Marché
        'score_attractivite':   score_attr,
        'market_tension':       market_ten,
        'market_score':         score_attr * market_ten,
        'cycle_marche':         cycle,
        'cycle_factor':         cf,
        'prix_region_median':   float(inp.prix_region_median or 200_000),
        'text_embedding_score': float(inp.text_embedding_score or 0.5),
        'nb_images':            int(inp.nb_images or 1),
        # Équipements
        'score_equipements':    score_eq,
        # Interactions de base (enrichies dans agent.py avec pm2 stats)
        'prix_m2_ville':        0.0,
        'prix_m2_gov':          0.0,
        'prix_m2_micro':        0.0,
        'pm2_x_cycle':          0.0,
        'surf_x_ville':         0.0,
        'surf_x_micro':         0.0,
        'surf_x_attract':       surf_log * score_attr,
        'ville_x_surf':         ville_enc * surf_log,
        'pieces_surf':          surf_log * nb_pieces,
        'pm2_x_pieces':         0.0,
        'type_x_surface':       type_bien_f * surf_log,
        'type_x_pm2ville':      0.0,
        'tx_x_pm2':             0.0,
    }

    # Features has_* individuelles
    for c in HAS_FEATURES:
        row[c] = int(getattr(inp, c, 0))

    return pd.DataFrame([row])


def build_description_bien(inp) -> str:
    """
    Construit une description lisible du bien.
    Ex: "Appartement S3 (2ch) | Piscine · Garage · Jardin"
    """
    TYPE_LABELS = {1:'Appartement', 2:'Maison', 3:'Villa',
                   4:'Chambre', 5:'Ferme', 6:'Local', 7:'Maison', 8:'Terrain', 9:'Villa'}
    TX_LABELS   = {1:'Location', 2:'Vente'}

    type_label = TYPE_LABELS.get(int(inp.type_bien), 'Bien')
    tx_label   = TX_LABELS.get(int(inp.type_transaction), 'Vente')

    # Pièces
    nb_p = int(inp.nb_pieces or 0)
    nb_c = int(inp.nb_chambres or 0)
    pieces_str = ""
    if nb_p > 0:
        pieces_str = f" S{nb_p}"
        if nb_c > 0:
            pieces_str += f" ({nb_c}ch)"

    # Équipements présents (du plus important au moins important)
    equip_priority = [
        ('has_piscine',      'Piscine'),
        ('has_vue_mer',      'Vue mer'),
        ('has_jardin',       'Jardin'),
        ('has_garage',       'Garage'),
        ('has_standing',     'Standing'),
        ('has_cheminee',     'Cheminée'),
        ('has_concierge',    'Concierge'),
        ('has_ascenseur',    'Ascenseur'),
        ('has_meuble',       'Meublé'),
        ('has_terrasse',     'Terrasse'),
        ('has_climatisation','Climatisation'),
        ('has_securite',     'Sécurité'),
        ('has_cuisine_equip','Cuisine équip.'),
    ]
    equip_list = [label for feat, label in equip_priority if getattr(inp, feat, 0) == 1]

    surf_str  = f"{int(inp.surface_m2)}m²"
    desc      = f"{type_label}{pieces_str} {surf_str} | {tx_label}"
    if equip_list:
        desc += " | " + " · ".join(equip_list[:5])  # max 5 équipements affichés

    return desc
