"""
prepare.py v4 — Préparation et division des données immobilières tunisiennes
EstateMind BO2 — Vision Pipeline

Sources :
    - mubawab_annonces.csv
    - bnb_properties.csv

Outputs (6 fichiers) :
    dataset_appart_vente.csv      → Appartements à vendre
    dataset_appart_location.csv   → Appartements à louer
    dataset_maison_vente.csv      → Maisons à vendre
    dataset_maison_location.csv   → Maisons à louer
    dataset_villa.csv             → Villas (vente + location)
    dataset_terrain.csv           → Terrains (vente uniquement)

Type Autre/Bureau/Ferme → supprimés
"""

import os, re, json, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

MUBAWAB_FILE = 'mubawab_annonces.csv'
BNB_FILE     = 'bnb_properties.csv'

# Encodage gouvernorat (ordre officiel tunisien)
GOV_ENC = {
    'Ariana': 1, 'Béja': 2, 'Ben Arous': 3, 'Bizerte': 4,
    'Gabès': 5, 'Gafsa': 6, 'Jendouba': 7, 'Kairouan': 8,
    'Kasserine': 9, 'Kébili': 10, 'Le Kef': 11, 'Mahdia': 12,
    'Manouba': 13, 'Médenine': 14, 'Monastir': 15, 'Nabeul': 16,
    'Sfax': 17, 'Sidi Bouzid': 18, 'Siliana': 19, 'Sousse': 20,
    'Tataouine': 21, 'Tozeur': 22, 'Tunis': 23, 'Zaghouan': 24,
}

# Seuils prix métier par segment
PRICE_LIMITS = {
    ('vente',    'appartement'): (15_000,  20_000_000),
    ('location', 'appartement'): (150,     25_000),
    ('vente',    'maison'):      (30_000,  20_000_000),
    ('location', 'maison'):      (200,     50_000),
    ('vente',    'villa'):       (50_000,  40_000_000),
    ('location', 'villa'):       (500,     100_000),
    ('vente',    'terrain'):     (5_000,   50_000_000),
    ('location', 'terrain'):     (200,     500_000),
}

# Seuils surface par type_bien
SURF_MAX = {
    'appartement': 2000,
    'maison':      5000,
    'villa':       5000,
    'terrain':     500_000,
}

# Mapping ville → gouvernorat (lowercase)
VILLE_GOV = {
    # Tunis
    'tunis': 'Tunis', 'carthage': 'Tunis', 'la marsa': 'Tunis', 'marsa': 'Tunis',
    'gammarth': 'Tunis', 'sidi bou said': 'Tunis', 'sidi bou saïd': 'Tunis',
    'sidi bousaid': 'Tunis', 'el aouina': 'Tunis', 'aouina': 'Tunis',
    'l aouina': 'Tunis', 'laouina': 'Tunis',
    'el menzah': 'Tunis', 'menzah': 'Tunis',
    'el menzah 1': 'Tunis', 'el menzah 2': 'Tunis', 'el menzah 3': 'Tunis',
    'el menzah 4': 'Tunis', 'el menzah 5': 'Tunis', 'el menzah 6': 'Tunis',
    'el menzah 7': 'Tunis', 'el menzah 8': 'Tunis', 'el menzah 9': 'Tunis',
    'el menzah 10': 'Tunis', 'el menzah 11': 'Tunis',
    'cite el khadra': 'Tunis', 'cité el khadra': 'Tunis', 'el khadra': 'Tunis',
    'le bardo': 'Tunis', 'bardo': 'Tunis',
    'ennasr': 'Tunis', 'ennasr 1': 'Tunis', 'ennasr 2': 'Tunis', 'nasr': 'Tunis',
    'ennaser': 'Tunis',
    'ain zaghouan': 'Tunis', 'aïn zaghouan': 'Tunis', 'ain zaghouan nord': 'Tunis',
    'les berges du lac': 'Tunis', 'berges du lac': 'Tunis',
    'lac': 'Tunis', 'lac 1': 'Tunis', 'lac 2': 'Tunis',
    'chotrana': 'Tunis', 'chotrana 1': 'Tunis', 'chotrana 2': 'Tunis',
    'mutuelleville': 'Tunis', 'mutuelle ville': 'Tunis',
    'el manar': 'Tunis', 'el manar 1': 'Tunis', 'el manar 2': 'Tunis',
    'montplaisir': 'Tunis', 'monfleury': 'Tunis', 'montfleury': 'Tunis',
    'el omrane': 'Tunis', 'el omrane superieur': 'Tunis',
    'el ouardia': 'Tunis', 'sijoumi': 'Tunis',
    'medina': 'Tunis', 'la medina': 'Tunis', 'medina jedida': 'Tunis',
    'el gorjani': 'Tunis', 'el kabaria': 'Tunis', 'sidi hassine': 'Tunis',
    'el hrairia': 'Tunis', 'ettahrir': 'Tunis', 'ettadhamen': 'Tunis',
    'le kram': 'Tunis', 'la goulette': 'Tunis',
    'sidi daoud': 'Tunis', 'bhar lazreg': 'Tunis',
    'charguia': 'Tunis', 'charguia 1': 'Tunis', 'charguia 2': 'Tunis',
    'el mourouj': 'Tunis', 'cite olympique': 'Tunis',
    'amilcar': 'Tunis', 'jardins de carthage': 'Tunis',
    'centre urbain nord': 'Tunis', 'lafayette': 'Tunis',
    'hammam chatt': 'Tunis', 'barraket essahel': 'Tunis',
    'riadh andalous': 'Tunis', 'ksar said': 'Tunis',
    # Ariana
    'ariana': 'Ariana', 'ariana ville': 'Ariana',
    'raoued': 'Ariana', 'la soukra': 'Ariana', 'soukra': 'Ariana',
    'la nouvelle soukra': 'Ariana', 'mnihla': 'Ariana',
    'borj el amri': 'Ariana', 'borj louzir': 'Ariana',
    'kalaat landalous': 'Ariana', 'sidi thabet': 'Ariana',
    'ettadhamen ariana': 'Ariana', 'ghazala': 'Ariana',
    # Ben Arous
    'ben arous': 'Ben Arous', 'ezzahra': 'Ben Arous',
    'hammam lif': 'Ben Arous', 'hammam chott': 'Ben Arous',
    'borj cedria': 'Ben Arous', 'megrine': 'Ben Arous', 'mégrine': 'Ben Arous',
    'rades': 'Ben Arous', 'radès': 'Ben Arous',
    'fouchana': 'Ben Arous', 'mornag': 'Ben Arous',
    'boumhel': 'Ben Arous', 'boumhal': 'Ben Arous',
    'borj touil': 'Ben Arous', 'nouvelle medina': 'Ben Arous',
    # Manouba
    'manouba': 'Manouba', 'la manouba': 'Manouba',
    'tebourba': 'Manouba', 'jedaida': 'Manouba',
    'douar hicher': 'Manouba', 'tantana': 'Manouba',
    # Nabeul
    'nabeul': 'Nabeul', 'hammamet': 'Nabeul', 'hammamt': 'Nabeul',
    'kelibia': 'Nabeul', 'kélibia': 'Nabeul',
    'korba': 'Nabeul', 'grombalia': 'Nabeul', 'bou argoub': 'Nabeul',
    'menzel temime': 'Nabeul', 'soliman': 'Nabeul',
    'el haouaria': 'Nabeul', 'haouaria': 'Nabeul',
    'takelsa': 'Nabeul', 'beni khalled': 'Nabeul',
    'hammam ghezaz': 'Nabeul', 'hammam el ghezaz': 'Nabeul',
    'dar chaabane': 'Nabeul', 'yasmine hammamet': 'Nabeul',
    'mrezga': 'Nabeul', 'bir bouregba': 'Nabeul',
    'menzel bouzelfa': 'Nabeul', 'beni khiar': 'Nabeul',
    'el mida': 'Nabeul', 'tazarka': 'Nabeul',
    'el maamoura': 'Nabeul',
    # Zaghouan
    'zaghouan': 'Zaghouan', 'zaqhouen': 'Zaghouan',
    'bou arada': 'Zaghouan', 'el fahs': 'Zaghouan',
    'zriba': 'Zaghouan', 'hammam zriba': 'Zaghouan',
    # Bizerte
    'bizerte': 'Bizerte', 'mateur': 'Bizerte',
    'menzel bourguiba': 'Bizerte', 'menzel jemil': 'Bizerte',
    'el alia': 'Bizerte', 'ras jebel': 'Bizerte',
    'ghar el melh': 'Bizerte', 'zarzouna': 'Bizerte',
    'sejnane': 'Bizerte', 'utique': 'Bizerte',
    'raf raf': 'Bizerte', 'rafraf': 'Bizerte', 'metline': 'Bizerte',
    # Béja
    'beja': 'Béja', 'béja': 'Béja',
    'medjez el bab': 'Béja', 'testour': 'Béja', 'nefza': 'Béja',
    # Jendouba
    'jendouba': 'Jendouba', 'tabarka': 'Jendouba',
    'ain draham': 'Jendouba', 'bou salem': 'Jendouba',
    # Le Kef
    'le kef': 'Le Kef', 'el kef': 'Le Kef', 'kef': 'Le Kef',
    'sers': 'Le Kef', 'tajerouine': 'Le Kef',
    # Siliana
    'siliana': 'Siliana', 'makthar': 'Siliana', 'bou arada siliana': 'Siliana',
    # Kairouan
    'kairouan': 'Kairouan', 'kairouan ville': 'Kairouan',
    'al-qayrawan': 'Kairouan', 'haffouz': 'Kairouan',
    'sbikha': 'Kairouan', 'oueslatia': 'Kairouan',
    # Kasserine
    'kasserine': 'Kasserine', 'sbeitla': 'Kasserine',
    'feriana': 'Kasserine', 'fériana': 'Kasserine', 'sbiba': 'Kasserine',
    # Sidi Bouzid
    'sidi bouzid': 'Sidi Bouzid', 'meknassy': 'Sidi Bouzid',
    'mezzouna': 'Sidi Bouzid',
    # Sousse
    'sousse': 'Sousse', 'akouda': 'Sousse',
    'hammam sousse': 'Sousse', 'sahloul': 'Sousse',
    'sousse ville': 'Sousse', 'sousse corniche': 'Sousse',
    'hergla': 'Sousse', 'kantaoui': 'Sousse', 'port el kantaoui': 'Sousse',
    'kalaa kebira': 'Sousse', 'kalâa kebira': 'Sousse',
    'kalaa sghira': 'Sousse', 'kalâa sghira': 'Sousse',
    'bouficha': 'Sousse', "m'saken": 'Sousse', 'msaken': 'Sousse',
    'sahline': 'Sousse', 'chott meriam': 'Sousse',
    'khezama est': 'Sousse', 'khezama ouest': 'Sousse', 'khzema': 'Sousse',
    'enfidha': 'Sousse',
    # Monastir
    'monastir': 'Monastir', 'monastir ville': 'Monastir',
    'moknine': 'Monastir', 'bekalta': 'Monastir', 'jemmal': 'Monastir',
    'zeramdine': 'Monastir', 'teboulba': 'Monastir',
    'sayada': 'Monastir', 'sahline monastir': 'Monastir',
    'messadine': 'Monastir', 'tezdaine': 'Monastir',
    # Mahdia
    'mahdia': 'Mahdia', 'el jem': 'Mahdia',
    'ksour essef': 'Mahdia', 'chebba': 'Mahdia',
    'salakta': 'Mahdia', 'rejiche': 'Mahdia',
    # Sfax
    'sfax': 'Sfax', 'sakiet ezzit': 'Sfax', 'sakiet eddaier': 'Sfax',
    'thyna': 'Sfax', 'agareb': 'Sfax', 'jebeniana': 'Sfax',
    'mahres': 'Sfax', 'mahrès': 'Sfax', 'menzel chaker': 'Sfax',
    'kerkennah': 'Sfax', 'gremda': 'Sfax',
    # Gafsa
    'gafsa': 'Gafsa', 'metlaoui': 'Gafsa', 'redeyef': 'Gafsa',
    # Tozeur
    'tozeur': 'Tozeur', 'nefta': 'Tozeur', 'degache': 'Tozeur',
    # Kébili
    'kebili': 'Kébili', 'kébili': 'Kébili', 'douz': 'Kébili',
    # Gabès
    'gabes': 'Gabès', 'gabès': 'Gabès',
    'matmata': 'Gabès', 'mareth': 'Gabès', 'el hamma': 'Gabès',
    # Médenine
    'medenine': 'Médenine', 'médenine': 'Médenine',
    'zarzis': 'Médenine', 'djerba': 'Médenine', 'jerba': 'Médenine',
    'houmt souk': 'Médenine', 'midoun': 'Médenine',
    'ajim': 'Médenine', 'ben gardane': 'Médenine',
    # Tataouine
    'tataouine': 'Tataouine', 'remada': 'Tataouine',
    'ghomrassen': 'Tataouine',
}

# Équipements Mubawab
EQUIP_MAP = {
    'has_terrasse':       'Terrasse',
    'has_garage':         'Garage',
    'has_climatisation':  'Climatisation',
    'has_chauffage':      'Chauffage central',
    'has_cuisine_equip':  'Cuisine équipée',
    'has_ascenseur':      'Ascenseur',
    'has_concierge':      'Concierge',
    'has_securite':       'Sécurité',
    'has_jardin':         'Jardin',
    'has_meuble':         'Meublé',
    'has_porte_blindee':  'Porte blindée',
    'has_double_vitrage': 'Double vitrage',
    'has_piscine':        'Piscine',
    'has_vue_mer':        'Vue sur mer',
    'has_vue_montagne':   'Vue sur les montagnes',
    'has_cheminee':       'Cheminée',
}

# Mots-clés BnB
DESC_KEYWORDS = {
    'has_meuble':        ['meublé', 'meuble', 'furnished'],
    'has_piscine':       ['piscine'],
    'has_jardin':        ['jardin'],
    'has_terrasse':      ['terrasse', 'balcon'],
    'has_securite':      ['résidence gardée', 'residence gardee', 'gardien',
                          'sécurisée', 'securisee', 'concierge'],
    'has_standing':      ['standing', 'luxe', 'prestige', 'haut gamme'],
    'has_neuf':          ['neuf', 'jamais habité', 'livraison', 'cle en main'],
    'has_vue_mer':       ['vue sur mer', 'vue mer', 'bord de mer'],
    'has_ascenseur':     ['ascenseur'],
    'has_climatisation': ['climatisation'],
    'has_garage':        ['garage'],
    'has_cuisine_equip': ['cuisine équipée', 'cuisine equipee', 'cuisine aménagée'],
}

ALL_EQUIP = list(set(list(EQUIP_MAP.keys()) + list(DESC_KEYWORDS.keys())))

# Features par segment
FEATURES_APPART = [
    'gouvernorat_enc', 'ville_enc', 'surface_m2', 'nb_pieces', 'nb_chambres',
    'nb_images', 'source_enc',
    'has_ascenseur', 'has_meuble', 'has_terrasse', 'has_piscine',
    'has_vue_mer', 'has_garage', 'has_securite', 'has_climatisation',
    'has_chauffage', 'has_cuisine_equip', 'has_double_vitrage',
    'has_porte_blindee', 'has_concierge', 'has_jardin', 'has_standing',
    'has_vue_montagne',
]

FEATURES_MAISON = [
    'gouvernorat_enc', 'ville_enc', 'surface_m2', 'nb_pieces', 'nb_chambres',
    'nb_images', 'source_enc',
    'has_jardin', 'has_garage', 'has_piscine', 'has_terrasse',
    'has_vue_mer', 'has_securite', 'has_meuble', 'has_climatisation',
    'has_chauffage', 'has_cuisine_equip', 'has_double_vitrage',
    'has_cheminee', 'has_standing', 'has_vue_montagne',
]

FEATURES_VILLA = [
    'gouvernorat_enc', 'ville_enc', 'surface_m2', 'nb_pieces', 'nb_chambres',
    'nb_images', 'source_enc',
    'type_transaction',  # villa contient vente + location
    'has_piscine', 'has_jardin', 'has_vue_mer', 'has_garage',
    'has_meuble', 'has_terrasse', 'has_standing', 'has_cuisine_equip',
    'has_climatisation', 'has_cheminee', 'has_vue_montagne',
]

FEATURES_TERRAIN = [
    'gouvernorat_enc', 'ville_enc', 'surface_m2', 'nb_images', 'source_enc',
    'type_transaction',  # garder au cas où quelques locations
    'has_vue_mer', 'has_vue_montagne',
]

# Config des 6 fichiers de sortie
SEGMENTS = {
    'appart_vente': {
        'file':        'dataset_appart_vente.csv',
        'type_bien':   'Appartement',
        'transaction': 0,
        'features':    FEATURES_APPART,
    },
    'appart_location': {
        'file':        'dataset_appart_location.csv',
        'type_bien':   'Appartement',
        'transaction': 1,
        'features':    FEATURES_APPART,
    },
    'maison_vente': {
        'file':        'dataset_maison_vente.csv',
        'type_bien':   'Maison',
        'transaction': 0,
        'features':    FEATURES_MAISON,
    },
    'maison_location': {
        'file':        'dataset_maison_location.csv',
        'type_bien':   'Maison',
        'transaction': 1,
        'features':    FEATURES_MAISON,
    },
    'villa': {
        'file':        'dataset_villa.csv',
        'type_bien':   'Villa',
        'transaction': None,  # vente + location
        'features':    FEATURES_VILLA,
    },
    'terrain': {
        'file':        'dataset_terrain.csv',
        'type_bien':   'Terrain',
        'transaction': None,  # vente uniquement (8 location seulement)
        'features':    FEATURES_TERRAIN,
    },
}

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')


# ================================================================
# HELPERS
# ================================================================

def parse_prix(val):
    if pd.isna(val): return np.nan
    s = re.sub(r'[^\d.]', '', str(val).replace(',', '.').replace(' ', ''))
    try:    return float(s)
    except: return np.nan


def get_gouvernorat(ville, gov_raw=None):
    if pd.notna(gov_raw):
        g = str(gov_raw).strip()
        if g and g not in ['', 'nan', 'Tunisie', 'Inconnu', 'Unknown', 'Tunisia']:
            if g in GOV_ENC: return g
    if pd.notna(ville):
        v = str(ville).strip().lower()
        if v in VILLE_GOV: return VILLE_GOV[v]
        for key, gov in VILLE_GOV.items():
            if key and len(key) > 3 and (key in v or v in key):
                return gov
    return None


def normalize_type_bien(val, titre=None):
    if pd.isna(val): return None
    v = str(val).strip()
    if titre and 'villa' in str(titre).lower(): return 'Villa'
    if 'Villa'       in v: return 'Villa'
    if any(x in v for x in ['Appartement', 'Studio', 'Duplex']): return 'Appartement'
    if 'Maison'      in v: return 'Maison'
    if 'Terrain'     in v: return 'Terrain'
    # Ferme, Bureau, Autre → None (supprimés)
    return None


def extract_equip_mubawab(equip_str):
    result = {col: 0 for col in EQUIP_MAP}
    if pd.isna(equip_str): return result
    parts = [p.strip() for p in str(equip_str).split('|')]
    for col, name in EQUIP_MAP.items():
        if name in parts: result[col] = 1
    return result


def extract_equip_text(text):
    result = {col: 0 for col in DESC_KEYWORDS}
    if pd.isna(text): return result
    t = str(text).lower()
    for col, kws in DESC_KEYWORDS.items():
        if any(kw in t for kw in kws): result[col] = 1
    return result


def apply_price_filter(df, type_bien_key):
    masks = []
    for tt, label in [(0, 'vente'), (1, 'location')]:
        key = (label, type_bien_key)
        if key not in PRICE_LIMITS: continue
        pmin, pmax = PRICE_LIMITS[key]
        seg_mask = df['type_transaction'] == tt
        ok_mask  = (df['prix'] >= pmin) & (df['prix'] <= pmax)
        n_drop   = (seg_mask & ~ok_mask).sum()
        if n_drop > 0:
            log(f'  Prix hors seuil [{label}] : {n_drop} lignes supprimées '
                f'({pmin:,} → {pmax:,} TND)')
        masks.append(~seg_mask | ok_mask)
    if masks:
        final_mask = masks[0]
        for m in masks[1:]: final_mask = final_mask & m
        return df[final_mask].copy()
    return df


# ================================================================
# CHARGEMENT SOURCES
# ================================================================

def load_mubawab():
    section('MUBAWAB')
    df = pd.read_csv(MUBAWAB_FILE)
    log(f'Chargé : {len(df)} lignes')

    df = df[df['categorie'] != 'vacances'].copy()
    df['type_transaction'] = df['categorie'].map({'vente': 0, 'location': 1})
    df = df.dropna(subset=['type_transaction'])

    df['prix'] = pd.to_numeric(df['prix_tnd'], errors='coerce')
    df = df.dropna(subset=['prix'])

    df['type_bien'] = df['type_bien'].apply(normalize_type_bien)
    df = df.dropna(subset=['type_bien'])  # supprime Autre, Bureau, Ferme

    df['gouvernorat'] = df.apply(
        lambda r: get_gouvernorat(r['ville'], r['gouvernorat']), axis=1)
    df = df.dropna(subset=['gouvernorat'])
    log(f'  Après filtre type_bien + gouvernorat : {len(df)}')

    df['surface_m2']  = pd.to_numeric(df['surface_m2'], errors='coerce')
    df['nb_pieces']   = pd.to_numeric(df['nb_pieces'],  errors='coerce')
    df['nb_chambres'] = pd.to_numeric(df['nb_chambres'],errors='coerce')
    df.loc[df['nb_pieces']   > 20, 'nb_pieces']   = np.nan
    df.loc[df['nb_chambres'] > 20, 'nb_chambres'] = np.nan
    df['nb_images']  = pd.to_numeric(df['nb_images'], errors='coerce').fillna(1)
    df['image_url']  = df['image_url'].astype(str)
    df['source_enc'] = 0  # mubawab
    df['ville']      = df['ville'].fillna('Inconnu')

    equip_df = pd.DataFrame(
        df['equipements'].apply(extract_equip_mubawab).tolist(), index=df.index)
    for col in equip_df.columns:
        df[col] = equip_df[col]
    for col in ALL_EQUIP:
        if col not in df.columns: df[col] = 0

    log(f'  Lignes finales : {len(df)}')
    return df


def load_bnb():
    section('BNB')
    df = pd.read_csv(BNB_FILE)
    log(f'Chargé : {len(df)} lignes')

    def get_tt(loc):
        if pd.isna(loc): return np.nan
        l = str(loc).lower()
        if 'location' in l: return 1
        if 'vente'    in l: return 0
        return np.nan

    df['type_transaction'] = df['localisation'].apply(get_tt)
    df = df.dropna(subset=['type_transaction'])

    df['prix'] = df['prix_montant'].apply(parse_prix)
    df = df.dropna(subset=['prix'])

    def get_ville(loc):
        if pd.isna(loc): return None
        m = re.search(r'(?:Location|Vente) à (.+)', str(loc))
        return m.group(1).strip() if m else None

    df['ville'] = df['localisation'].apply(get_ville)

    def get_tb(titre):
        if pd.isna(titre): return None
        t = str(titre).lower()
        if 'villa'        in t: return 'Villa'
        if 'maison'       in t: return 'Maison'
        if any(x in t for x in ['appartement', 'studio']): return 'Appartement'
        # Bureau, terrain → None (supprimés)
        return None

    df['type_bien'] = df['titre'].apply(get_tb)
    df = df.dropna(subset=['type_bien'])  # supprime Bureau, Terrain, Autre

    df['gouvernorat'] = df['ville'].apply(lambda v: get_gouvernorat(v))
    df = df.dropna(subset=['gouvernorat'])
    log(f'  Après filtre type_bien + gouvernorat : {len(df)}')

    df['surface_m2']  = pd.to_numeric(df['meta_Surface'], errors='coerce')
    df['nb_pieces']   = df['titre'].apply(
        lambda t: int(m.group(1)) if (m := re.search(r'S\+?(\d+)', str(t), re.I)) else np.nan)
    df['nb_chambres'] = pd.to_numeric(df['meta_Lits'], errors='coerce')
    df['nb_images']   = 1
    df['image_url']   = df['image'].astype(str)
    df['source_enc']  = 1  # bnb

    text = df['titre'].fillna('') + ' ' + df['description'].fillna('')
    equip_df = pd.DataFrame(
        text.apply(extract_equip_text).tolist(), index=df.index)
    for col in equip_df.columns:
        df[col] = equip_df[col]
    for col in ALL_EQUIP:
        if col not in df.columns: df[col] = 0

    log(f'  Lignes finales : {len(df)}')
    return df


# ================================================================
# CREATION DES 6 FICHIERS
# ================================================================

def build_ville_enc_segment(df, seg_name):
    """
    Calcule ville_enc specifiquement pour un segment.
    Mediane prix par ville dans CE segment uniquement, normalisee 0-1.
    Sauvegarde dans ville_mapping_{seg_name}.json pour l inference.
    """
    data = df[['ville','prix']].dropna(subset=['ville','prix'])

    ville_stats = data.groupby('ville').agg(
        prix_median=('prix','median'),
        n=('prix','count')
    ).reset_index()

    # Garder villes avec >= 3 annonces dans ce segment
    ville_stats = ville_stats[ville_stats['n'] >= 3]

    if len(ville_stats) == 0:
        return {}

    vmin = ville_stats['prix_median'].min()
    vmax = ville_stats['prix_median'].max()
    if vmax == vmin:
        ville_stats['ville_enc'] = 0.5
    else:
        ville_stats['ville_enc'] = (
            (ville_stats['prix_median'] - vmin) / (vmax - vmin)
        ).round(4)

    ville_map = dict(zip(ville_stats['ville'], ville_stats['ville_enc']))
    
    # Ajouter la mediane comme valeur par defaut pour villes inconnues
    median_val = float(ville_stats['ville_enc'].median()) if len(ville_stats) > 0 else 0.5
    ville_map['__default__'] = round(median_val, 4)

    mapping_file = f'ville_mapping_{seg_name}.json'
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(ville_map, f, ensure_ascii=False, indent=2)
    log(f'  {mapping_file} : {len(ville_map)-1} villes | default={median_val:.3f}')

    return ville_map


def create_segments(df_mub, df_bnb):
    section('CREATION DES 6 FICHIERS')

    # Fusionner les deux sources
    COMMON = ['prix', 'type_transaction', 'type_bien', 'gouvernorat', 'ville',
              'surface_m2', 'nb_pieces', 'nb_chambres', 'nb_images',
              'image_url', 'source_enc'] + ALL_EQUIP

    def select(df):
        for c in COMMON:
            if c not in df.columns: df[c] = np.nan
        return df[COMMON].copy()

    df = pd.concat([select(df_mub), select(df_bnb)], ignore_index=True)
    log(f'Total fusionné : {len(df)} lignes')

    # ville_enc sera calcule separement par segment ci-dessous

    # Encoder gouvernorat
    df['gouvernorat_enc'] = df['gouvernorat'].map(GOV_ENC).fillna(0).astype(int)

    # Équipements nulls → 0
    for col in ALL_EQUIP:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df['nb_images']        = df['nb_images'].fillna(1).astype(int)
    df['type_transaction'] = df['type_transaction'].astype(int)
    df['source_enc']       = df['source_enc'].fillna(0).astype(int)

    results = {}

    for seg_name, cfg in SEGMENTS.items():
        # Filtrer par type_bien
        mask = df['type_bien'] == cfg['type_bien']

        # Filtrer par transaction si spécifié
        if cfg['transaction'] is not None:
            mask = mask & (df['type_transaction'] == cfg['transaction'])

        seg = df[mask].copy()

        # Seuils prix
        tb_key = cfg['type_bien'].lower()
        seg = apply_price_filter(seg, tb_key)

        # Seuils surface
        smax = SURF_MAX.get(tb_key, 5000)
        n_surf = (seg['surface_m2'] > smax).sum()
        if n_surf > 0:
            seg.loc[seg['surface_m2'] > smax, 'surface_m2'] = np.nan
            log(f'  [{seg_name}] Surface > {smax}m² : {n_surf} nullifiées')
        seg = seg[~((seg['surface_m2'] < 20) & seg['surface_m2'].notna())]

        # Imputer surface/pieces par médiane
        for col in ['surface_m2', 'nb_pieces', 'nb_chambres']:
            med = seg[col].median()
            seg[col] = seg[col].fillna(med)

        # Calculer ville_enc specifique a ce segment
        log(f'  Calcul ville_enc pour [{seg_name}]...')
        seg['ville'] = df['ville'].loc[seg.index].values if 'ville' in df.columns else 'Inconnu'
        ville_map_seg = build_ville_enc_segment(seg, seg_name)
        median_enc = float(np.median(list(ville_map_seg.values()))) if ville_map_seg else 0.5
        seg['ville_enc'] = seg['ville'].map(ville_map_seg).fillna(median_enc).round(4)
        n_unknown = seg['ville'].map(ville_map_seg).isna().sum()
        log(f'  ville_enc [{seg_name}] : min={seg["ville_enc"].min():.3f} | max={seg["ville_enc"].max():.3f} | villes_inconnues={n_unknown} → median={median_enc:.3f}')

        # Selectionner les features du segment + target + image_url
        feat_cols = cfg['features']
        out_cols  = feat_cols + ['prix', 'image_url']
        seg_out   = seg[[c for c in out_cols if c in seg.columns]].copy()

        # Sauvegarder
        seg_out.to_csv(cfg['file'], index=False)

        img_pct = seg_out['image_url'].notna().sum() / len(seg_out) * 100
        log(f'  [{seg_name}] → {cfg["file"]} | {len(seg_out)} lignes | '
            f'{len(feat_cols)} features | images={img_pct:.0f}%')
        results[seg_name] = len(seg_out)

    return results


# ================================================================
# RAPPORT FINAL
# ================================================================

def print_report(results):
    section('RAPPORT FINAL')
    total = sum(results.values())
    print()
    for name, n in results.items():
        cfg = SEGMENTS[name]
        tt  = {0:'Vente', 1:'Location', None:'Vente+Location'}[cfg['transaction']]
        print(f'  {name:<20} : {n:>5} lignes | {tt}')
    print(f'\n  Total : {total:,} annonces dans 6 fichiers')
    print()
    print('  Prochaine étape : python vision_new.py')


# ================================================================
# MAIN
# ================================================================

def run():
    section('PREPARE.PY v4 — DIVISION EN 6 SEGMENTS')

    for f in [MUBAWAB_FILE, BNB_FILE]:
        if not os.path.exists(f):
            print(f'ERREUR : {f} introuvable')
            return

    df_mub = load_mubawab()
    df_bnb = load_bnb()
    results = create_segments(df_mub, df_bnb)
    print_report(results)


if __name__ == '__main__':
    run()