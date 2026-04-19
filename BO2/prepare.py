"""
prepare.py v5 — Preparation des donnees immobilieres tunisiennes
EstateMind BO2

Sources :
    - mubawab_annonces.csv
    - bnb_properties.csv
    - residentiel_BO2.xlsx
    - foncier_BO2.xlsx
    - commercial_BO2.xlsx
    - divers_BO2.xlsx

Outputs :
    dataset_residentiel.csv  → Appartements + Maisons + Villas
    dataset_terrain.csv      → Terrains
    ville_mapping_residentiel.json
    ville_mapping_terrain.json

Usage :
    python prepare.py
"""

import os, re, json, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

# ================================================================
# CONFIG
# ================================================================

MUBAWAB_FILE = 'mubawab_annonces.csv'
BNB_FILE     = 'bnb_properties.csv'

BO2_FILES = {
    'residentiel': 'residentiel_BO2.xlsx',
    'foncier':     'foncier_BO2.xlsx',
    'commercial':  'commercial_BO2.xlsx',
    'divers':      'divers_BO2.xlsx',
}

BO2_FEATURES = [
    'lat', 'lon', 'score_attractivite', 'market_tension',
    'cycle_marche', 'prix_region_median', 'text_embedding_score',
]

# type_bien encoding
# 1 = Appartement, 2 = Maison, 3 = Villa
# type_transaction : 1 = Location, 2 = Vente

# Encodage gouvernorat officiel tunisien
GOV_ENC = {
    'Ariana': 1, 'Béja': 2, 'Ben Arous': 3, 'Bizerte': 4,
    'Gabès': 5, 'Gafsa': 6, 'Jendouba': 7, 'Kairouan': 8,
    'Kasserine': 9, 'Kébili': 10, 'Le Kef': 11, 'Mahdia': 12,
    'Manouba': 13, 'Médenine': 14, 'Monastir': 15, 'Nabeul': 16,
    'Sfax': 17, 'Sidi Bouzid': 18, 'Siliana': 19, 'Sousse': 20,
    'Tataouine': 21, 'Tozeur': 22, 'Tunis': 23, 'Zaghouan': 24,
}

# Seuils prix par type et transaction
PRICE_LIMITS = {
    (1, 2): (15_000,  20_000_000),   # appart vente
    (1, 1): (150,     25_000),        # appart location
    (2, 2): (30_000,  20_000_000),   # maison vente
    (2, 1): (200,     50_000),        # maison location
    (3, 2): (50_000,  40_000_000),   # villa vente
    (3, 1): (500,     100_000),       # villa location
    (4, 2): (5_000,   50_000_000),   # terrain vente
    (4, 1): (200,     500_000),       # terrain location
}

SURF_MAX = {1: 2000, 2: 5000, 3: 5000, 4: 500_000}

# Colonnes equipements
ALL_HAS = [
    'has_ascenseur', 'has_meuble', 'has_terrasse', 'has_piscine',
    'has_vue_mer', 'has_garage', 'has_securite', 'has_climatisation',
    'has_chauffage', 'has_cuisine_equip', 'has_double_vitrage',
    'has_porte_blindee', 'has_concierge', 'has_jardin', 'has_standing',
    'has_vue_montagne', 'has_cheminee',
]

# Colonnes du dataset residentiel final
COLS_RESIDENTIEL = (
    ['gouvernorat_enc', 'ville_enc', 'quartier_enc', 'lat', 'lon', 'surface_m2',
     'nb_pieces', 'nb_chambres', 'nb_sdb', 'score_attractivite', 'market_tension',
     'cycle_marche', 'prix_region_median', 'text_embedding_score',
     'nb_images', 'source_enc', 'prix', 'image_url', 'images',
     'type_bien', 'type_transaction'] + ALL_HAS
)

# Colonnes du dataset terrain final
COLS_TERRAIN = [
    'gouvernorat_enc', 'ville_enc', 'lat', 'lon', 'surface_m2',
    'score_attractivite', 'market_tension', 'prix_region_median',
    'text_embedding_score', 'nb_images', 'source_enc',
    'type_transaction', 'has_vue_mer', 'has_vue_montagne',
    'prix', 'image_url', 'images',
]

# Mapping ville → gouvernorat
VILLE_GOV = {
    'tunis': 'Tunis', 'carthage': 'Tunis', 'la marsa': 'Tunis', 'marsa': 'Tunis',
    'gammarth': 'Tunis', 'sidi bou said': 'Tunis', 'el menzah': 'Tunis',
    'menzah': 'Tunis', 'ennasr': 'Tunis', 'el manar': 'Tunis',
    'le bardo': 'Tunis', 'bardo': 'Tunis', 'lac': 'Tunis',
    'les berges du lac': 'Tunis', 'berges du lac': 'Tunis',
    'el aouina': 'Tunis', 'aouina': 'Tunis', 'charguia': 'Tunis',
    'montplaisir': 'Tunis', 'lafayette': 'Tunis', 'el omrane': 'Tunis',
    'le kram': 'Tunis', 'la goulette': 'Tunis', 'sidi daoud': 'Tunis',
    'ain zaghouan': 'Tunis', 'jardins de carthage': 'Tunis',
    'centre urbain nord': 'Tunis', 'mutuelleville': 'Tunis',
    'ariana': 'Ariana', 'raoued': 'Ariana', 'la soukra': 'Ariana',
    'soukra': 'Ariana', 'mnihla': 'Ariana', 'borj louzir': 'Ariana',
    'ghazala': 'Ariana', 'sidi thabet': 'Ariana',
    'ben arous': 'Ben Arous', 'ezzahra': 'Ben Arous', 'hammam lif': 'Ben Arous',
    'megrine': 'Ben Arous', 'rades': 'Ben Arous', 'radès': 'Ben Arous',
    'fouchana': 'Ben Arous', 'mornag': 'Ben Arous', 'borj cedria': 'Ben Arous',
    'manouba': 'Manouba', 'douar hicher': 'Manouba', 'jedaida': 'Manouba',
    'nabeul': 'Nabeul', 'hammamet': 'Nabeul', 'kelibia': 'Nabeul',
    'korba': 'Nabeul', 'soliman': 'Nabeul', 'grombalia': 'Nabeul',
    'yasmine hammamet': 'Nabeul', 'dar chaabane': 'Nabeul',
    'bizerte': 'Bizerte', 'mateur': 'Bizerte', 'menzel bourguiba': 'Bizerte',
    'raf raf': 'Bizerte', 'rafraf': 'Bizerte',
    'sousse': 'Sousse', 'hammam sousse': 'Sousse', 'akouda': 'Sousse',
    'sahloul': 'Sousse', 'kantaoui': 'Sousse', 'kalaa kebira': 'Sousse',
    'monastir': 'Monastir', 'moknine': 'Monastir', 'jemmal': 'Monastir',
    'sfax': 'Sfax', 'sakiet ezzit': 'Sfax', 'sakiet eddaier': 'Sfax',
    'mahdia': 'Mahdia', 'el jem': 'Mahdia',
    'kairouan': 'Kairouan', 'zaghouan': 'Zaghouan',
    'gabes': 'Gabès', 'gabès': 'Gabès',
    'medenine': 'Médenine', 'zarzis': 'Médenine',
    'djerba': 'Médenine', 'houmt souk': 'Médenine', 'midoun': 'Médenine',
    'gafsa': 'Gafsa', 'tozeur': 'Tozeur', 'kebili': 'Kébili',
    'tataouine': 'Tataouine', 'siliana': 'Siliana', 'kasserine': 'Kasserine',
    'sidi bouzid': 'Sidi Bouzid', 'le kef': 'Le Kef', 'beja': 'Béja',
    'jendouba': 'Jendouba', 'tabarka': 'Jendouba',
}

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
    'has_standing':       'Standing',
}

DESC_KEYWORDS = {
    'has_meuble':        ['meublé', 'meuble', 'furnished'],
    'has_piscine':       ['piscine'],
    'has_jardin':        ['jardin'],
    'has_terrasse':      ['terrasse', 'balcon'],
    'has_securite':      ['gardien', 'sécurisée', 'concierge'],
    'has_standing':      ['standing', 'luxe', 'prestige'],
    'has_vue_mer':       ['vue sur mer', 'vue mer', 'bord de mer'],
    'has_ascenseur':     ['ascenseur'],
    'has_climatisation': ['climatisation'],
    'has_garage':        ['garage'],
    'has_cuisine_equip': ['cuisine équipée', 'cuisine equipee'],
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
    if titre and 'villa' in str(titre).lower(): return 3
    if 'Villa'       in v: return 3
    if any(x in v for x in ['Appartement', 'Studio', 'Duplex']): return 1
    if 'Maison'      in v: return 2
    if 'Terrain'     in v: return 4
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


def build_ville_enc(df, seg_name, min_ann=5):
    """
    Calcule ville_enc depuis le dataset fusionne (mubawab + bnb).
    Normalisation percentile 5%-95% pour eviter que les valeurs
    extremes (ex: Carthage) ecrasent les autres villes.

    Args:
        df       : DataFrame fusionne avec colonnes ville et prix
        seg_name : nom du segment pour le fichier JSON
        min_ann  : minimum annonces par ville (default=5)
    """
    data = df[['ville', 'prix']].dropna(subset=['ville', 'prix'])

    # Filtre outliers 2%-98%
    p2, p98 = data['prix'].quantile(0.02), data['prix'].quantile(0.98)
    data = data[(data['prix'] >= p2) & (data['prix'] <= p98)]

    stats = data.groupby('ville').agg(
        prix_median=('prix', 'median'),
        n=('prix', 'count')
    ).reset_index()
    stats = stats[stats['n'] >= min_ann]

    if len(stats) == 0:
        log(f'  ⚠ Pas assez de donnees pour {seg_name}')
        return {'__default__': 0.3}, 0.3

    # Normalisation percentile 5%-95%
    # Evite que Carthage (valeur extreme) ecrase La Marsa, Sousse etc.
    vmin = stats['prix_median'].quantile(0.05)
    vmax = stats['prix_median'].quantile(0.95)
    if vmax == vmin:
        stats['ville_enc'] = 0.5
    else:
        stats['ville_enc'] = ((stats['prix_median'] - vmin) /
                               (vmax - vmin)).clip(0, 1).round(4)

    ville_map = dict(zip(stats['ville'], stats['ville_enc']))
    median_val = float(stats['ville_enc'].median())
    ville_map['__default__'] = round(median_val, 4)

    with open(f'ville_mapping_{seg_name}.json', 'w', encoding='utf-8') as f:
        json.dump(ville_map, f, ensure_ascii=False, indent=2)

    # Afficher top 5 pour verification
    top5 = stats.nlargest(5, 'prix_median')[['ville','prix_median','ville_enc']]
    log(f'  ville_mapping_{seg_name}.json : {len(ville_map)-1} villes | default={median_val:.3f}')
    for _, row in top5.iterrows():
        log(f'    {row["ville"]:<20} : {row["prix_median"]:>10,.0f} TND | enc={row["ville_enc"]:.3f}')

    return ville_map, median_val


# ================================================================
# CHARGEMENT BO2
# ================================================================

def load_bo2():
    dfs = []
    for name, filepath in BO2_FILES.items():
        if not os.path.exists(filepath):
            log(f'  ⚠ {filepath} introuvable')
            continue
        df = pd.read_excel(filepath)
        cols = ['image_url'] + [c for c in BO2_FEATURES if c in df.columns]
        df = df[cols].copy()
        for col in BO2_FEATURES:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        dfs.append(df)
        log(f'  BO2 {name} : {len(df)} lignes')
    if not dfs:
        return pd.DataFrame(columns=['image_url'] + BO2_FEATURES)
    bo2 = pd.concat(dfs, ignore_index=True)
    bo2 = bo2.drop_duplicates(subset=['image_url'], keep='first')
    log(f'  BO2 total apres dedup : {len(bo2)} lignes')
    return bo2


# ================================================================
# CHARGEMENT MUBAWAB
# ================================================================

def load_mubawab():
    section('MUBAWAB')
    df = pd.read_csv(MUBAWAB_FILE)
    log(f'Charge : {len(df)} lignes')

    df = df[df['categorie'] != 'vacances'].copy()
    df['type_transaction'] = df['categorie'].map({'vente': 2, 'location': 1})
    df = df.dropna(subset=['type_transaction'])
    df['type_transaction'] = df['type_transaction'].astype(int)

    df['prix'] = pd.to_numeric(df['prix_tnd'], errors='coerce')
    df = df.dropna(subset=['prix'])

    df['type_bien'] = df.apply(
        lambda r: normalize_type_bien(r['type_bien'], None), axis=1)
    df = df.dropna(subset=['type_bien'])
    df['type_bien'] = df['type_bien'].astype(int)

    df['gouvernorat'] = df.apply(
        lambda r: get_gouvernorat(r['ville'], r['gouvernorat']), axis=1)
    df = df.dropna(subset=['gouvernorat'])

    df['surface_m2']  = pd.to_numeric(df['surface_m2'],  errors='coerce')
    df['nb_pieces']   = pd.to_numeric(df['nb_pieces'],   errors='coerce')
    df['nb_chambres'] = pd.to_numeric(df['nb_chambres'], errors='coerce')
    df.loc[df['nb_pieces']   > 20, 'nb_pieces']   = np.nan
    df.loc[df['nb_chambres'] > 20, 'nb_chambres'] = np.nan
    df['nb_images']  = pd.to_numeric(df['nb_images'], errors='coerce').fillna(1)
    df['image_url']  = df['image_url'].astype(str)
    df['images']     = df['images'].astype(str) if 'images' in df.columns else df['image_url']
    df['source_enc'] = 0
    df['ville']      = df['ville'].fillna('Inconnu')
    df['quartier']   = df['quartier'].fillna('') if 'quartier' in df.columns else ''
    df['nb_sdb']     = pd.to_numeric(df['nb_sdb'], errors='coerce').fillna(0).astype(int)                        if 'nb_sdb' in df.columns else 0

    equip_df = pd.DataFrame(
        df['equipements'].apply(extract_equip_mubawab).tolist(), index=df.index)
    for col in equip_df.columns:
        df[col] = equip_df[col]
    for col in ALL_HAS:
        if col not in df.columns: df[col] = 0

    log(f'Lignes finales : {len(df)}')
    return df


# ================================================================
# CHARGEMENT BNB
# ================================================================

def load_bnb():
    section('BNB')
    df = pd.read_csv(BNB_FILE)
    log(f'Charge : {len(df)} lignes')

    def get_tt(loc):
        if pd.isna(loc): return np.nan
        l = str(loc).lower()
        if 'location' in l: return 1
        if 'vente'    in l: return 2
        return np.nan

    df['type_transaction'] = df['localisation'].apply(get_tt)
    df = df.dropna(subset=['type_transaction'])
    df['type_transaction'] = df['type_transaction'].astype(int)

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
        if 'villa'   in t: return 3
        if 'maison'  in t: return 2
        if any(x in t for x in ['appartement', 'studio']): return 1
        return None

    df['type_bien'] = df['titre'].apply(get_tb)
    df = df.dropna(subset=['type_bien'])
    df['type_bien'] = df['type_bien'].astype(int)

    df['gouvernorat'] = df['ville'].apply(lambda v: get_gouvernorat(v))
    df = df.dropna(subset=['gouvernorat'])

    df['surface_m2']  = pd.to_numeric(df['meta_Surface'], errors='coerce')
    df['nb_pieces']   = df['titre'].apply(
        lambda t: int(m.group(1)) if (m := re.search(r'S\+?(\d+)', str(t), re.I)) else np.nan)
    df['nb_chambres'] = pd.to_numeric(df['meta_Lits'], errors='coerce')
    df['nb_images']   = 1
    df['image_url']   = df['image'].astype(str)
    df['images']      = df['image'].astype(str)  # BnB = 1 seule image
    df['source_enc']  = 1
    df['quartier']    = ''   # BnB n a pas de quartier
    df['nb_sdb']      = 0    # BnB n a pas nb_sdb

    text = df['titre'].fillna('') + ' ' + df['description'].fillna('')
    equip_df = pd.DataFrame(
        text.apply(extract_equip_text).tolist(), index=df.index)
    for col in equip_df.columns:
        df[col] = equip_df[col]
    for col in ALL_HAS:
        if col not in df.columns: df[col] = 0

    log(f'Lignes finales : {len(df)}')
    return df


# ================================================================
# CREATION DES 2 FICHIERS
# ================================================================

def create_datasets(df_mub, df_bnb):
    section('FUSION ET CREATION DES DATASETS')

    # Colonnes communes
    COMMON = (['prix', 'type_transaction', 'type_bien', 'gouvernorat', 'ville',
               'quartier', 'surface_m2', 'nb_pieces', 'nb_chambres', 'nb_sdb',
               'nb_images', 'image_url', 'images', 'source_enc'] + ALL_HAS)

    def select(df):
        for c in COMMON:
            if c not in df.columns: df[c] = np.nan
        return df[COMMON].copy()

    df = pd.concat([select(df_mub), select(df_bnb)], ignore_index=True)
    log(f'Total fusionne : {len(df)} lignes')

    # Enrichissement BO2
    section('ENRICHISSEMENT BO2')
    bo2 = load_bo2()
    if not bo2.empty:
        df = df.merge(bo2, on='image_url', how='left')
        n_matched = df['lat'].notna().sum()
        log(f'Jointure BO2 : {n_matched}/{len(df)} lignes enrichies ({n_matched/len(df)*100:.0f}%)')
        for col in BO2_FEATURES:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(df[col].median())
    else:
        for col in BO2_FEATURES:
            df[col] = 0.0

    # Gouvernorat encode
    df['gouvernorat_enc'] = df['gouvernorat'].map(GOV_ENC).fillna(0).astype(int)

    # Equipements → 0
    for col in ALL_HAS:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df['nb_images']        = df['nb_images'].fillna(1).astype(int)
    df['type_transaction'] = pd.to_numeric(df['type_transaction'], errors='coerce').fillna(2).astype(int)
    df['source_enc']       = df['source_enc'].fillna(0).astype(int)
    df['type_bien']        = pd.to_numeric(df['type_bien'], errors='coerce').astype(int)

    # ── DATASET RESIDENTIEL VENTE + LOCATION ────────────────────
    section('DATASET RESIDENTIEL (Appart + Maison + Villa)')

    res_all = df[df['type_bien'].isin([1, 2, 3])].copy()

    # Filtres prix
    for (tb, tt), (pmin, pmax) in PRICE_LIMITS.items():
        if tb == 4: continue
        mask = (res_all['type_bien'] == tb) & (res_all['type_transaction'] == tt)
        n_drop = (mask & ((res_all['prix'] < pmin) | (res_all['prix'] > pmax))).sum()
        if n_drop > 0:
            res_all = res_all[~(mask & ((res_all['prix'] < pmin) | (res_all['prix'] > pmax)))]
            log(f'  Prix hors seuil type_bien={tb} tt={tt} : {n_drop} supprimes')

    # Filtres surface
    for tb, smax in SURF_MAX.items():
        if tb == 4: continue
        mask = res_all['type_bien'] == tb
        res_all.loc[mask & (res_all['surface_m2'] > smax), 'surface_m2'] = np.nan
    res_all = res_all[~((res_all['surface_m2'] < 20) & res_all['surface_m2'].notna())]

    # Imputer surface/pieces
    for col in ['surface_m2', 'nb_pieces', 'nb_chambres']:
        res_all[col] = res_all[col].fillna(res_all[col].median())

    res_all['ville'] = df['ville'].loc[res_all.index].fillna('Inconnu').values

    # ── Vente ────────────────────────────────────────────────────
    section('DATASET RESIDENTIEL VENTE')
    res_v = res_all[res_all['type_transaction'] == 2].copy()
    ville_map_v, med_v = build_ville_enc(res_v, 'residentiel_vente')
    res_v['ville_enc'] = res_v['ville'].map(ville_map_v).fillna(med_v).round(4)

    # Quartier enc vente
    df_q_v = res_v[['quartier', 'prix']].copy()
    df_q_v.columns = ['ville', 'prix']
    df_q_v['ville'] = df_q_v['ville'].fillna('').astype(str)
    quartier_map_v, med_qv = build_ville_enc(df_q_v, 'quartier_vente', min_ann=3)
    res_v['quartier_enc'] = res_v['quartier'].fillna('').map(quartier_map_v).fillna(med_qv).round(4)
    res_v['nb_sdb'] = pd.to_numeric(res_v.get('nb_sdb', 0), errors='coerce').fillna(0).astype(int)

    for col in COLS_RESIDENTIEL:
        if col not in res_v.columns: res_v[col] = 0
    res_v = res_v[COLS_RESIDENTIEL].reset_index(drop=True)
    res_v.to_csv('dataset_residentiel_vente.csv', index=False)
    log(f'  dataset_residentiel_vente.csv : {len(res_v):,} lignes')
    for tb, label in {1:'Appartement', 2:'Maison', 3:'Villa'}.items():
        log(f'    {label:<15}: {(res_v["type_bien"]==tb).sum():,}')

    # ── Location ─────────────────────────────────────────────────
    section('DATASET RESIDENTIEL LOCATION')
    res_l = res_all[res_all['type_transaction'] == 1].copy()
    ville_map_l, med_l = build_ville_enc(res_l, 'residentiel_location')
    res_l['ville_enc'] = res_l['ville'].map(ville_map_l).fillna(med_l).round(4)

    # Quartier enc location
    df_q_l = res_l[['quartier', 'prix']].copy()
    df_q_l.columns = ['ville', 'prix']
    df_q_l['ville'] = df_q_l['ville'].fillna('').astype(str)
    quartier_map_l, med_ql = build_ville_enc(df_q_l, 'quartier_location', min_ann=3)
    res_l['quartier_enc'] = res_l['quartier'].fillna('').map(quartier_map_l).fillna(med_ql).round(4)
    res_l['nb_sdb'] = pd.to_numeric(res_l.get('nb_sdb', 0), errors='coerce').fillna(0).astype(int)

    for col in COLS_RESIDENTIEL:
        if col not in res_l.columns: res_l[col] = 0
    res_l = res_l[COLS_RESIDENTIEL].reset_index(drop=True)
    res_l.to_csv('dataset_residentiel_location.csv', index=False)
    log(f'  dataset_residentiel_location.csv : {len(res_l):,} lignes')
    for tb, label in {1:'Appartement', 2:'Maison', 3:'Villa'}.items():
        log(f'    {label:<15}: {(res_l["type_bien"]==tb).sum():,}')

    # retourner les deux pour le rapport
    res = res_v  # pour compatibilite

    # ── DATASET TERRAIN ──────────────────────────────────────────
    section('DATASET TERRAIN')

    ter = df[df['type_bien'] == 4].copy()

    # Filtres prix terrain
    for tt, (pmin, pmax) in {2: PRICE_LIMITS[(4,2)], 1: PRICE_LIMITS[(4,1)]}.items():
        mask = ter['type_transaction'] == tt
        n_drop = (mask & ((ter['prix'] < pmin) | (ter['prix'] > pmax))).sum()
        if n_drop > 0:
            ter = ter[~(mask & ((ter['prix'] < pmin) | (ter['prix'] > pmax)))]
            log(f'  Prix hors seuil terrain tt={tt} : {n_drop} supprimes')

    ter.loc[ter['surface_m2'] > SURF_MAX[4], 'surface_m2'] = np.nan
    ter = ter[~((ter['surface_m2'] < 20) & ter['surface_m2'].notna())]
    ter['surface_m2'] = ter['surface_m2'].fillna(ter['surface_m2'].median())

    # ville_enc terrain
    ter['ville'] = df['ville'].loc[ter.index].values
    ville_map_t, med_t = build_ville_enc(ter, 'terrain')
    ter['ville_enc'] = ter['ville'].map(ville_map_t).fillna(med_t).round(4)

    # Colonnes finales
    for col in COLS_TERRAIN:
        if col not in ter.columns: ter[col] = 0
    ter = ter[COLS_TERRAIN].reset_index(drop=True)

    ter.to_csv('dataset_terrain.csv', index=False)
    log(f'  dataset_terrain.csv : {len(ter):,} lignes | {len(ter.columns)} colonnes')
    for tt, label in {1: 'Location', 2: 'Vente'}.items():
        log(f'    {label:<15}: {(ter["type_transaction"]==tt).sum():,}')

    return len(res_v) + len(res_l), len(ter)


# ================================================================
# MAIN
# ================================================================

def run():
    section('PREPARE.PY v5 — RESIDENTIEL + TERRAIN')

    for f in [MUBAWAB_FILE, BNB_FILE]:
        if not os.path.exists(f):
            print(f'ERREUR : {f} introuvable')
            return

    df_mub = load_mubawab()
    df_bnb = load_bnb()
    n_res, n_ter = create_datasets(df_mub, df_bnb)

    section('RAPPORT FINAL')
    print(f'  dataset_residentiel_vente.csv    : produit')
    print(f'  dataset_residentiel_location.csv : produit')
    print(f'  dataset_terrain.csv              : {n_ter:,} lignes')
    print(f'  Total residentiel                : {n_res:,} lignes')
    print(f'\n  Prochaine etape : python vision_new.py')


if __name__ == '__main__':
    run()