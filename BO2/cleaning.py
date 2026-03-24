"""
cleaning.py — Data cleaning module.

KEY CHANGE vs previous version:
  - prix, surface_m2, nb_pieces are NEVER imputed.
  - Rows with illogical values (below minimum thresholds) are DROPPED.
  - Only type_categorise (categorical) is imputed via mode.
  - gouvernorat Unknown → reverse geocoding (not imputation).
"""

import os, re, math, warnings
import numpy as np
import pandas as pd
from datetime import datetime

warnings.filterwarnings('ignore')

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from mappings import (
    GOUVERNORAT_ENC, VILLE_TO_GOUVERNORAT, TYPE_BIEN_ENC, TYPE_BIEN_DEC,
    TYPE_MAP, TYPE_KW, DESC_KW, NLP_KW, GROUPE_MAP, SEUILS,
    LOC_KEYWORDS, VENTE_KEYWORDS,
    normalize_gouvernorat, encode_gouvernorat, encode_type_bien, std_transaction,
)


def section(t): print("\n" + "=" * 65 + f"\n   {t}\n" + "=" * 65)
def log(m):     print(f"  {m}")


# ================================================================
# UTILITY FUNCTIONS
# ================================================================

def remove_noise(val):
    if pd.isna(val): return None
    val = str(val).encode('ascii', 'ignore').decode('ascii')
    val = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', val)
    val = re.sub(r'[^\w\s\.,;:!\?\-\(\)\/\'\"\@\#\%\&\+\=]', '', val)
    return re.sub(r'\s+', ' ', val).strip() or None


def clean_price(v):
    if pd.isna(v): return None
    v = re.sub(r'[^\d.]', '', str(v).replace(',', '.'))
    try: return float(v) if v else None
    except: return None


def clean_surface(v):
    if pd.isna(v): return None
    v = re.sub(r'[^\d.]', '', str(v).replace(',', '.'))
    try: return float(v) if v else None
    except: return None


# ── Normalisation des noms de ville ─────────────────────────────
# Supprime les préfixes parasites scrapés ("location tunis" → "tunis")
# Corrige les typos et variantes ("nabul" → "nabeul", "mgrine" → "megrine")
# Conserve les zones précises qui ont une vraie valeur immobilière
# ("les berges du lac 2" → conservé car zone de luxe distincte)

_CITY_PREFIXES = [
    'location de vacances ', 'location ', 'vente ',
    'à louer ', 'a louer ', 'à vendre ', 'a vendre ',
]

# Dates relatives scrapées depuis Tayara/Facebook → extraire la ville avant la virgule
# ex: "tunis , 2 months ago" → "tunis"
# ex: "la marsa , a month ago" → "la marsa"
# ex: ", 2 months ago" → None (pas de ville)
_DATE_KEYWORDS = ['month', 'day', 'week', 'hour', 'minute', 'ago', 'just now']

_CITY_TYPOS: dict[str, str | None] = {
    # Invalides → None
    'non spcifi': None, 'non specifi': None, 'non spécifié': None,
    'la': None, 'louer': None, 'vendre': None, 'inconnu': None,
    'autre': None, '': None, 'none': None, 'nan': None,
    'tunisie': None, 'tunisia': None,
    # Typos fréquentes
    'nabul': 'nabeul', 'nabel': 'nabeul', 'nbeul': 'nabeul',
    'mgrine': 'megrine', 'mégrine': 'megrine',
    'kala kebira': 'kalaa kebira', 'kala sghira': 'kalaa sghira',
    'cit el khadra': 'cite el khadra',
    'cit el wafa afh 2': 'cite el wafa', 'cite el wafa afh2': 'cite el wafa',
    'kairouan ville': 'kairouan', 'ariana ville': 'ariana',
    'sfax ville': 'sfax', 'sousse ville': 'sousse',
    'monastir ville': 'monastir', 'mahdia ville': 'mahdia',
    'gammarth sup': 'gammarth', 'gammarth superieur': 'gammarth',
    'sidi bou ali sousse': 'sidi bou ali',
    'location tunis': 'tunis', 'vente tunis': 'tunis',
    # Zones Manouba fréquemment mal codées
    'sahloul': 'sahloul',     # Nabeul, pas Manouba
    'klibia': 'kelibia',      # Nabeul
    'khezama est': 'khezama est',
    'khezama ouest': 'khezama ouest',
    # Adresses → None (trop longues, pas exploitables)
}

def clean_city(val):
    if pd.isna(val): return None
    v = remove_noise(val)
    if not v: return None
    v = v.strip().lower()

    # 1. Supprime les préfixes parasites (location/vente)
    for prefix in _CITY_PREFIXES:
        if v.startswith(prefix):
            v = v[len(prefix):].strip()
            break

    # 2. Dates relatives scrapées: "tunis , 2 months ago" → "tunis"
    #    Si la valeur contient une date relative, on extrait la partie avant la virgule
    if any(kw in v for kw in _DATE_KEYWORDS):
        if ',' in v:
            candidate = v.split(',')[0].strip()
            # Si la partie avant la virgule est une vraie ville (pas vide)
            if candidate and len(candidate) > 1:
                v = candidate
            else:
                return None  # ex: ", 2 months ago" → pas de ville
        else:
            return None  # ex: "2 months ago" sans ville

    # 3. Adresses trop longues → non exploitables
    if len(v) > 45 and any(c.isdigit() for c in v):
        # Adresses avec numéros de rue → tenter d'extraire une ville connue
        # ex: "boulevard 14 janvier , kantaoui 4089 hammam soussa"
        for kw in ['hammamet', 'hammam sousse', 'tunis', 'sfax', 'sousse',
                   'nabeul', 'ariana', 'monastir', 'bizerte', 'mahdia',
                   'djerba', 'la marsa', 'sfax', 'kairouan']:
            if kw in v:
                v = kw
                break
        else:
            return None  # adresse incompréhensible

    # 4. Corrige les typos/variantes connues
    if v in _CITY_TYPOS:
        v = _CITY_TYPOS[v]

    if not v or len(v) < 2: return None
    return v.strip()


def extract_ville_from_localisation(val):
    """
    Extrait la ville précise depuis un champ 'localisation' de type :
      'Tunis - La Marsa'        → 'la marsa'
      'Sfax - Sakiet Ezzit'     → 'sakiet ezzit'
      'Nabeul - Hammamet'       → 'hammamet'
      'La Marsa'                → 'la marsa'  (pas de tiret → ville = champ entier)
      'Tunis'                   → 'tunis'
    """
    if pd.isna(val): return None
    v = remove_noise(val)
    if not v: return None
    v = v.strip()
    # Normalise tiret long → tiret court avant split
    v = v.replace(' – ', ' - ').replace('–', ' - ').replace(' — ', ' - ').replace('—', ' - ').replace(' – ', ' - ').replace('–', ' - ')
    # Si le champ contient " - " → prendre la partie après le tiret (= quartier précis)
    if ' - ' in v:
        parts = v.split(' - ', 1)
        ville_part = parts[1].strip()
    else:
        ville_part = v.strip()
    # Appliquer le nettoyage standard (préfixes, typos)
    return clean_city(ville_part)


def parse_date_flexible(val):
    if pd.isna(val): return pd.NaT
    val = str(val).strip()
    for fmt in ['%d/%m/%Y %H:%M', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M', '%Y-%m-%d', '%d-%m-%Y']:
        try: return datetime.strptime(val[:19], fmt)
        except: pass
    return pd.to_datetime(val, dayfirst=True, errors='coerce')


def parse_relative_date(relative_str, reference_date):
    if pd.isna(relative_str): return pd.to_datetime(reference_date)
    s = str(relative_str).lower().strip()
    try:
        ref = pd.to_datetime(reference_date)
        m = re.search(r'(\d+)', s)
        n = int(m.group(1)) if m else 1
        if 'minute' in s: return ref - pd.Timedelta(minutes=n)
        if 'hour'   in s: return ref - pd.Timedelta(hours=n)
        if 'day'    in s: return ref - pd.Timedelta(days=n)
        if 'week'   in s: return ref - pd.Timedelta(weeks=n)
        if 'month'  in s: return ref - pd.Timedelta(days=n * 30)
        if 'year'   in s: return ref - pd.Timedelta(days=n * 365)
        if 'just'   in s: return ref
    except: pass
    return pd.to_datetime(reference_date)


def extract_date_from_image_url_bnb(img_url):
    if pd.isna(img_url): return pd.NaT
    m = re.search(r'/uploads/(\d{4})/(\d{2})/', str(img_url))
    if m:
        try: return datetime(int(m.group(1)), int(m.group(2)), 1)
        except: pass
    return pd.NaT


def extract_date_from_facebook_cdn(img_url):
    if pd.isna(img_url): return pd.NaT
    m = re.search(r'oe=([0-9A-Fa-f]{8})', str(img_url))
    if m:
        try:
            ts  = int(m.group(1), 16)
            exp = datetime.fromtimestamp(ts)
            return exp - pd.Timedelta(days=90)
        except: pass
    return pd.NaT


def extract_date_from_filename(filename):
    m = re.search(r'(\d{8})', str(filename))
    if m:
        try: return datetime.strptime(m.group(1), '%Y%m%d')
        except: pass
    return datetime.now()


def extract_pieces_from_apptype(val):
    if pd.isna(val): return None, None
    v = str(val).strip()
    m = re.match(r'^App[\.\s]*\.?\s*(\d+)\s*Pic$', v, re.IGNORECASE)
    if m: return 'Appartement', int(m.group(1))
    if re.match(r'^Surfaces?$', v, re.IGNORECASE): return 'Terrain', None
    return None, None


def std_type_bien(val, surface_m2=None, nb_pieces=None):
    if pd.isna(val): return None
    tb, _ = extract_pieces_from_apptype(val)
    if tb: return tb
    v = str(val).lower().strip()
    if re.search(r'immo\s*neuf|projet\s*neuf', v):
        try:
            if (surface_m2 and float(surface_m2) > 150) or (nb_pieces and int(nb_pieces) > 4):
                return 'Maison'
        except: pass
        return 'Appartement'
    if v == 'autre': return 'Autre'
    for k, can in TYPE_MAP.items():
        if k in v: return can
    return str(val).strip().title()


def refine_autre(row) -> str:
    if row.get('type_bien') != 'Autre': return row.get('type_bien')
    desc = str(row.get('description') or '').lower()
    for typ, kws in DESC_KW.items():
        for kw in kws:
            if kw in desc: return typ
    surf = row.get('surface_m2')
    try:
        if surf and float(surf) > 500: return 'Terrain'
        if surf and float(surf) < 30:  return 'Chambre'
    except: pass
    return 'Autre'


def resolve_transaction(prix_val, trans_val, desc_str=None):
    if not pd.isna(trans_val):
        try:
            v = int(float(trans_val))
            if v in (1, 2): return v
        except: pass
    desc = str(desc_str or '').lower()
    if desc:
        loc_score   = sum(1 for kw in LOC_KEYWORDS   if kw in desc)
        vente_score = sum(1 for kw in VENTE_KEYWORDS  if kw in desc)
        if loc_score > 0 and loc_score >= vente_score:  return 1
        if vente_score > 0 and vente_score > loc_score: return 2
    if not pd.isna(prix_val):
        try:
            p = float(prix_val)
            if p < 5000:  return 1
            if p > 20000: return 2
            return 1
        except: pass
    return np.nan


def extract_nb_pieces_from_desc(desc) -> float:
    if pd.isna(desc): return np.nan
    s = str(desc).lower()
    m = re.search(r'\bs\s*\+\s*(\d)\b', s)
    if m: return int(m.group(1)) + 1
    m = re.search(r'(\d+)\s*pi[eè]ces?', s)
    if m: return int(m.group(1))
    m = re.search(r'(\d+)\s*chambre', s)
    if m: return int(m.group(1))
    m = re.search(r'\b[ft](\d)\b', s)
    if m: return int(m.group(1))
    if 'studio'  in s: return 1
    m = re.search(r'(\d+)\s*pic', s)
    if m: return int(m.group(1))
    if 'duplex'  in s: return 2
    if 'triplex' in s: return 3
    return np.nan


# ================================================================
# ETL — 8 SOURCES
# ================================================================

def load_sources(sources: list[tuple]) -> tuple[pd.DataFrame, int]:
    section("ETAPE 1 — ETL : CHARGEMENT ET FUSION")
    all_dfs = []

    for name, path, ftype, sep in sources:
        if not os.path.exists(path):
            log(f"MANQUANT  {name}"); continue
        try:
            raw = (pd.read_excel(path) if ftype == 'xlsx'
                   else pd.read_csv(path, sep=sep or ',', on_bad_lines='skip'))
        except Exception as e:
            log(f"ERREUR {name}: {e}"); continue

        if name == 'Facebook Marketplace':
            img_col  = 'image_url' if 'image_url' in raw.columns else None
            date_pub = (raw[img_col].apply(extract_date_from_facebook_cdn) if img_col
                        else pd.Series([extract_date_from_filename(path)] * len(raw)))
            std = pd.DataFrame({
                'prix': raw['prix_affiche'].apply(clean_price), 'surface_m2': None,
                'ville': raw['ville'].apply(clean_city), 'gouvernorat': raw['gouvernorat'].apply(clean_city),
                'type_bien': raw['type_bien'].apply(remove_noise),
                'type_transaction': raw['type_annonce'].apply(remove_noise),
                'chambres': None, 'pieces': None,
                'description': raw['description'].apply(remove_noise),
                'image_url': raw['image_url'], 'url': None, 'source': name, 'date_publication': date_pub,
            })
        elif name == 'Mubawab':
            date_pub = pd.Series([extract_date_from_filename(path)] * len(raw))
            std = pd.DataFrame({
                'prix': raw['prix_tnd'].apply(clean_price), 'surface_m2': raw['surface_m2'].apply(clean_surface),
                'ville': raw['ville'].apply(clean_city), 'gouvernorat': raw['gouvernorat'].apply(clean_city),
                'type_bien': raw['type_bien'].apply(remove_noise),
                'type_transaction': raw['categorie'].apply(remove_noise),
                'chambres': pd.to_numeric(raw['nb_chambres'], errors='coerce'),
                'pieces': pd.to_numeric(raw['nb_pieces'], errors='coerce'),
                'description': raw['titre'].apply(remove_noise),
                'image_url': raw['image_url'], 'url': raw['url'], 'source': name, 'date_publication': date_pub,
            })
        elif name == 'Mubawab Partial':
            dc = 'description' if 'description' in raw.columns else 'titre'
            date_pub = (raw['date_collecte'].apply(parse_date_flexible)
                        if 'date_collecte' in raw.columns
                        else pd.Series([extract_date_from_filename(path)] * len(raw)))
            std = pd.DataFrame({
                'prix': raw['prix'].apply(clean_price), 'surface_m2': raw['surface_m2'].apply(clean_surface),
                'ville': raw['ville'].apply(clean_city), 'gouvernorat': raw['ville'].apply(clean_city),
                'type_bien': (raw['type_propriete'].apply(remove_noise) if 'type_propriete' in raw.columns else None),
                'type_transaction': None,
                'chambres': pd.to_numeric(raw['nombre_chambres'], errors='coerce'), 'pieces': None,
                'description': raw[dc].apply(remove_noise),
                'image_url': None, 'url': raw['url'] if 'url' in raw.columns else None,
                'source': name, 'date_publication': date_pub,
            })
        elif name == 'Tayara':
            if 'scraped_at' in raw.columns and 'date' in raw.columns:
                date_pub = pd.Series([parse_relative_date(row['date'], row['scraped_at'])
                                      for _, row in raw.iterrows()])
            elif 'scraped_at' in raw.columns:
                date_pub = raw['scraped_at'].apply(parse_date_flexible)
            else:
                date_pub = pd.Series([extract_date_from_filename(path)] * len(raw))
            std = pd.DataFrame({
                'prix': raw['price'].apply(clean_price), 'surface_m2': raw['superficie'].apply(clean_surface),
                'ville': raw['location'].apply(clean_city), 'gouvernorat': raw['location'].apply(clean_city),
                'type_bien': raw['category'].apply(remove_noise),
                'type_transaction': raw['type_de_transaction'].apply(remove_noise),
                'chambres': pd.to_numeric(raw['chambres'], errors='coerce'), 'pieces': None,
                'description': raw['description'].apply(remove_noise),
                'image_url': raw['images'], 'url': raw['url'], 'source': name, 'date_publication': date_pub,
            })
        elif name == 'Tunisie Annonces':
            if   'date_insertion'    in raw.columns: date_pub = raw['date_insertion'].apply(parse_date_flexible)
            elif 'date_modification' in raw.columns: date_pub = raw['date_modification'].apply(parse_date_flexible)
            else:                                    date_pub = pd.Series([pd.NaT] * len(raw))
            std = pd.DataFrame({
                'prix': raw['prix_montant'].apply(clean_price), 'surface_m2': raw['surface_couverte'].apply(clean_surface),
                'ville': raw['region'].apply(clean_city) if 'region' in raw.columns else raw['gouvernorat'].apply(clean_city), 'gouvernorat': raw['gouvernorat'].apply(clean_city),
                'type_bien': raw['type_bien'].apply(remove_noise),
                'type_transaction': raw['type_transaction'].apply(remove_noise),
                'chambres': pd.to_numeric(raw['nombre_chambres'], errors='coerce'),
                'pieces': pd.to_numeric(raw['nombre_pieces'], errors='coerce'),
                'description': raw['description_complete'].apply(remove_noise),
                'image_url': None, 'url': raw['url_detail'], 'source': name, 'date_publication': date_pub,
            })
        elif name == 'Century21':
            date_pub = (raw['date_scraping'].apply(parse_date_flexible)
                        if 'date_scraping' in raw.columns
                        else pd.Series([extract_date_from_filename(path)] * len(raw)))
            std = pd.DataFrame({
                'prix': raw['prix'].apply(clean_price), 'surface_m2': raw['surface_m2'].apply(clean_surface),
                'ville': raw['localisation'].apply(extract_ville_from_localisation), 'gouvernorat': raw['localisation'].apply(clean_city),
                'type_bien': raw['type'].apply(remove_noise), 'type_transaction': raw['status'].apply(remove_noise),
                'chambres': None, 'pieces': None,
                'description': raw['description'].apply(remove_noise),
                'image_url': raw['image_url'], 'url': raw['url'], 'source': name, 'date_publication': date_pub,
            })
        elif name == 'HomeInTunisia':
            date_pub = (raw['date_scraping'].apply(parse_date_flexible)
                        if 'date_scraping' in raw.columns
                        else pd.Series([extract_date_from_filename(path)] * len(raw)))
            std = pd.DataFrame({
                'prix': raw['prix'].apply(clean_price), 'surface_m2': raw['surface_m2'].apply(clean_surface),
                'ville': raw['localisation'].apply(extract_ville_from_localisation), 'gouvernorat': raw['localisation'].apply(clean_city),
                'type_bien': raw['type'].apply(remove_noise), 'type_transaction': None,
                'chambres': pd.to_numeric(raw['nombre_chambres'], errors='coerce'),
                'pieces': pd.to_numeric(raw['nombre_pieces'], errors='coerce'),
                'description': raw['titre'].apply(remove_noise),
                'image_url': raw['image_url'], 'url': raw['url'], 'source': name, 'date_publication': date_pub,
            })
        elif name == 'BnB':
            img_col = 'image' if 'image' in raw.columns else None
            if img_col:
                date_pub = raw[img_col].apply(extract_date_from_image_url_bnb)
                date_pub = date_pub.fillna(extract_date_from_filename(path))
            else:
                date_pub = pd.Series([extract_date_from_filename(path)] * len(raw))

            # Extract type_bien from 'titre' column using TYPE_MAP keyword matching.
            # Strategy:
            #   1. Word-boundary regex match (longest key first) — avoids "chambre" in
            #      "chambres" blocking "villa" in "villa 3 chambres vue mer".
            #   2. Substring fallback for keys that don't work as whole words (s+1, s+2…).
            #   3. Default → 'Chambre' (BnB = short-term rental, most are rooms/apartments).
            _sorted_keys = sorted(TYPE_MAP.keys(), key=len, reverse=True)

            def _extract_type_from_titre(titre_val):
                if pd.isna(titre_val) or str(titre_val).lower().strip() in ('nan', 'none', ''):
                    return None  # No title → cannot classify → row will be dropped
                t = str(titre_val).lower().strip()
                # Pass 1: whole-word match (handles "villa 3 chambres" correctly)
                for key in _sorted_keys:
                    if re.search(r'\b' + re.escape(key) + r'\b', t):
                        return TYPE_MAP[key]
                # Pass 2: substring match (for keys like 's+2', 'rdc' that aren't full words)
                for key in _sorted_keys:
                    if key in t:
                        return TYPE_MAP[key]
                # Nothing found → return None so the row is dropped (type_bien=Unknown → supprimé)
                return None

            titre_col = 'titre' if 'titre' in raw.columns else (
                        'title' if 'title' in raw.columns else None)
            if titre_col:
                bnb_type = raw[titre_col].apply(_extract_type_from_titre)
            else:
                # No titre column — use description as fallback
                bnb_type = raw['description'].apply(_extract_type_from_titre) if 'description' in raw.columns else pd.Series([None] * len(raw))

            std = pd.DataFrame({
                'prix': raw['prix_montant'].apply(clean_price),
                'surface_m2': (raw['meta_Surface'].apply(clean_surface) if 'meta_Surface' in raw.columns else None),
                'ville': raw['localisation'].apply(extract_ville_from_localisation), 'gouvernorat': raw['localisation'].apply(clean_city),
                'type_bien': bnb_type,
                'type_transaction': 'Location Courte Duree',
                'chambres': (pd.to_numeric(raw['meta_Lits'], errors='coerce') if 'meta_Lits' in raw.columns else None),
                'pieces': None, 'description': raw['description'].apply(remove_noise),
                'image_url': raw['image'] if 'image' in raw.columns else None,
                'url': raw['url'], 'source': name, 'date_publication': date_pub,
            })
            n_typed = bnb_type.notna().sum()
            n_chambre = (bnb_type == 'Chambre').sum()
            log(f"    BnB type_bien extrait depuis titre : {n_typed}/{len(raw)} | Chambre={n_chambre} (fallback)")
        else:
            log(f"[SKIP] Source inconnue: {name}"); continue

        if 'type_bien' in std.columns:
            def _fix_apptype_row(row):
                tb_raw = row.get('type_bien')
                tb_new, pieces_val = extract_pieces_from_apptype(tb_raw)
                if tb_new is not None:
                    row_out = row.copy()
                    row_out['type_bien'] = tb_new
                    if pieces_val is not None and pd.isna(row_out.get('pieces', np.nan)):
                        row_out['pieces'] = pieces_val
                    return row_out
                return row
            std = std.apply(_fix_apptype_row, axis=1)

        all_dfs.append(std)
        log(f"OK  {name:<25}: {len(std):>6} annonces")

    if not all_dfs:
        log("Aucune source chargée — arrêt ETL")
        return pd.DataFrame(), 0

    df = pd.concat(all_dfs, ignore_index=True, sort=False)
    n0 = len(df)
    log(f"\nTotal brut fusionne : {n0}")
    return df, n0


# ================================================================
# DEDUPLICATION — 3 PASSES
# ================================================================

def deduplicate(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    section("ETAPE 2 — DEDUPLICATION")
    n_p1 = len(df)
    df   = df.drop_duplicates(keep='first')
    log(f"Passe 1 - Stricts                      : -{n_p1 - len(df)} supprimes")

    n_p2 = len(df)
    df   = pd.concat([
        df[df['url'].notna()].drop_duplicates(subset=['url'], keep='first'),
        df[df['url'].isna()],
    ], ignore_index=True)
    log(f"Passe 2 - Meme URL                     : -{n_p2 - len(df)} supprimes")

    n_p3  = len(df)
    df['_u'] = df['url'].fillna('').str.strip()
    df['_d'] = df['description'].fillna('').str.lower().str.strip()
    mask     = (df['_u'] != '') & (df['_d'] != '')
    df = pd.concat([
        df[mask].drop_duplicates(subset=['_u', '_d'], keep='first'),
        df[~mask],
    ], ignore_index=True).drop(columns=['_u', '_d'])
    log(f"Passe 3 - URL + Description identiques : -{n_p3 - len(df)} supprimes")

    n_apres = len(df)
    log(f"\n  Total apres deduplication : {n_apres:>7}")
    return df, n_apres


# ================================================================
# TYPE COERCION
# ================================================================

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    section("ETAPE 2b — VERIFICATION TYPES DE DONNEES")
    COLS_NUM = ['prix', 'surface_m2', 'chambres', 'pieces']
    COLS_CAT = ['gouvernorat', 'ville', 'type_bien', 'type_transaction', 'source']
    for col in COLS_NUM:
        if col in df.columns:
            before = df[col].dtype
            df[col] = pd.to_numeric(df[col], errors='coerce')
            tag = "[FIX]" if str(before) != str(df[col].dtype) else "[OK] "
            log(f"  {tag} {col}: {before} → {df[col].dtype}")
    if 'date_publication' in df.columns:
        df['date_publication'] = pd.to_datetime(df['date_publication'], errors='coerce')
    for col in COLS_CAT:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip().replace({'nan': None, 'None': None, '': None})
    log(f"\nDimensions : {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
    return df


# ================================================================
# STANDARDIZATION
# ================================================================

def standardize(df: pd.DataFrame) -> pd.DataFrame:
    section("ETAPE 3 — STANDARDISATION")

    df['type_transaction'] = df['type_transaction'].apply(std_transaction)
    df['type_transaction'] = df.apply(
        lambda r: resolve_transaction(r.get('prix'), r.get('type_transaction'), r.get('description')), axis=1)
    tt_mode = df['type_transaction'].mode()
    if len(tt_mode) > 0:
        df['type_transaction'] = df['type_transaction'].fillna(int(tt_mode.iloc[0]))
    df['type_transaction'] = df['type_transaction'].astype(int)
    log(f"  type_transaction : {df['type_transaction'].value_counts().sort_index().to_dict()}")

    df['type_bien'] = df.apply(
        lambda r: std_type_bien(r.get('type_bien'), r.get('surface_m2'), r.get('pieces')), axis=1)
    df['type_bien'] = df.apply(refine_autre, axis=1)

    # nb_pieces : extraction depuis description uniquement, pas d'imputation
    if 'pieces'   not in df.columns: df['pieces']   = np.nan
    if 'chambres' not in df.columns: df['chambres'] = np.nan
    df['nb_pieces'] = df['pieces'].combine_first(df['chambres'])
    mask_na = df['nb_pieces'].isna()
    if mask_na.sum() > 0 and 'description' in df.columns:
        extracted = df.loc[mask_na, 'description'].apply(extract_nb_pieces_from_desc)
        df.loc[mask_na, 'nb_pieces'] = extracted
        log(f"  nb_pieces extraits description : {extracted.notna().sum()} / {mask_na.sum()} NA")
    # Remaining NA for nb_pieces → kept as NaN (not imputed)
    log(f"  nb_pieces NA restants (conservés) : {df['nb_pieces'].isna().sum()}")

    log("  Normalisation gouvernorat...")
    df['_gouvernorat_str'] = df['gouvernorat'].apply(normalize_gouvernorat)
    log(f"  Taux Unknown : {(df['_gouvernorat_str'] == 'Unknown').mean() * 100:.1f}%")
    return df


# ================================================================
# SEGMENTATION
# ================================================================

def classify_row(row) -> str:
    tb   = str(row.get('type_bien', '') or '').lower()
    desc = str(row.get('description', '') or '').lower()
    # BnB source: let type_bien drive segmentation (extracted from titre)
    for typ, kws in TYPE_KW.items():
        for kw in kws:
            if kw in tb: return typ
    if not tb or tb in ('none', 'nan', ''):
        for typ, kws in DESC_KW.items():
            for kw in kws:
                if kw in desc: return typ
    return 'Divers'


def segment(df: pd.DataFrame) -> pd.DataFrame:
    section("ETAPE 4 — SEGMENTATION EN 4 GROUPES")
    df['type_categorise'] = df.apply(classify_row, axis=1)
    df['groupe']          = df['type_categorise'].map(GROUPE_MAP)
    for g, c in df['groupe'].value_counts().items():
        log(f"  {g:<15}: {c:>6} ({c / len(df) * 100:.1f}%)")
    return df


# ================================================================
# CLEANING PER GROUP
# IMPORTANT: rows with invalid prix/surface are DROPPED, not imputed
# ================================================================

def clean_groups(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Per group:
      1. Drop rows where prix=NaN — no price = unusable for pricing model (target missing).
      2. Drop rows where prix is clearly illogical — separate thresholds for
         LOCATION (loyer mensuel) and VENTE (prix d'achat).
         e.g. 800 TND/month rent is valid; 800 TND sale price is absurd.
      3. Drop rows where surface_m2 is illogical (below floor) — NaN kept, will be imputed.
      4. Drop rows where prix/m² > 100,000 TND/m² on VENTE (data entry errors).
      5. Anomaly detection via Isolation Forest on (prix, surface) pairs,
         run separately per transaction type (location vs vente).
      6. NLP keyword extraction from descriptions.
    """
    section("ETAPE 5 — NETTOYAGE + ISOLATION FOREST + NLP")
    groupes_clean: dict[str, pd.DataFrame] = {}

    for groupe in ['Residentiel', 'Foncier', 'Commercial', 'Divers']:
        dg = df[df['groupe'] == groupe].copy()
        if len(dg) == 0:
            continue
        n_avant = len(dg)
        s = SEUILS[groupe]

        # ── Prix NaN → ligne supprimée ──────────────────────────────
        # Objectif = estimation de prix réels.
        # Une annonce sans prix ne peut pas servir de target ni d'input fiable.
        n_prix_nan = dg['prix'].isna().sum()
        dg = dg[dg['prix'].notna()].copy()
        if n_prix_nan > 0:
            log(f"  [{groupe}] prix=NaN supprimés : {n_prix_nan}")

        # ── Prix invalides — seuils séparés location / vente ────────
        # Ligne LOCATION (type_transaction == 1) : seuils loyer mensuel
        # Ligne VENTE     (type_transaction == 2) : seuils prix d'achat

        is_loc  = dg['type_transaction'] == 1
        is_vent = dg['type_transaction'] == 2
        prix_ok = dg['prix'].notna()  # always True here (NaN already dropped above)

        mask_loc_invalid = prix_ok & is_loc & (
            (dg['prix'] < s['prix_min_loc']) | (dg['prix'] > s['prix_max_loc'])
        )
        mask_vent_invalid = prix_ok & ~is_loc & (
            (dg['prix'] < s['prix_min_vent']) | (dg['prix'] > s['prix_max_vent'])
        )
        mask_prix_invalid = mask_loc_invalid | mask_vent_invalid
        n_prix_drop = mask_prix_invalid.sum()
        dg = dg[~mask_prix_invalid]

        # ── Surface invalide ─────────────────────────────────────────
        mask_surf_invalid = dg['surface_m2'].notna() & (
            (dg['surface_m2'] < s['surf_min']) | (dg['surface_m2'] > s['surf_max'])
        )
        n_surf_drop = mask_surf_invalid.sum()
        dg = dg[~mask_surf_invalid]

        # ── Prix/m² absurde — uniquement pour les VENTES ────────────
        # Pour les locations, prix/m² n'a pas de sens (loyer ≠ valeur vénale)
        dg['_prix_m2'] = np.where(
            dg['prix'].notna() & dg['surface_m2'].notna() &
            (dg['surface_m2'] > 0) & (dg['type_transaction'] == 2),
            dg['prix'] / dg['surface_m2'],
            np.nan,
        )
        n_pm2_drop = (dg['_prix_m2'] > 100_000).sum()
        dg = dg[~(dg['_prix_m2'] > 100_000)]

        # ── Isolation Forest — séparé par type_transaction ──────────
        # On lance IF séparément sur locations et ventes pour ne pas
        # mélanger des échelles de prix complètement différentes.
        n_anom = 0
        from sklearn.preprocessing import StandardScaler
        for tt in [1, 2]:
            mask_tt  = dg['type_transaction'] == tt
            feats_tt = dg.loc[mask_tt, ['prix', 'surface_m2']].dropna()
            if len(feats_tt) >= 50:
                X     = StandardScaler().fit_transform(feats_tt)
                preds = IsolationForest(contamination=0.04, random_state=42).fit_predict(X)
                anoms = feats_tt.index[preds == -1]
                dg    = dg.drop(index=anoms)
                n_anom += len(anoms)

        # ── NLP keyword features ─────────────────────────────────────
        if 'description' in dg.columns:
            desc = dg['description'].fillna('').str.lower()
            for feat, kws in NLP_KW.items():
                dg[feat] = desc.str.contains('|'.join(kws), na=False)

        dg = dg.drop(columns=['_prix_m2'], errors='ignore')
        groupes_clean[groupe] = dg

        log(f"  {groupe:<15}: {n_avant:>6} → {len(dg):>6} | "
            f"prix_invalides={n_prix_drop} "
            f"(loc={mask_loc_invalid.sum()} vent={mask_vent_invalid.sum()}) "
            f"surf_invalides={n_surf_drop} prix_m2_absurde={n_pm2_drop} IF={n_anom}")

    return groupes_clean


# ================================================================
# MISSING VALUE HANDLING
#
# Règles (après suppression des prix NaN en étape 5) :
#   surface_m2      → imputation médiane par groupe
#   nb_pieces       → imputation médiane par groupe
#   type_categorise → imputation par mode
#   colonnes >80% NA → supprimées (sauf colonnes protégées)
# Note: prix NaN déjà supprimés en étape 5 (clean_groups)
# ================================================================

def handle_missing(groupes_clean: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    section("ETAPE 5b — GESTION DES VALEURS MANQUANTES")
    log("  surface_m2      : imputation médiane par groupe")
    log("  nb_pieces       : imputation régression surface×type_bien (médiane conditionnelle)")
    log("  type_categorise : imputation par mode")
    log("  prix            : déjà traité (NaN supprimés en étape 5)")

    COLS_PROTEGEES = {'prix', 'gouvernorat', 'type_bien', 'image_url'}
    SEUIL_DROP     = 0.80

    for groupe, dg in groupes_clean.items():
        n = len(dg)
        log(f"\n  [{groupe}] — {n} annonces")

        # Drop columns with >80% NA (except protected)
        cols_drop = [col for col in dg.columns
                     if dg[col].isna().mean() > SEUIL_DROP and col not in COLS_PROTEGEES]
        if cols_drop:
            dg = dg.drop(columns=cols_drop, errors='ignore')
            log(f"    Colonnes supprimées (>{SEUIL_DROP*100:.0f}% NA) : {cols_drop}")

        # ── surface_m2 : imputation médiane ──────────────────────
        if 'surface_m2' in dg.columns:
            n_na = dg['surface_m2'].isna().sum()
            if n_na > 0:
                med = dg['surface_m2'].median()
                if not pd.isna(med):
                    dg['surface_m2'] = dg['surface_m2'].fillna(med)
                    log(f"    surface_m2 NA : {n_na} → imputés médiane={med:.1f} m²")
                else:
                    log(f"    surface_m2 NA : {n_na} → médiane indisponible, conservés")

        # ── nb_pieces : imputation par régression surface×type_bien ──
        # Meilleure que la médiane globale car :
        #   - un appartement 50m²  → ~2 pièces
        #   - une villa 300m²       → ~5 pièces
        #   - une maison 150m²      → ~4 pièces
        # On utilise la médiane conditionnelle (surface_cat, type_bien)
        # calculée depuis les annonces connues du MÊME groupe.
        if 'nb_pieces' in dg.columns:
            n_na = dg['nb_pieces'].isna().sum()
            if n_na > 0 and 'surface_m2' in dg.columns:
                known = dg[dg['nb_pieces'].notna()].copy()
                if len(known) >= 20:
                    # Catégories de surface calibrées sur marché tunisien
                    bins   = [0, 40, 70, 100, 140, 200, 300, 500, 99999]
                    labels = [1,  2,  3,   4,   5,   6,   7,    8]
                    known['_scat'] = pd.cut(known['surface_m2'], bins=bins, labels=labels)
                    dg['_scat']    = pd.cut(dg['surface_m2'],    bins=bins, labels=labels)
                    # Table de lookup : (type_bien, surf_cat) → médiane nb_pieces
                    has_type = 'type_bien' in dg.columns and dg['type_bien'].notna().any()
                    if has_type:
                        lookup = (known.groupby(['type_bien','_scat'], observed=True)['nb_pieces']
                                  .median().round().astype(int))
                    else:
                        lookup = known.groupby('_scat', observed=True)['nb_pieces'].median().round().astype(int)
                    # Imputer chaque NaN depuis la table de lookup
                    n_reg = 0
                    fallback_med = known['nb_pieces'].median()
                    for idx in dg[dg['nb_pieces'].isna()].index:
                        scat = dg.at[idx, '_scat']
                        if has_type:
                            tb = dg.at[idx, 'type_bien']
                            val = lookup.get((tb, scat), lookup.xs(scat, level='_scat').median()
                                            if scat in lookup.index.get_level_values('_scat')
                                            else fallback_med)
                        else:
                            val = lookup.get(scat, fallback_med)
                        dg.at[idx, 'nb_pieces'] = round(float(val)) if not pd.isna(val) else fallback_med
                        n_reg += 1
                    dg = dg.drop(columns=['_scat'], errors='ignore')
                    log(f"    nb_pieces NA : {n_na} → imputés par régression surface×type_bien ({n_reg} corrigés)")
                else:
                    # Pas assez d'annonces connues → médiane de groupe
                    med_pieces = dg['nb_pieces'].median()
                    if not pd.isna(med_pieces):
                        dg['nb_pieces'] = dg['nb_pieces'].fillna(med_pieces)
                        log(f"    nb_pieces NA : {n_na} → imputés médiane groupe={med_pieces:.1f}")
            elif 'nb_pieces' in dg.columns and n_na > 0:
                med_pieces = dg['nb_pieces'].median()
                if not pd.isna(med_pieces):
                    dg['nb_pieces'] = dg['nb_pieces'].fillna(med_pieces)
                    log(f"    nb_pieces NA : {n_na} → imputés médiane={med_pieces:.1f}")

        # ── type_categorise : imputation mode ────────────────────
        if 'type_categorise' in dg.columns:
            n_na = dg['type_categorise'].isna().sum()
            if n_na > 0:
                mode_s   = dg['type_categorise'].mode()
                fill_val = mode_s.iloc[0] if len(mode_s) > 0 else 'Divers'
                dg['type_categorise'] = dg['type_categorise'].fillna(fill_val)
                log(f"    type_categorise mode='{fill_val}' : {n_na} NA comblés")

        groupes_clean[groupe] = dg

    return groupes_clean


# ================================================================
# CATEGORICAL ENCODING
# After encoding, rows where gouvernorat=0 (Unknown) or type_bien=0 (Unknown)
# are DROPPED — these are records we cannot reliably place or classify.
# ================================================================

def encode_categorical(
    df: pd.DataFrame,
    groupes_clean: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    section("ETAPE 5d — ENCODAGE MANUEL + SUPPRESSION Unknown")
    log("  Règle : lignes avec gouvernorat=Unknown ou type_bien=Unknown → SUPPRIMÉES")

    # ── Gouvernorat encoding ──────────────────────────────────────
    for groupe, dg in groupes_clean.items():
        if '_gouvernorat_str' not in dg.columns:
            valid_idx = [i for i in dg.index if i in df.index]
            if valid_idx and '_gouvernorat_str' in df.columns:
                dg.loc[valid_idx, '_gouvernorat_str'] = df.loc[valid_idx, '_gouvernorat_str'].values
            else:
                dg['_gouvernorat_str'] = dg['gouvernorat'].apply(normalize_gouvernorat)
        dg['gouvernorat'] = dg['_gouvernorat_str'].apply(encode_gouvernorat)
        n_unk = (dg['_gouvernorat_str'] == 'Unknown').sum()
        log(f"  [{groupe}] gouvernorat encodé | Unknown={n_unk} (seront supprimés après géo-correction)")
        groupes_clean[groupe] = dg

    df['gouvernorat'] = df['_gouvernorat_str'].apply(encode_gouvernorat)

    # ── type_bien encoding ────────────────────────────────────────
    df['type_bien'] = df['type_bien'].apply(
        lambda x: encode_type_bien(x) if not isinstance(x, (int, float)) or pd.isna(x) else x)
    if df['type_bien'].dtype == object:
        df['type_bien'] = df['type_bien'].apply(encode_type_bien)

    for groupe, dg in groupes_clean.items():
        if 'type_bien' in dg.columns and dg['type_bien'].dtype == object:
            dg['type_bien'] = dg['type_bien'].apply(encode_type_bien)
        elif 'type_bien' in dg.columns:
            dg['type_bien'] = dg['type_bien'].apply(
                lambda x: TYPE_BIEN_ENC.get(str(x), 0) if not str(x).lstrip('-').isdigit() else int(x))

        # Drop rows with type_bien = 0 (Unknown / unclassifiable)
        n_before = len(dg)
        dg = dg[dg['type_bien'] != 0]
        n_dropped_tb = n_before - len(dg)
        if n_dropped_tb > 0:
            log(f"  [{groupe}] type_bien=Unknown supprimé : {n_dropped_tb} lignes")

        nlp_cols = [c for c in dg.columns if c.startswith('nlp_')]
        if nlp_cols:
            dg[nlp_cols] = dg[nlp_cols].astype(int)
        groupes_clean[groupe] = dg

    return df, groupes_clean



# ================================================================
# VILLE ENCODING — frequency-based
# ================================================================

def encode_ville(
    df,
    groupes_clean,
    min_freq: int = 30,
):
    """
    Encodage hiérarchique ville_encoded = gouvernorat * 1000 + rang_interne

    Format :
      GOV * 1000 + rang   → ville connue dans ce gouvernorat
                            rang = 1 (plus fréquente), 2, 3...
      GOV * 1000          → ville rare ou inconnue dans ce gouvernorat
      0                   → gouvernorat inconnu (ne devrait pas arriver après étape 6b)

    Exemples concrets :
      tunis       (gov=23, rang 1) → 23001
      la marsa    (gov=23, rang 2) → 23002
      gammarth    (gov=23, rang 5) → 23005
      ville rare dans Tunis        → 23000
      sfax        (gov=17, rang 1) → 17001
      ville rare dans Sfax         → 17000
      nabeul      (gov=16, rang 1) → 16001

    Avantages :
      - Le modèle peut inférer que 23001, 23002, 23005 sont dans Tunis (23xxx)
      - Cohérence totale avec la colonne gouvernorat
      - Les villes rares sont groupées par région (pas perdues, pas overfittées)
      - Décodage immédiat : 23002 → gouvernorat 23 (Tunis), rang 2
    """
    section("ETAPE 5e — ENCODAGE VILLE (hierarchique: gov*1000 + rang)")

    # ── Construire le rang par gouvernorat ───────────────────────
    # 1. Normalise ville_key
    df["_ville_key"] = df["ville"].fillna("").str.lower().str.strip()

    # 2. Résoudre gouvernorat_int depuis la colonne gouvernorat (déjà encodée int)
    #    Si gouvernorat est string → encoder, sinon utiliser directement
    def _gov_int(val):
        if pd.isna(val): return 0
        try: return int(val)
        except: return 0

    df["_gov_int"] = df["gouvernorat"].apply(_gov_int)

    # 3. Compter les annonces par (gouvernorat, ville) → rang interne
    grp = (df.groupby(["_gov_int", "_ville_key"])
             .size()
             .reset_index(name="_cnt"))
    grp = grp[grp["_ville_key"] != ""]  # exclure villes vides
    grp = grp[grp["_gov_int"] > 0]      # exclure gouvernorat inconnu

    # 4. Pour chaque gouvernorat, trier par fréquence → rang 1,2,3...
    #    Uniquement les villes avec >= min_freq annonces reçoivent un rang individuel
    grp = grp.sort_values(["_gov_int", "_cnt"], ascending=[True, False])
    grp["_rang"] = grp.groupby("_gov_int").cumcount() + 1

    # Villes rares (< min_freq) → rang = 0 (groupe "ville_rare_gouvernorat")
    grp.loc[grp["_cnt"] < min_freq, "_rang"] = 0

    # 5. Build lookup dict {(gov_int, ville_key): ville_encoded}
    rank_map: dict[tuple, int] = {}
    ville_label_map: dict[tuple, str] = {}
    for _, row in grp.iterrows():
        gov  = int(row["_gov_int"])
        vk   = row["_ville_key"]
        rang = int(row["_rang"])
        code = gov * 1000 + rang
        rank_map[(gov, vk)] = code
        ville_label_map[(gov, vk)] = vk

    # 6. Encode
    def _enc(ville_val, gouvernorat_code):
        gov = _gov_int(gouvernorat_code)
        if gov == 0:
            return 0  # gouvernorat inconnu → code 0
        vk = str(ville_val or "").lower().strip()
        if not vk or vk in ("nan", "none", ""):
            return gov * 1000  # ville absente → groupe rare du gouvernorat
        key = (gov, vk)
        if key in rank_map:
            return rank_map[key]
        # Ville non vue → groupe rare du gouvernorat
        return gov * 1000

    df["ville_encoded"] = df.apply(
        lambda r: _enc(r.get("ville"), r.get("gouvernorat")), axis=1
    )

    # Propagate to groupes_clean
    for groupe, dg in groupes_clean.items():
        valid_idx = [i for i in dg.index if i in df.index]
        if valid_idx and "ville_encoded" in df.columns:
            dg.loc[valid_idx, "ville_encoded"] = df.loc[valid_idx, "ville_encoded"].values
        else:
            dg["ville_encoded"] = dg.apply(
                lambda r: _enc(r.get("ville"), r.get("gouvernorat")), axis=1
            )
        groupes_clean[groupe] = dg

    # Cleanup temp columns
    df = df.drop(columns=["_ville_key", "_gov_int"], errors="ignore")

    # Stats
    n_freq = (grp["_rang"] > 0).sum()
    n_rare = (grp["_rang"] == 0).sum()
    encoded_pct = (df["ville_encoded"] > 0).mean() * 100
    log(f"  Villes frequentes (>={min_freq}) : {n_freq} codes individuels")
    log(f"  Villes rares (<{min_freq})        : {n_rare} → groupees par gouvernorat (GOV*1000)")
    log(f"  ville_encoded : {encoded_pct:.1f}% lignes avec code > 0")

    # Show examples
    examples = []
    for (gov, vk), code in list(rank_map.items())[:8]:
        if code % 1000 > 0:
            from mappings import GOUVERNORAT_DEC
            gov_name = GOUVERNORAT_DEC.get(gov, str(gov))
            examples.append(f"{vk}({gov_name})={code}")
    log(f"  Exemples : {' | '.join(examples)}")

    # Return flat rank_map {ville_key: code} for encoding_mappings.json
    flat_rank_map = {vk: code for (gov, vk), code in rank_map.items()}
    return df, groupes_clean, flat_rank_map, rank_map


# ================================================================
# GEOCODING
# ================================================================

CENTROIDES: dict[str, tuple[float, float]] = {
    'tunis': (36.8190, 10.1658), 'ariana': (36.8665, 10.1647),
    'ben arous': (36.7533, 10.2283), 'manouba': (36.8100, 10.0972),
    'nabeul': (36.4561, 10.7376), 'sousse': (35.8245, 10.6346),
    'sfax': (34.7405, 10.7603), 'monastir': (35.7643, 10.8113),
    'mahdia': (35.5047, 11.0622), 'bizerte': (37.2744, 9.8739),
    'beja': (36.7256, 9.1817), 'jendouba': (36.5011, 8.7757),
    'zaghouan': (36.4029, 10.1429), 'siliana': (36.0850, 9.3708),
    'kairouan': (35.6781, 10.0963), 'kasserine': (35.1676, 8.8365),
    'sidi bouzid': (35.0382, 9.4850), 'gabes': (33.8881, 10.0975),
    'medenine': (33.3549, 10.5055), 'tataouine': (32.9211, 10.4519),
    'gafsa': (34.4250, 8.7842), 'tozeur': (33.9197, 8.1335),
    'kebili': (33.7052, 8.9691), 'la marsa': (36.8769, 10.3247),
    'la soukra': (36.8990, 10.2358), 'el menzah': (36.8440, 10.1920),
    'le bardo': (36.8094, 10.1414), 'hammam lif': (36.7293, 10.3349),
    'hammam sousse': (35.8604, 10.5953), 'msaken': (35.7306, 10.5779),
    'el aouina': (36.8451, 10.2272), 'ennasr': (36.8610, 10.1950),
    'carthage': (36.8528, 10.3247), 'sidi bou said': (36.8694, 10.3406),
    'gammarth': (36.9100, 10.2900), 'megrine': (36.7700, 10.2300),
}

GOV_CENTROIDS: dict[str, tuple[float, float]] = {
    'Tunis': (36.8190, 10.1658), 'Ariana': (36.8665, 10.1647),
    'Ben Arous': (36.7533, 10.2283), 'Manouba': (36.8100, 10.0972),
    'Nabeul': (36.4561, 10.7376), 'Zaghouan': (36.4029, 10.1429),
    'Bizerte': (37.2744, 9.8739), 'Béja': (36.7256, 9.1817),
    'Jendouba': (36.5011, 8.7757), 'Le Kef': (36.1744, 8.7148),
    'Siliana': (36.0850, 9.3708), 'Sousse': (35.8245, 10.6346),
    'Monastir': (35.7643, 10.8113), 'Mahdia': (35.5047, 11.0622),
    'Kairouan': (35.6781, 10.0963), 'Kasserine': (35.1676, 8.8365),
    'Sidi Bouzid': (35.0382, 9.4850), 'Sfax': (34.7405, 10.7603),
    'Gabès': (33.8881, 10.0975), 'Médenine': (33.3549, 10.5055),
    'Tataouine': (32.9211, 10.4519), 'Gafsa': (34.4250, 8.7842),
    'Tozeur': (33.9197, 8.1335), 'Kébili': (33.7052, 8.9691),
}

_gov_names = list(GOV_CENTROIDS.keys())
_gov_lats  = np.array([GOV_CENTROIDS[g][0] for g in _gov_names])
_gov_lons  = np.array([GOV_CENTROIDS[g][1] for g in _gov_names])
_KM_PER_LAT = 111.0
_KM_PER_LON = 111.0 * math.cos(math.radians(34.0))


def build_gouvernorat_coords(satellite_dir: str) -> dict[str, tuple[float, float]]:
    _FALLBACK = {'Unknown': (36.8, 10.1), **GOV_CENTROIDS}
    buildings_path = f'{satellite_dir}/tunisia_buildings_20260215_133916.csv'
    if not os.path.exists(buildings_path):
        log("[WARN] buildings.csv manquant → fallback coords manuelles")
        return _FALLBACK
    df_build = pd.read_csv(buildings_path)
    df_build['gouvernorat_norm'] = (
        df_build['city'].str.lower().str.strip()
        .map(VILLE_TO_GOUVERNORAT).fillna('Unknown'))
    coords_df = (
        df_build[df_build['gouvernorat_norm'] != 'Unknown']
        .groupby('gouvernorat_norm')[['lat', 'lon']].median().reset_index())
    coords = {row['gouvernorat_norm']: (round(row['lat'], 4), round(row['lon'], 4))
              for _, row in coords_df.iterrows()}
    for gov, coord in _FALLBACK.items():
        if gov not in coords:
            coords[gov] = coord
    log(f"  ✔ GOUVERNORAT_COORDS : {len(coords)} gouvernorats")
    return coords


def assign_latlon(ville, gouvernorat_str, gouvernorat_coords):
    vk = str(ville or '').lower().strip()
    gk = str(gouvernorat_str or '').strip()
    if vk in CENTROIDES: return CENTROIDES[vk]
    if gk in gouvernorat_coords: return gouvernorat_coords[gk]
    return gouvernorat_coords.get('Unknown', (36.8, 10.1))


def geocode(df: pd.DataFrame, gouvernorat_coords: dict) -> pd.DataFrame:
    section("ETAPE 6 — GEOCODAGE")
    latlon    = df.apply(lambda r: assign_latlon(r.get('ville'), r.get('_gouvernorat_str'), gouvernorat_coords), axis=1)
    df['lat'] = latlon.apply(lambda x: x[0])
    df['lon'] = latlon.apply(lambda x: x[1])
    df['lat'] = df['lat'].fillna(36.8)
    df['lon'] = df['lon'].fillna(10.1)
    log(f"Geocodage : {df['lat'].notna().mean() * 100:.1f}% annonces avec lat/lon")
    return df


def reverse_geocode_correction(
    df: pd.DataFrame,
    groupes_clean: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    # Attempts to resolve Unknown gouvernorat via nearest centroid.
    # After correction, remaining Unknown rows are DROPPED.
    section("ETAPE 6b — CORRECTION GOUVERNORAT + SUPPRESSION Unknown restants")
    mask_unknown    = df['_gouvernorat_str'] == 'Unknown'
    n_unknown_avant = mask_unknown.sum()
    log(f"  Lignes Unknown avant correction : {n_unknown_avant} ({n_unknown_avant / len(df) * 100:.1f}%)")

    if n_unknown_avant > 0:
        lats_unk = df.loc[mask_unknown, 'lat'].values
        lons_unk = df.loc[mask_unknown, 'lon'].values
        dlat_mat  = (lats_unk[:, None] - _gov_lats[None, :]) * _KM_PER_LAT
        dlon_mat  = (lons_unk[:, None] - _gov_lons[None, :]) * _KM_PER_LON
        dist_mat  = np.sqrt(dlat_mat ** 2 + dlon_mat ** 2)
        best_idx  = np.argmin(dist_mat, axis=1)
        best_gov  = [_gov_names[i] for i in best_idx]

        df.loc[mask_unknown, '_gouvernorat_str'] = best_gov
        df.loc[mask_unknown, 'gouvernorat'] = (
            pd.Series(best_gov, index=df.loc[mask_unknown].index)
            .map(GOUVERNORAT_ENC).fillna(0).astype(int))

        for groupe, dg in groupes_clean.items():
            grp_idx = [i for i in dg.index if i in df.index and mask_unknown[i]]
            if grp_idx:
                dg.loc[grp_idx, '_gouvernorat_str'] = df.loc[grp_idx, '_gouvernorat_str']
                dg.loc[grp_idx, 'gouvernorat']      = df.loc[grp_idx, 'gouvernorat']
                groupes_clean[groupe] = dg

        n_apres = (df['_gouvernorat_str'] == 'Unknown').sum()
        log(f"  ✔ {n_unknown_avant - n_apres} corrigés par géo-inverse")
        log(f"  Unknown restants après correction : {n_apres}")
    else:
        log("  ✔ Aucun Unknown — correction non nécessaire")

    # DROP lignes avec gouvernorat=0 (Unknown non corrigeable)
    n_before = len(df)
    df = df[df['gouvernorat'] != 0].copy()
    n_dropped = n_before - len(df)
    if n_dropped > 0:
        log(f"  Lignes gouvernorat=Unknown supprimées de df : {n_dropped}")

    for groupe, dg in groupes_clean.items():
        n_grp = len(dg)
        if 'gouvernorat' in dg.columns:
            dg = dg[dg['gouvernorat'] != 0].copy()
        n_drop_grp = n_grp - len(dg)
        if n_drop_grp > 0:
            log(f"  [{groupe}] gouvernorat=Unknown supprimé : {n_drop_grp} lignes")
        groupes_clean[groupe] = dg

    total_after = sum(len(dg) for dg in groupes_clean.values())
    log(f"  ✔ Total après suppression Unknown : {total_after:,} annonces")
    return df, groupes_clean
