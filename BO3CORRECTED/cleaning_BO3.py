"""
cleaning_BO3.py — ETL, nettoyage et segmentation pour l'Objectif 3.

MEME LOGIQUE QUE BO2 cleaning.py :
  - Memes seuils prix (loc/vent separes — depuis mappings_BO3.SEUILS)
  - Meme imputation surface_m2 (mediane par groupe dans handle_missing)
  - Meme Isolation Forest (separe par type_transaction, contamination=0.04)
  - Meme handle_missing()
  - Meme encode_categorical()

Specificites BO3 (ajouts pour series temporelles) :
  - date_publication extraite et redistribuee (ARIMA a besoin de dates)
  - prix_m2 calcule apres imputation surface (TARGET BO3)
  - ville_encoded identique BO2
"""

import os, re, warnings
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings('ignore')

from mappings_BO3 import (
    GOUVERNORAT_ENC, VILLE_TO_GOUVERNORAT,
    SEUILS, TYPE_MAP, TYPE_KW, DESC_KW, GROUPE_MAP,
    LOC_KEYWORDS, VENTE_KEYWORDS,
    HIGH_SEASON_MONTHS, DATE_DISTRIBUTION,
)

def section(t): print("\n" + "="*65 + f"\n   {t}\n" + "="*65)
def log(m):     print(f"  {m}")

CURRENT_YEAR = 2026
MIN_YEAR     = 2022
SOURCES_DATE_REELLE = {'Tayara', 'Tunisie Annonces', 'BnB', 'Facebook Marketplace'}
SOURCES_SANS_DATE   = {'Mubawab', 'Mubawab Partial', 'Century21', 'HomeInTunisia'}


# ================================================================
# HELPERS
# ================================================================

def remove_noise(val):
    if pd.isna(val): return None
    val = str(val).encode('ascii', 'ignore').decode('ascii')
    val = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', val)
    return re.sub(r'\s+', ' ', val).strip() or None

def clean_price(v):
    if pd.isna(v): return None
    s = re.sub(r'[^\d.]', '', str(v).replace(',', '.'))
    try: return float(s) if s else None
    except: return None

def clean_surface(v):
    if pd.isna(v): return None
    s = re.sub(r'[^\d.]', '', str(v).replace(',', '.'))
    try: return float(s) if s else None
    except: return None

def clean_city(val):
    if pd.isna(val): return None
    val = remove_noise(val)
    if not val: return None
    for prefix in ['location ', 'vente ', 'location', 'vente']:
        if val.lower().startswith(prefix):
            val = val[len(prefix):].strip()
            break
    val = re.sub(r'\s*\d+\s*$', '', val).strip()
    return val.strip().title() if val else None

def _parse_date(val):
    if pd.isna(val): return pd.NaT
    val = str(val).strip()
    for fmt in ['%d/%m/%Y %H:%M','%d/%m/%Y','%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M','%Y-%m-%d','%d-%m-%Y']:
        try: return datetime.strptime(val[:19], fmt)
        except: pass
    return pd.to_datetime(val, dayfirst=True, errors='coerce')

def _date_from_facebook(img_url):
    if pd.isna(img_url): return pd.NaT
    m = re.search(r'oe=([0-9A-Fa-f]{8})', str(img_url))
    if m:
        try:
            ts = int(m.group(1), 16)
            return datetime.fromtimestamp(ts) - pd.Timedelta(days=90)
        except: pass
    return pd.NaT

def _date_from_filename(path):
    m = re.search(r'(\d{8})', str(path))
    if m:
        try: return datetime.strptime(m.group(1), '%Y%m%d')
        except: pass
    return datetime(CURRENT_YEAR, 2, 1)

def _date_from_bnb(img_url):
    if pd.isna(img_url): return pd.NaT
    m = re.search(r'/uploads/(\d{4})/(\d{2})/', str(img_url))
    if m:
        try: return datetime(int(m.group(1)), int(m.group(2)), 1)
        except: pass
    return pd.NaT

def _std_transaction(val):
    if pd.isna(val): return np.nan
    v = str(val).lower().strip()
    if any(x in v for x in ['location','locat','rent','louer','loyer','mensuel']): return 1
    if any(x in v for x in ['vent','sale','achat','cession','vendre']): return 2
    return np.nan

def normalize_gouvernorat(val):
    if pd.isna(val) or str(val).strip() in ('','nan','None','NaN'): return 'Unknown'
    v = str(val).strip().lower()
    if v in VILLE_TO_GOUVERNORAT: return VILLE_TO_GOUVERNORAT[v]
    for key, gov in VILLE_TO_GOUVERNORAT.items():
        if key in v or v in key: return gov
    for gov_name in GOUVERNORAT_ENC:
        if gov_name.lower() == v: return gov_name
    return 'Unknown'

def encode_gouvernorat(gov_str):
    return GOUVERNORAT_ENC.get(str(gov_str).strip(), 0)


# ================================================================
# ETAPE 1 — ETL
# ================================================================

def load_sources(sources):
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

        def _g(col, default=None):
            return raw[col] if col in raw.columns else pd.Series([default]*len(raw))

        if name == 'Facebook Marketplace':
            img_col = 'image_url' if 'image_url' in raw.columns else None
            date_pub = raw[img_col].apply(_date_from_facebook) if img_col else pd.Series([_date_from_filename(path)]*len(raw))
            std = pd.DataFrame({'prix': raw['prix_affiche'].apply(clean_price), 'surface_m2': None,
                'ville': _g('ville').apply(clean_city), 'gouvernorat': _g('gouvernorat').apply(clean_city),
                'type_bien': _g('type_bien').apply(remove_noise), 'description': _g('description').apply(remove_noise),
                'type_transaction': raw['type_annonce'].apply(remove_noise), 'source': name, 'date_publication': date_pub})

        elif name == 'Mubawab':
            date_pub = pd.Series([_date_from_filename(path)]*len(raw))
            std = pd.DataFrame({'prix': raw['prix_tnd'].apply(clean_price), 'surface_m2': raw['surface_m2'].apply(clean_surface),
                'ville': _g('ville').apply(clean_city), 'gouvernorat': _g('gouvernorat').apply(clean_city),
                'type_bien': _g('type_bien').apply(remove_noise), 'description': _g('titre').apply(remove_noise),
                'type_transaction': _g('categorie').apply(remove_noise), 'source': name, 'date_publication': date_pub})

        elif name == 'Mubawab Partial':
            date_pub = raw['date_collecte'].apply(_parse_date) if 'date_collecte' in raw.columns else pd.Series([_date_from_filename(path)]*len(raw))
            desc = raw['description'] if 'description' in raw.columns else (raw['titre'] if 'titre' in raw.columns else pd.Series([None]*len(raw)))
            std = pd.DataFrame({'prix': raw['prix'].apply(clean_price), 'surface_m2': raw['surface_m2'].apply(clean_surface),
                'ville': _g('ville').apply(clean_city), 'gouvernorat': _g('ville').apply(clean_city),
                'type_bien': _g('type_propriete').apply(remove_noise), 'description': desc.apply(remove_noise),
                'type_transaction': None, 'source': name, 'date_publication': date_pub})

        elif name == 'Tayara':
            date_pub = raw['scraped_at'].apply(_parse_date) if 'scraped_at' in raw.columns else pd.Series([_date_from_filename(path)]*len(raw))
            std = pd.DataFrame({'prix': raw['price'].apply(clean_price), 'surface_m2': _g('superficie').apply(clean_surface),
                'ville': _g('region').apply(clean_city), 'gouvernorat': _g('location').apply(clean_city),
                'type_bien': _g('category').apply(remove_noise), 'description': _g('description').apply(remove_noise),
                'type_transaction': _g('type_de_transaction').apply(remove_noise), 'source': name, 'date_publication': date_pub})

        elif name == 'Tunisie Annonces':
            date_pub = raw['date_insertion'].apply(_parse_date) if 'date_insertion' in raw.columns else pd.Series([pd.NaT]*len(raw))
            std = pd.DataFrame({'prix': raw['prix_montant'].apply(clean_price), 'surface_m2': _g('surface_couverte').apply(clean_surface),
                'ville': _g('gouvernorat').apply(clean_city), 'gouvernorat': _g('gouvernorat').apply(clean_city),
                'type_bien': _g('type_bien').apply(remove_noise), 'description': _g('description_complete').apply(remove_noise),
                'type_transaction': _g('type_transaction').apply(remove_noise), 'source': name, 'date_publication': date_pub})

        elif name == 'Century21':
            date_pub = raw['date_scraping'].apply(_parse_date) if 'date_scraping' in raw.columns else pd.Series([_date_from_filename(path)]*len(raw))
            std = pd.DataFrame({'prix': raw['prix'].apply(clean_price), 'surface_m2': _g('surface_m2').apply(clean_surface),
                'ville': _g('localisation').apply(clean_city), 'gouvernorat': _g('localisation').apply(clean_city),
                'type_bien': _g('type').apply(remove_noise), 'description': _g('description').apply(remove_noise),
                'type_transaction': _g('status').apply(remove_noise), 'source': name, 'date_publication': date_pub})

        elif name == 'HomeInTunisia':
            date_pub = raw['date_scraping'].apply(_parse_date) if 'date_scraping' in raw.columns else pd.Series([_date_from_filename(path)]*len(raw))
            std = pd.DataFrame({'prix': raw['prix'].apply(clean_price), 'surface_m2': _g('surface_m2').apply(clean_surface),
                'ville': _g('localisation').apply(clean_city), 'gouvernorat': _g('localisation').apply(clean_city),
                'type_bien': _g('type').apply(remove_noise), 'description': _g('titre').apply(remove_noise),
                'type_transaction': None, 'source': name, 'date_publication': date_pub})

        elif name == 'BnB':
            img_col = 'image' if 'image' in raw.columns else None
            date_pub = raw[img_col].apply(_date_from_bnb).fillna(_date_from_filename(path)) if img_col else pd.Series([_date_from_filename(path)]*len(raw))
            # Extraire type_bien depuis titre (meme logique que BO2)
            _bnb_kw = {'villa':'Villa','maison':'Maison','appartement':'Appartement',
                       'studio':'Appartement','chambre':'Chambre','duplex':'Appartement',
                       'penthouse':'Appartement','terrain':'Terrain',
                       'local':'Local Commercial','bureau':'Bureau'}
            def _bnb_type(t):
                if pd.isna(t): return 'Chambre'
                t = str(t).lower()
                for kw, typ in _bnb_kw.items():
                    if kw in t: return typ
                return 'Chambre'
            titre_col = raw.get('titre', pd.Series([None]*len(raw)))
            std = pd.DataFrame({'prix': raw['prix_montant'].apply(clean_price), 'surface_m2': _g('meta_Surface').apply(clean_surface),
                'ville': _g('localisation').apply(clean_city), 'gouvernorat': _g('localisation').apply(clean_city),
                'type_bien': titre_col.apply(_bnb_type), 'description': _g('description').apply(remove_noise),
                'type_transaction': 'Location Courte Duree', 'source': name, 'date_publication': date_pub})
        else:
            continue

        all_dfs.append(std)
        log(f"OK  {name:<25}: {len(std):>6} annonces")

    df = pd.concat(all_dfs, ignore_index=True, sort=False)
    n0 = len(df)
    log(f"\nTotal brut fusionne : {n0:,}")
    return df, n0


# ================================================================
# ETAPE 2 — DEDUPLICATION
# ================================================================

def deduplicate(df):
    section("ETAPE 2 — DEDUPLICATION")
    n1 = len(df)
    df = df.drop_duplicates(keep='first')
    log(f"Passe 1 - Stricts                      : -{n1-len(df):,} supprimes")
    n2 = len(df)
    df['_d'] = df.get('description', pd.Series(['']*len(df))).fillna('').str.lower().str.strip()
    df['_g'] = df['gouvernorat'].fillna('').str.lower().str.strip()
    df['_p'] = df['prix'].astype(str)
    mask = df.duplicated(subset=['_d','_g','_p'], keep='first') & (df['_d'] != '')
    df   = df[~mask].drop(columns=['_d','_g','_p'])
    log(f"Passe 2 - Desc+Gouv+Prix               : -{n2-len(df):,} supprimes")
    log(f"Total apres dedup : {len(df):,}  (-{n1-len(df)} soit {(n1-len(df))/n1*100:.1f}%)")
    return df, len(df)


# ================================================================
# ETAPE 3 — STANDARDISATION
# ================================================================

def standardize(df):
    section("ETAPE 3 — STANDARDISATION")

    # Dates — redistribution sources sans date
    df['date_publication'] = pd.to_datetime(df['date_publication'], errors='coerce')
    MIN_DATE = pd.Timestamp('2020-01-01')
    MAX_DATE = pd.Timestamp(datetime.now())
    mask_fiable = df['source'].isin(SOURCES_DATE_REELLE)
    df.loc[mask_fiable & (df['date_publication'] < MIN_DATE), 'date_publication'] = pd.NaT
    df.loc[mask_fiable & (df['date_publication'] > MAX_DATE), 'date_publication'] = pd.NaT

    mask_redistrib = df['source'].isin(SOURCES_SANS_DATE) | (~mask_fiable & df['date_publication'].isna())
    n_r = mask_redistrib.sum()
    if n_r > 0:
        periodes = sorted(DATE_DISTRIBUTION.keys())
        poids    = [DATE_DISTRIBUTION[p] for p in periodes]
        s        = sum(poids)
        poids    = [p/s for p in poids]
        rng      = np.random.default_rng(42)
        choices  = rng.choice(len(periodes), size=n_r, p=poids)
        annees   = [periodes[i][0] for i in choices]
        trims    = [periodes[i][1] for i in choices]
        mois_fin = [min((q-1)*3+1+int(rng.integers(0,3)), 12) for q in trims]
        df.loc[mask_redistrib, 'date_publication'] = [pd.Timestamp(a,m,1) for a,m in zip(annees, mois_fin)]
        log(f"Redistribution dates ({n_r:,} annonces sans date reelle)")
        by_yr = pd.to_datetime(df.loc[mask_redistrib,'date_publication']).dt.year.value_counts().sort_index()
        for yr, cnt in by_yr.items():
            log(f"  {yr}: {cnt:,} ann. ({cnt/n_r*100:.1f}%)")

    df['date_publication'] = df['date_publication'].fillna(pd.Timestamp('2025-06-01'))
    df['annee']       = df['date_publication'].dt.year.astype(int)
    df['mois']        = df['date_publication'].dt.month.astype(int)
    df['annee_mois']  = df['date_publication'].dt.strftime('%Y-%m')
    df['trimestre']   = df['mois'].apply(lambda m: (m-1)//3+1)
    df['semestre']    = df['mois'].apply(lambda m: 1 if m<=6 else 2)
    df['high_season'] = df['mois'].apply(lambda m: 1 if m in HIGH_SEASON_MONTHS else 0)
    log(f"Plage temporelle : {df['annee_mois'].min()} -> {df['annee_mois'].max()}")
    log(f"Distribution annee : {df['annee'].value_counts().sort_index().to_dict()}")

    # type_transaction
    df['type_transaction'] = df['type_transaction'].apply(_std_transaction)
    mode_tt = df['type_transaction'].mode()
    if len(mode_tt) > 0:
        df['type_transaction'] = df['type_transaction'].fillna(int(mode_tt.iloc[0]))
    df['type_transaction'] = df['type_transaction'].astype(int)
    log(f"type_transaction : {df['type_transaction'].value_counts().sort_index().to_dict()}")

    # Coerce numeriques
    df['prix']       = pd.to_numeric(df['prix'],       errors='coerce')
    df['surface_m2'] = pd.to_numeric(df['surface_m2'], errors='coerce')

    # Gouvernorat
    log("Normalisation gouvernorat...")
    df['_gouvernorat_str'] = df['gouvernorat'].apply(normalize_gouvernorat)
    log(f"Taux Unknown : {(df['_gouvernorat_str']=='Unknown').mean()*100:.1f}%")
    return df


# ================================================================
# HELPERS CLASSIFICATION — IDENTIQUES BO2
# ================================================================

def _extract_pieces_from_apptype(val):
    """Identique BO2 extract_pieces_from_apptype()"""
    if pd.isna(val): return None, None
    v = str(val).strip()
    m = re.match(r'^App[\.\s]*\.?\s*(\d+)\s*Pic$', v, re.IGNORECASE)
    if m: return 'Appartement', int(m.group(1))
    if re.match(r'^Surfaces?$', v, re.IGNORECASE): return 'Terrain', None
    return None, None


def _std_type_bien(val, surface_m2=None):
    """
    Identique BO2 std_type_bien() — extrait type canonique depuis type_bien brut.
    Applique TYPE_MAP, regex spéciaux, fallback.
    """
    if pd.isna(val): return None
    tb, _ = _extract_pieces_from_apptype(val)
    if tb: return tb
    v = str(val).lower().strip()
    if re.search(r'immo\s*neuf|projet\s*neuf', v):
        try:
            if surface_m2 and float(surface_m2) > 150: return 'Maison'
        except: pass
        return 'Appartement'
    if v == 'autre': return 'Autre'
    for k, can in TYPE_MAP.items():
        if k in v: return can
    return str(val).strip().title()


def _refine_autre(row) -> str:
    """Identique BO2 refine_autre() — raffine les types 'Autre'."""
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


def _classify_row(row) -> str:
    """
    Identique BO2 classify_row() — utilise type_bien standardisé + DESC_KW fallback.
    Retourne le type_bien canonique (pas le groupe).
    """
    tb   = str(row.get('type_bien', '') or '').lower()
    desc = str(row.get('description', '') or '').lower()
    for typ, kws in TYPE_KW.items():
        for kw in kws:
            if kw in tb: return typ
    if not tb or tb in ('none', 'nan', ''):
        for typ, kws in DESC_KW.items():
            for kw in kws:
                if kw in desc: return typ
    return 'Divers'


# ================================================================
# ETAPE 4 — SEGMENTATION (IDENTIQUE BO2)
# ================================================================

def segment(df):
    section("ETAPE 4 — SEGMENTATION EN 4 GROUPES")

    # Étape A : standardiser type_bien (comme BO2 standardize + std_type_bien)
    df['type_bien'] = df.apply(
        lambda r: _std_type_bien(r.get('type_bien'), r.get('surface_m2')), axis=1)

    # Étape B : raffiner les 'Autre' (comme BO2 refine_autre)
    df['type_bien'] = df.apply(_refine_autre, axis=1)

    # Étape C : classifier → type_categorise (comme BO2 classify_row)
    df['type_categorise'] = df.apply(_classify_row, axis=1)

    # Étape D : mapper vers groupe via GROUPE_MAP (comme BO2)
    df['groupe'] = df['type_categorise'].map(GROUPE_MAP).fillna('Divers')

    for g, c in df['groupe'].value_counts().items():
        log(f"  {g:<15}: {c:>6} ({c/len(df)*100:.1f}%)")
    return df


# ================================================================
# ETAPE 4b — ENCODAGE VILLE (identique BO2)
# ================================================================

def encode_ville(df, min_freq=30):
    section("ETAPE 4b — ENCODAGE VILLE (hierarchique: gov*1000 + rang)")
    df['_ville_key'] = df['ville'].fillna('').str.lower().str.strip()
    df['_gov_int']   = df['_gouvernorat_str'].apply(encode_gouvernorat)

    grp = df.groupby(['_gov_int','_ville_key']).size().reset_index(name='_cnt')
    grp = grp[(grp['_ville_key']!='') & (grp['_gov_int']>0)]
    grp = grp.sort_values(['_gov_int','_cnt'], ascending=[True,False])
    grp['_rang'] = grp.groupby('_gov_int').cumcount() + 1
    grp.loc[grp['_cnt'] < min_freq, '_rang'] = 0

    rank_map = {(int(r['_gov_int']), r['_ville_key']): int(r['_gov_int'])*1000+int(r['_rang'])
                for _, r in grp.iterrows()}

    def _enc(vk, gov):
        g = int(gov) if pd.notna(gov) else 0
        if g == 0: return 0
        if not vk or vk in ('nan','none',''): return g*1000
        return rank_map.get((g, vk), g*1000)

    df['ville_encoded'] = df.apply(lambda r: _enc(r['_ville_key'], r['_gov_int']), axis=1)
    df = df.drop(columns=['_ville_key','_gov_int'], errors='ignore')
    n_top  = (df['ville_encoded'] % 1000 > 0).sum()
    n_rare = (df['ville_encoded'] % 1000 == 0).sum()
    log(f"Villes frequentes (>={min_freq}) : {grp[grp['_rang']>0]['_ville_key'].nunique()} codes individuels")
    log(f"Lignes avec code ville > 0     : {n_top:,} ({n_top/len(df)*100:.1f}%)")
    log(f"Lignes ville rare/inconnue     : {n_rare:,}")
    return df


# ================================================================
# ETAPE 5 — NETTOYAGE + ISOLATION FOREST (MEME QUE BO2)
# ================================================================

def clean_groups(df):
    section("ETAPE 5 — NETTOYAGE + ISOLATION FOREST + NLP")
    groupes_clean = {}

    for groupe in ['Residentiel','Foncier','Commercial','Divers']:
        dg = df[df['groupe']==groupe].copy()
        if len(dg) == 0: continue
        n_avant = len(dg)
        s = SEUILS[groupe]

        # 1. Drop prix NaN
        n_prix_nan = dg['prix'].isna().sum()
        dg = dg[dg['prix'].notna()].copy()
        if n_prix_nan > 0:
            log(f"  [{groupe}] prix=NaN supprimes : {n_prix_nan}")

        # 2. Prix invalides — seuils separes loc/vent (MEMES que BO2)
        is_loc  = dg['type_transaction'] == 1
        is_vent = dg['type_transaction'] == 2
        mask_loc_inv  = is_loc  & ((dg['prix'] < s['prix_min_loc'])  | (dg['prix'] > s['prix_max_loc']))
        mask_vent_inv = is_vent & ((dg['prix'] < s['prix_min_vent']) | (dg['prix'] > s['prix_max_vent']))
        mask_prix_inv = mask_loc_inv | mask_vent_inv
        n_prix_drop   = mask_prix_inv.sum()
        dg = dg[~mask_prix_inv]

        # 3. Surface invalide (NaN conserve — sera impute dans handle_missing)
        mask_surf_inv = dg['surface_m2'].notna() & (
            (dg['surface_m2'] < s['surf_min']) | (dg['surface_m2'] > s['surf_max']))
        n_surf_drop = mask_surf_inv.sum()
        dg = dg[~mask_surf_inv]

        # 4. Prix/m2 absurde — VENTES seulement (comme BO2)
        dg['_prix_m2'] = np.where(
            dg['prix'].notna() & dg['surface_m2'].notna() &
            (dg['surface_m2']>0) & (dg['type_transaction']==2),
            dg['prix'] / dg['surface_m2'], np.nan)
        n_pm2_drop = (dg['_prix_m2'] > 100_000).sum()
        dg = dg[~(dg['_prix_m2'] > 100_000)]

        # 5. Isolation Forest separe loc/vent (contamination=0.04 comme BO2)
        n_anom = 0
        for tt in [1, 2]:
            mask_tt  = dg['type_transaction'] == tt
            feats_tt = dg.loc[mask_tt, ['prix','surface_m2']].dropna()
            if len(feats_tt) >= 50:
                X     = StandardScaler().fit_transform(feats_tt)
                preds = IsolationForest(contamination=0.04, random_state=42).fit_predict(X)
                anoms = feats_tt.index[preds == -1]
                dg    = dg.drop(index=anoms)
                n_anom += len(anoms)

        dg = dg.drop(columns=['_prix_m2'], errors='ignore')
        groupes_clean[groupe] = dg
        log(f"  {groupe:<15}: {n_avant:>6} -> {len(dg):>6} | "
            f"prix_invalides={n_prix_drop} "
            f"(loc={mask_loc_inv.sum()} vent={mask_vent_inv.sum()}) "
            f"surf_invalides={n_surf_drop} prix_m2_absurde={n_pm2_drop} IF={n_anom}")

    return groupes_clean


# ================================================================
# ETAPE 5b — GESTION DES VALEURS MANQUANTES (MEME QUE BO2)
# ================================================================

def handle_missing(groupes_clean):
    section("ETAPE 5b — GESTION DES VALEURS MANQUANTES")
    log("  surface_m2      : imputation mediane par groupe")
    log("  type_categorise : imputation par mode")
    log("  prix            : deja traite (NaN supprimes en etape 5)")

    COLS_PROTEGEES = {'prix','gouvernorat','type_bien','image_url','date_publication'}
    SEUIL_DROP     = 0.80

    for groupe, dg in groupes_clean.items():
        log(f"\n  [{groupe}] — {len(dg)} annonces")

        # Drop colonnes >80% NA
        cols_drop = [c for c in dg.columns
                     if dg[c].isna().mean() > SEUIL_DROP and c not in COLS_PROTEGEES]
        if cols_drop:
            dg = dg.drop(columns=cols_drop, errors='ignore')
            log(f"    Colonnes supprimees (>{SEUIL_DROP*100:.0f}% NA) : {cols_drop}")

        # surface_m2 — mediane par groupe (MEME QUE BO2)
        # Créer la colonne si elle n'existe pas
        if 'surface_m2' not in dg.columns:
            dg['surface_m2'] = np.nan
            log(f"    surface_m2 MANQUANTE : colonne creee (tous NaN)")
        
        n_na = dg['surface_m2'].isna().sum()
        if n_na > 0:
            med = dg['surface_m2'].median()
            if pd.isna(med): med = 100.0
            dg['surface_m2'] = dg['surface_m2'].fillna(med)
            log(f"    surface_m2 NA : {n_na} -> imputes mediane={med:.1f} m2")

        # type_categorise — mode
        if 'type_categorise' in dg.columns:
            n_na = dg['type_categorise'].isna().sum()
            if n_na > 0:
                mode_s   = dg['type_categorise'].mode()
                fill_val = mode_s.iloc[0] if len(mode_s) > 0 else 'Divers'
                dg['type_categorise'] = dg['type_categorise'].fillna(fill_val)
                log(f"    type_categorise mode='{fill_val}' : {n_na} NA combles")

        groupes_clean[groupe] = dg

    return groupes_clean


# ================================================================
# ETAPE 5c — ENCODAGE GOUVERNORAT + SUPPRESSION UNKNOWN
# ================================================================

def encode_categorical(df, groupes_clean):
    section("ETAPE 5d — ENCODAGE GOUVERNORAT + SUPPRESSION Unknown")

    for groupe, dg in groupes_clean.items():
        # Propager _gouvernorat_str depuis df si absent dans dg
        if '_gouvernorat_str' not in dg.columns:
            valid_idx = [i for i in dg.index if i in df.index]
            if valid_idx:
                dg.loc[valid_idx, '_gouvernorat_str'] = df.loc[valid_idx, '_gouvernorat_str'].values

        # Encoder gouvernorat
        dg['gouvernorat'] = dg['_gouvernorat_str'].apply(encode_gouvernorat)
        n_unk = (dg['gouvernorat'] == 0).sum()
        log(f"  [{groupe}] gouvernorat encode | Unknown={n_unk} (seront supprimes apres geocodage)")

        # Inférer gouvernorat depuis ville si Unknown
        for _ in range(3):
            mask_unk = dg['gouvernorat'] == 0
            if mask_unk.sum() == 0: break
            dg.loc[mask_unk, '_gouvernorat_str'] = dg.loc[mask_unk, 'ville'].apply(normalize_gouvernorat)
            dg.loc[mask_unk, 'gouvernorat']      = dg.loc[mask_unk, '_gouvernorat_str'].apply(encode_gouvernorat)

        # Supprimer Unknown restants
        n_before = len(dg)
        dg = dg[dg['gouvernorat'] > 0].copy()
        if n_before > len(dg):
            log(f"  [{groupe}] Unknown supprimes : -{n_before-len(dg)}")

        groupes_clean[groupe] = dg

    return df, groupes_clean
