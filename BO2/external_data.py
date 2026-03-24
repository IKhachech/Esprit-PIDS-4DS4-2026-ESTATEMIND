"""
external_data.py — Chargement des données externes officielles.

Sources :
  - BCT     : bct_dataset_20260219_1332.xlsx
  - INS     : ins_dataset_20260219_1307.xlsx
  - Signaux : signaux_immobilier_tunisie.xlsx

Utilise zipfile (metadata_encoding='utf-8') + openpyxl pour contourner
le bug cp437 absent sur certains environnements Python.
"""

import os, re, io, zipfile, warnings
import numpy as np
import pandas as pd
import openpyxl

warnings.filterwarnings('ignore')

DEFAULT_PATHS = {
    'bct':     'bct/bct_dataset_20260219_1332.xlsx',
    'ins':     'Ins/ins_dataset_20260219_1307.xlsx',
    'signaux': 'goolgeMaps/signaux_immobilier_tunisie.xlsx',
}


# ================================================================
# HELPERS
# ================================================================

def _open_xlsx(path: str):
    """Ouvre un xlsx sans dépendre du codec cp437 (absent sur certains systèmes)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(path, 'r', metadata_encoding='utf-8') as zin:
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                zout.writestr(item, zin.read(item.filename))
    buf.seek(0)
    return openpyxl.load_workbook(buf, read_only=True, data_only=True)


def _sheet_to_df(path: str, sheet_name: str, header_row: int) -> pd.DataFrame:
    """Charge une feuille Excel → DataFrame. header_row = index 0-based de la ligne d'en-tête."""
    wb   = _open_xlsx(path)
    ws   = wb[sheet_name]
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    wb.close()

    if not rows or header_row >= len(rows):
        return pd.DataFrame()

    headers = [str(c) if c is not None else f'col_{i}'
               for i, c in enumerate(rows[header_row])]
    data    = [r for r in rows[header_row + 1:] if any(v is not None for v in r)]
    return pd.DataFrame(data, columns=headers)


def _parse_periode_yq(periode: str):
    """'deuxième-trimestre 2024' → (2024, 2) ou (None, None)."""
    if pd.isna(periode): return None, None
    s = str(periode).lower().strip()
    m = re.search(r'(\d{4})', s)
    if not m: return None, None
    yr = int(m.group(1))
    if   'premi'  in s: q = 1
    elif 'deuxi'  in s: q = 2
    elif 'troisi' in s: q = 3
    elif 'quatri' in s: q = 4
    else: return None, None
    return yr, q


def _cycle(pib: float, inflation: float, taux: float) -> str:
    if pib >= 2.0 and inflation < 6.0 and taux < 7.5:  return 'peak'
    if pib >= 0.5 and inflation < 7.0:                  return 'growth'
    if -1.0 <= pib < 0.0:                               return 'recovery'
    if pib < -1.0 or (taux >= 8.0 and inflation > 8.0): return 'decline'
    return 'stabilization'


# ================================================================
# BCT
# ================================================================

def load_bct(path: str) -> dict:
    defaults = {'taux_directeur': 7.0, 'taux_moyen': 7.08, 'tre': 6.0, 'date': 'inconnu'}
    if not os.path.exists(path):
        print(f"  [WARN] BCT absent → fallback")
        return defaults
    try:
        df = _sheet_to_df(path, '🗂️ Dataset Complet', header_row=3)
        df['Valeur'] = pd.to_numeric(df['Valeur'], errors='coerce')
        result = dict(defaults)
        for _, row in df.iterrows():
            ind = str(row.get('Indicateur', '')).lower()
            val = row.get('Valeur')
            if pd.isna(val): continue
            if 'directeur' in ind:
                result['taux_directeur'] = float(val)
                d = row.get('Date')
                if d and not pd.isna(d): result['date'] = str(d)
            elif ind.strip() == 'taux':
                result['taux_moyen'] = float(val)
            elif any(x in ind for x in ['épargne', 'epargne', 'tre']):
                result['tre'] = float(val)
        print(f"  ✔ BCT chargé : taux_directeur={result['taux_directeur']}% | date={result['date']}")
        return result
    except Exception as e:
        print(f"  [WARN] BCT erreur : {e} → fallback")
        return defaults


# ================================================================
# INS
# ================================================================

def load_ins(path: str) -> dict:
    defaults = {'pib_by_quarter': {}, 'inflation_by_month': {}, 'immo_by_quarter': {}}
    if not os.path.exists(path):
        print(f"  [WARN] INS absent → fallback")
        return defaults
    result = {k: {} for k in defaults}
    try:
        # PIB
        df_pib = _sheet_to_df(path, '📈 PIB Croissance', header_row=3)
        df_pib['Valeur'] = pd.to_numeric(df_pib['Valeur'], errors='coerce')
        for _, row in df_pib.iterrows():
            y, q = _parse_periode_yq(row.get('Période'))
            val  = row.get('Valeur')
            if y and not pd.isna(val):
                result['pib_by_quarter'][(y, q)] = float(val)

        # Inflation
        df_inf = _sheet_to_df(path, '💰 Inflation', header_row=3)
        df_inf['Valeur'] = pd.to_numeric(df_inf['Valeur'], errors='coerce')
        MOIS = {'janvier':1,'février':2,'mars':3,'avril':4,'mai':5,'juin':6,
                'juillet':7,'août':8,'septembre':9,'octobre':10,'novembre':11,'décembre':12}
        for _, row in df_inf.iterrows():
            p   = str(row.get('Période', '')).lower()
            m   = re.search(r'(\d{4})', p)
            val = row.get('Valeur')
            if not m or pd.isna(val): continue
            yr   = int(m.group(1))
            mois = next((v for k, v in MOIS.items() if k in p), None)
            if mois: result['inflation_by_month'][(yr, mois)] = float(val)

        # Immobilier
        df_immo = _sheet_to_df(path, '🏠 Immobilier', header_row=3)
        df_immo['Valeur'] = pd.to_numeric(df_immo['Valeur'], errors='coerce')
        df_immo = df_immo[~df_immo['Série'].astype(str).str.contains('générale|general', case=False, na=True)]
        for _, row in df_immo.iterrows():
            y, q  = _parse_periode_yq(row.get('Période'))
            serie = str(row.get('Série', '')).strip()
            val   = row.get('Valeur')
            if y and not pd.isna(val):
                result['immo_by_quarter'][(y, q, serie)] = float(val)

        print(f"  ✔ INS chargé : {len(result['pib_by_quarter'])} trim. PIB | "
              f"{len(result['inflation_by_month'])} mois inflation | "
              f"{len(result['immo_by_quarter'])} indices immo")
        return result
    except Exception as e:
        print(f"  [WARN] INS erreur : {e} → fallback")
        return defaults


# ================================================================
# SIGNAUX
# ================================================================

def load_signaux(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"  [WARN] Signaux absent → fallback")
        return pd.DataFrame()
    try:
        df = _sheet_to_df(path, '🏆 Score Attractivité', header_row=0)
        df = df.dropna(subset=['gouvernorat'])
        df['gouvernorat_key'] = df['gouvernorat'].astype(str).str.strip().str.lower()
        cols = ['gouvernorat','gouvernorat_key','region','profil','population',
                'score_attractivite','nb_immo_direct','nb_projets_neufs',
                'nb_emploi','nb_credit','note_immo_direct','note_projets_neufs','note_emploi']
        df = df[[c for c in cols if c in df.columns]]
        for col in ['score_attractivite','population','nb_immo_direct','nb_projets_neufs',
                    'nb_emploi','nb_credit','note_immo_direct','note_projets_neufs','note_emploi']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        print(f"  ✔ Signaux chargés : {len(df)} gouvernorats | "
              f"score=[{df['score_attractivite'].min():.1f}–{df['score_attractivite'].max():.1f}]")
        return df.reset_index(drop=True)
    except Exception as e:
        print(f"  [WARN] Signaux erreur : {e} → fallback")
        return pd.DataFrame()


# ================================================================
# MARKET_CONTEXT
# ================================================================

def build_market_context(bct: dict, ins: dict) -> dict:
    taux_ref = bct.get('taux_directeur', 7.0)
    TAUX_HIST = {
        (2022,1):6.25,(2022,2):6.25,(2022,3):7.00,(2022,4):8.00,
        (2023,1):8.00,(2023,2):8.00,(2023,3):8.00,(2023,4):8.00,
        (2024,1):7.50,(2024,2):7.50,(2024,3):7.50,(2024,4):7.25,
        (2025,1):7.25,(2025,2):7.00,(2025,3):7.00,(2025,4):7.00,
        (2026,1):taux_ref,
    }
    pib   = ins.get('pib_by_quarter', {})
    infla = ins.get('inflation_by_month', {})
    ctx   = {}

    for (yr, q), pib_val in pib.items():
        taux  = TAUX_HIST.get((yr, q), taux_ref)
        qm    = {1:[1,2,3],2:[4,5,6],3:[7,8,9],4:[10,11,12]}.get(q, [])
        iv    = [infla.get((yr,m)) for m in qm if (yr,m) in infla]
        if not iv: iv = [infla.get((yr-1,m)) for m in qm if (yr-1,m) in infla]
        infl  = float(np.mean(iv)) if iv else 5.5
        ctx[(yr, q)] = (taux, _cycle(pib_val, infl, taux))

    for (yr, q), taux in TAUX_HIST.items():
        if (yr, q) not in ctx:
            proxy = list(pib.values())[-1] if pib else 1.5
            ctx[(yr, q)] = (taux, _cycle(proxy, 5.0, taux))

    ctx = dict(sorted(ctx.items()))
    print(f"  ✔ MARKET_CONTEXT : {len(ctx)} trimestres calculés depuis BCT + INS")
    for k, v in ctx.items():
        print(f"    {k} → taux={v[0]}% | cycle={v[1]}")
    return ctx


# ================================================================
# NEGO RATES
# ================================================================

def build_nego_rates(ins: dict, current_quarter: tuple = (2026, 1)) -> dict:
    FALLBACK = {
        'Appartement':0.04,'Chambre':0.04,'Bureau':0.05,'Maison':0.05,
        'Villa':0.06,'Terrain':0.07,'Ferme':0.08,'Local Commercial':0.06,
        'Autre':0.05,'Divers':0.04,
    }
    immo = ins.get('immo_by_quarter', {})
    avail = [(y, q) for (y, q, _) in immo]
    if not avail:
        print("  [WARN] Pas de données INS immo → taux négociation fallback hardcodé")
        return FALLBACK

    yr, q = min(avail, key=lambda yq: abs((yq[0]-current_quarter[0])*4+(yq[1]-current_quarter[1])))

    def g2n(g):
        if g > 10: return 0.03
        if g > 5:  return 0.05
        if g > 2:  return 0.07
        return 0.09

    ag = immo.get((yr, q, 'Appartement'), 5.0)
    mg = immo.get((yr, q, 'Maisons'),     5.0)
    tg = immo.get((yr, q, 'Terrain nus'), 5.0)

    nego = {
        'Appartement': g2n(ag),   'Chambre': g2n(ag),
        'Bureau':      g2n(ag*0.8),'Maison': g2n(mg),
        'Villa':       g2n(mg*0.9),'Terrain': g2n(tg),
        'Ferme':       g2n(tg*0.85),'Local Commercial': g2n(tg*0.9),
        'Autre': 0.05, 'Divers': 0.04,
    }
    print(f"  ✔ NEGO_RATES depuis INS Q{q}/{yr} :")
    print(f"    Appartement {ag}% → {nego['Appartement']*100:.0f}% | Maisons {mg}% → {nego['Maison']*100:.0f}% | Terrain {tg}% → {nego['Terrain']*100:.0f}%")
    return nego


# ================================================================
# GOV FEATURES
# ================================================================

def build_gouvernorat_features(signaux_df: pd.DataFrame) -> dict:
    if signaux_df.empty: return {}
    df = signaux_df.copy()
    for col in ['score_attractivite','population','nb_immo_direct','nb_projets_neufs','nb_emploi','nb_credit']:
        if col in df.columns:
            mx = df[col].max()
            df[f'{col}_norm'] = (df[col] / mx).round(4) if mx and mx > 0 else 0.0
    result = {}
    for _, row in df.iterrows():
        name = str(row.get('gouvernorat', '')).strip()
        if not name: continue
        result[name] = {
            'score_attractivite':      float(row.get('score_attractivite', 0) or 0) / 100.0,
            'score_attractivite_norm': float(row.get('score_attractivite_norm', 0) or 0),
            'population':              float(row.get('population', 0) or 0),
            'population_norm':         float(row.get('population_norm', 0) or 0),
            'nb_immo_direct':          float(row.get('nb_immo_direct', 0) or 0),
            'nb_immo_direct_norm':     float(row.get('nb_immo_direct_norm', 0) or 0),
            'nb_projets_neufs':        float(row.get('nb_projets_neufs', 0) or 0),
            'nb_projets_neufs_norm':   float(row.get('nb_projets_neufs_norm', 0) or 0),
            'nb_emploi':               float(row.get('nb_emploi', 0) or 0),
            'nb_emploi_norm':          float(row.get('nb_emploi_norm', 0) or 0),
            'nb_credit':               float(row.get('nb_credit', 0) or 0),
            'nb_credit_norm':          float(row.get('nb_credit_norm', 0) or 0),
            'note_immo_direct':        float(row.get('note_immo_direct', 0) or 0),
            'region':                  str(row.get('region', '') or ''),
            'profil':                  str(row.get('profil', '') or ''),
        }
    print(f"  ✔ GOV_FEATURES : {len(result)} gouvernorats")
    return result


# ================================================================
# ENTRY POINT
# ================================================================

def load_all(
    bct_path:        str   = DEFAULT_PATHS['bct'],
    ins_path:        str   = DEFAULT_PATHS['ins'],
    sig_path:        str   = DEFAULT_PATHS['signaux'],
    current_quarter: tuple = (2026, 1),
) -> dict:
    print("\n" + "=" * 65)
    print("   CHARGEMENT DONNÉES EXTERNES (BCT + INS + SIGNAUX)")
    print("=" * 65)
    bct     = load_bct(bct_path)
    ins     = load_ins(ins_path)
    signaux = load_signaux(sig_path)
    return {
        'bct':            bct,
        'ins':            ins,
        'signaux':        signaux,
        'market_context': build_market_context(bct, ins),
        'nego_rates':     build_nego_rates(ins, current_quarter),
        'gov_features':   build_gouvernorat_features(signaux),
    }
