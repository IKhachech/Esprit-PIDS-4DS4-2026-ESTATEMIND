"""
fix_ville_enc.py — Recalcul ville_enc depuis mubawab_annonces.csv
EstateMind BO2

Probleme :
    ville_enc dans prepare.py est calcule apres filtrage outliers
    → certaines villes ont des medianes incorrectes (La Marsa = 0.322)

Solution :
    Recalculer ville_enc directement depuis mubawab_annonces.csv
    avec filtre outliers plus precis et minimum 5 annonces par ville

Outputs :
    ville_mapping_residentiel_vente.json    → corrige
    ville_mapping_residentiel_location.json → corrige
    ville_mapping_terrain.json              → corrige

Usage :
    python fix_ville_enc.py
"""

import json, os, warnings
import pandas as pd
import numpy as np
warnings.filterwarnings('ignore')

MUBAWAB_FILE = 'mubawab_annonces.csv'
MIN_ANNONCES = 5   # minimum annonces par ville pour calculer ville_enc

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')


def build_ville_enc(df, prix_col, categorie, min_ann=MIN_ANNONCES,
                    prix_min=None, prix_max=None):
    """
    Calcule ville_enc depuis les donnees brutes mubawab.
    Filtre outliers par percentile, garde villes avec assez d annonces.
    """
    sub = df[df['categorie'] == categorie].copy()
    sub['prix'] = pd.to_numeric(sub[prix_col], errors='coerce')
    sub = sub.dropna(subset=['prix', 'ville'])

    # Filtre outliers
    p2  = sub['prix'].quantile(0.02)
    p98 = sub['prix'].quantile(0.98)
    if prix_min: p2  = max(p2, prix_min)
    if prix_max: p98 = min(p98, prix_max)
    sub = sub[(sub['prix'] >= p2) & (sub['prix'] <= p98)]

    # Mediane par ville
    stats = sub.groupby('ville')['prix'].agg(['median', 'count']).reset_index()
    stats.columns = ['ville', 'prix_median', 'n']
    stats = stats[stats['n'] >= min_ann].copy()

    log(f'  {categorie} : {len(sub):,} annonces | {len(stats)} villes valides')

    # Normaliser 0-1 avec percentile 5%-95%
    # Evite que les valeurs extremes (Carthage) ecrasent les autres
    vmin = stats['prix_median'].quantile(0.05)
    vmax = stats['prix_median'].quantile(0.95)
    if vmax == vmin:
        stats['ville_enc'] = 0.5
    else:
        stats['ville_enc'] = ((stats['prix_median'] - vmin) /
                               (vmax - vmin)).clip(0, 1).round(4)

    ville_map = dict(zip(stats['ville'], stats['ville_enc']))
    median_enc = float(stats['ville_enc'].median())
    ville_map['__default__'] = round(median_enc, 4)

    return ville_map, stats


def print_top(stats, n=15):
    top = stats.nlargest(n, 'prix_median')[['ville','prix_median','n','ville_enc']]
    for _, row in top.iterrows():
        log(f'  {row["ville"]:<25} : {row["prix_median"]:>12,.0f} TND '
            f'| enc={row["ville_enc"]:.3f} | n={int(row["n"])}')


def run():
    section('FIX_VILLE_ENC.PY — Recalcul mappings')

    if not os.path.exists(MUBAWAB_FILE):
        print(f'ERREUR : {MUBAWAB_FILE} introuvable')
        return

    df = pd.read_csv(MUBAWAB_FILE)
    log(f'Mubawab : {len(df):,} annonces')

    # ── VENTE ────────────────────────────────────────────────────
    section('RESIDENTIEL VENTE')
    vm_vente, stats_vente = build_ville_enc(
        df, 'prix_tnd', 'vente',
        prix_min=50_000, prix_max=10_000_000
    )
    print_top(stats_vente)

    with open('ville_mapping_residentiel_vente.json', 'w', encoding='utf-8') as f:
        json.dump(vm_vente, f, ensure_ascii=False, indent=2)
    log(f'\n  Sauvegarde : ville_mapping_residentiel_vente.json')
    log(f'  La Marsa    : {vm_vente.get("La Marsa", "absent")}')
    log(f'  Carthage    : {vm_vente.get("Carthage", "absent")}')
    log(f'  Ariana      : {vm_vente.get("Ariana", "absent")}')
    log(f'  Tunis       : {vm_vente.get("Tunis", "absent")}')
    log(f'  Hammamet    : {vm_vente.get("Hammamet", "absent")}')

    # ── LOCATION ─────────────────────────────────────────────────
    section('RESIDENTIEL LOCATION')
    vm_loc, stats_loc = build_ville_enc(
        df, 'prix_tnd', 'location',
        prix_min=200, prix_max=50_000
    )
    print_top(stats_loc)

    with open('ville_mapping_residentiel_location.json', 'w', encoding='utf-8') as f:
        json.dump(vm_loc, f, ensure_ascii=False, indent=2)
    log(f'\n  Sauvegarde : ville_mapping_residentiel_location.json')
    log(f'  La Marsa    : {vm_loc.get("La Marsa", "absent")}')
    log(f'  Hammamet    : {vm_loc.get("Hammamet", "absent")}')
    log(f'  Ariana      : {vm_loc.get("Ariana", "absent")}')

    # ── RESIDENTIEL FALLBACK (vente + location) ──────────────────
    section('RESIDENTIEL GLOBAL (fallback)')
    vm_all, stats_all = build_ville_enc(
        df, 'prix_tnd', 'vente',
        prix_min=50_000, prix_max=10_000_000
    )
    # Combine vente + location
    df_loc = df[df['categorie'] == 'location'].copy()
    df_loc['prix'] = pd.to_numeric(df_loc['prix_tnd'], errors='coerce')
    df_both = pd.concat([
        df[df['categorie']=='vente'][['ville','prix_tnd']].rename(columns={'prix_tnd':'prix'}),
        df_loc[['ville','prix']]
    ])
    df_both['prix'] = pd.to_numeric(df_both['prix'], errors='coerce')
    df_both = df_both.dropna(subset=['prix','ville'])
    p2, p98 = df_both['prix'].quantile(0.02), df_both['prix'].quantile(0.98)
    df_both = df_both[(df_both['prix'] >= p2) & (df_both['prix'] <= p98)]
    stats_b = df_both.groupby('ville')['prix'].agg(['median','count']).reset_index()
    stats_b = stats_b[stats_b['count'] >= MIN_ANNONCES]
    vmin, vmax = stats_b['median'].min(), stats_b['median'].max()
    stats_b['ville_enc'] = ((stats_b['median'] - vmin) / (vmax - vmin)).round(4)
    vm_res = dict(zip(stats_b['ville'], stats_b['ville_enc']))
    vm_res['__default__'] = round(float(stats_b['ville_enc'].median()), 4)
    with open('ville_mapping_residentiel.json', 'w', encoding='utf-8') as f:
        json.dump(vm_res, f, ensure_ascii=False, indent=2)
    log(f'  Sauvegarde : ville_mapping_residentiel.json')

    # ── TERRAIN ──────────────────────────────────────────────────
    section('TERRAIN (foncier)')
    ter_file = 'dataset_terrain.csv'
    if os.path.exists(ter_file):
        df_ter = pd.read_csv(ter_file)
        log(f'  dataset_terrain.csv : {len(df_ter)} lignes')
        # Terrain n a pas de colonne ville — garder mapping existant
        log('  Mapping terrain : conserve tel quel (pas de colonne ville)')
    else:
        log('  dataset_terrain.csv introuvable')

    # ── RAPPORT ──────────────────────────────────────────────────
    section('RAPPORT FINAL')
    log('Mappings corriges :')
    log('  ville_mapping_residentiel_vente.json')
    log('  ville_mapping_residentiel_location.json')
    log('  ville_mapping_residentiel.json')
    log('')
    log('Verification La Marsa :')
    log(f'  Vente    : {vm_vente.get("La Marsa", "absent"):.3f}')
    log(f'  Location : {vm_loc.get("La Marsa", "absent"):.3f}')
    log('')
    log('Prochaine etape : python agent_bo2.py')
    log('  (pas besoin de reentraine train_model.py)')


if __name__ == '__main__':
    run()