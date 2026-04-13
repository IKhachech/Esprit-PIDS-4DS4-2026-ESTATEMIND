"""
merge_datasets.py — Fusion datasets résidentiels
Emplacement : BO2/valuation_agent/
Lancer      : python merge_datasets.py

Produit : dataset_residentiel.csv dans le même dossier
"""
import pandas as pd
import numpy as np
import os

_HERE = os.path.dirname(os.path.abspath(__file__))

# Colonnes communes à harmoniser
ALL_HAS = [
    'has_ascenseur','has_meuble','has_terrasse','has_piscine',
    'has_vue_mer','has_garage','has_securite','has_climatisation',
    'has_chauffage','has_cuisine_equip','has_double_vitrage',
    'has_porte_blindee','has_concierge','has_jardin','has_standing',
    'has_vue_montagne','has_cheminee',
]
BASE_COLS = [
    'gouvernorat_enc','ville_enc','lat','lon','surface_m2',
    'nb_pieces','nb_chambres','score_attractivite','market_tension',
    'cycle_marche','prix_region_median','text_embedding_score',
    'nb_images','source_enc','prix','image_url',
]

print("=" * 55)
print("  FUSION DATASETS RÉSIDENTIELS")
print("=" * 55)

def load_and_tag(filename, type_bien, type_transaction=None):
    """
    Charge un CSV, ajoute type_bien et type_transaction.
    type_bien       : 1=Appartement, 2=Maison, 3=Villa
    type_transaction: 1=Location, 2=Vente, None=depuis colonne
    """
    path = os.path.join(_HERE, filename)
    # Villa utilise separateur ;
    sep = ';' if 'villa' in filename else ','
    df  = pd.read_csv(path, sep=sep)
    print(f"\n  {filename} : {len(df):,} lignes")

    df['type_bien'] = type_bien

    if type_transaction is not None:
        df['type_transaction'] = type_transaction
    else:
        # Villa : 0=Vente, 1=Location → convertir en 2=Vente, 1=Location
        if 'type_transaction' in df.columns:
            df['type_transaction'] = df['type_transaction'].map({0: 2, 1: 1}).fillna(2)
        else:
            df['type_transaction'] = 2

    # Ajouter colonnes has_ manquantes avec 0
    for col in ALL_HAS:
        if col not in df.columns:
            df[col] = 0

    print(f"    type_bien={type_bien} | "
          f"vente={( df['type_transaction']==2).sum():,} | "
          f"location={(df['type_transaction']==1).sum():,}")
    return df

# ── Chargement et tagging ───────────────────────────────────────
dfs = [
    load_and_tag('dataset_appart_vente.csv',    type_bien=1, type_transaction=2),
    load_and_tag('dataset_appart_location.csv', type_bien=1, type_transaction=1),
    load_and_tag('dataset_maison_vente.csv',    type_bien=2, type_transaction=2),
    load_and_tag('dataset_maison_location.csv', type_bien=2, type_transaction=1),
    load_and_tag('dataset_villa.csv',           type_bien=3, type_transaction=None),
]

# ── Fusion ─────────────────────────────────────────────────────
TARGET_COLS = BASE_COLS + ['type_bien','type_transaction'] + ALL_HAS

df_res = pd.concat(dfs, ignore_index=True)

# Garder uniquement les colonnes cibles (dans l'ordre)
for col in TARGET_COLS:
    if col not in df_res.columns:
        df_res[col] = 0
df_res = df_res[TARGET_COLS]

# ── Nettoyage ──────────────────────────────────────────────────
n_avant = len(df_res)
df_res  = df_res[df_res['prix'].notna() & (df_res['prix'] > 0)].copy()
df_res  = df_res[df_res['surface_m2'].fillna(0) > 5].copy()
df_res  = df_res.reset_index(drop=True)

# ── Stats finales ──────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  DATASET RÉSIDENTIEL FINAL")
print(f"{'='*55}")
print(f"  Total lignes       : {len(df_res):,}  (avant={n_avant:,})")
print(f"  Colonnes           : {len(df_res.columns)}")
print(f"\n  Par type_bien :")
for tb, label in {1:'Appartement', 2:'Maison', 3:'Villa'}.items():
    n = (df_res['type_bien']==tb).sum()
    print(f"    {label:<15} : {n:,}")

print(f"\n  Par type_transaction :")
for tt, label in {1:'Location', 2:'Vente'}.items():
    n = (df_res['type_transaction']==tt).sum()
    print(f"    {label:<15} : {n:,}")

print(f"\n  Prix médian vente    : {df_res[df_res['type_transaction']==2]['prix'].median():,.0f} TND")
print(f"  Prix médian location : {df_res[df_res['type_transaction']==1]['prix'].median():,.0f} TND")

# Corrélations type_bien et type_transaction avec prix
log_p = np.log1p(df_res['prix'])
c1 = df_res['type_bien'].corr(log_p)
c2 = df_res['type_transaction'].corr(log_p)
print(f"\n  Corr type_bien×prix        : {c1:+.4f}")
print(f"  Corr type_transaction×prix : {c2:+.4f}")

# ── Sauvegarde ─────────────────────────────────────────────────
out = os.path.join(_HERE, 'dataset_residentiel.csv')
df_res.to_csv(out, index=False)
print(f"\n  Sauvegardé : {out}")
print(f"  Taille     : {os.path.getsize(out)/1024/1024:.1f} MB")
print(f"\nLancez maintenant : python -m BO2.valuation_agent.train_model")
