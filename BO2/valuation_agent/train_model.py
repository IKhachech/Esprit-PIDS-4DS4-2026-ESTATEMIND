"""
train_model.py — XGBoost + BERT — Architecture unifiée
Emplacement : BO2/valuation_agent/
Lancer      : python -m BO2.valuation_agent.train_model

Datasets :
  dataset_residentiel.csv  → un seul modèle résidentiel
  dataset_terrain.csv      → modèle foncier séparé

Modèles produits dans agent/ :
  xgb_residentiel.json     — vente + location + appart + maison + villa
  xgb_foncier.json         — terrain
"""
import pandas as pd
import numpy as np
import sys, os, json, pickle, math

_HERE     = os.path.dirname(os.path.abspath(__file__))
_AGENTS   = os.path.dirname(os.path.dirname(_HERE))
sys.path.insert(0, _AGENTS)

from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.metrics import r2_score, mean_absolute_error
from sentence_transformers import SentenceTransformer

# ================================================================
# CONFIG
# ================================================================
DATA_DIR  = _HERE          # CSV dans BO2/valuation_agent/
SAVED_DIR = os.path.join(_HERE, 'agent')   # modèles dans agent/
N_BERT    = 8
CYCLE_MULT = {0:1.0, 1:0.90, 2:1.05, 3:1.10, 4:0.95, 5:0.85}

# type_bien labels (pour BERT texte)
TYPE_BIEN_LABELS = {1:'appartement', 2:'maison', 3:'villa'}
TYPE_TX_LABELS   = {1:'location', 2:'vente'}

# Features équipements
HAS_FEATURES = [
    'has_ascenseur','has_meuble','has_terrasse','has_piscine',
    'has_vue_mer','has_garage','has_securite','has_climatisation',
    'has_chauffage','has_cuisine_equip','has_double_vitrage',
    'has_porte_blindee','has_concierge','has_jardin','has_standing',
    'has_vue_montagne','has_cheminee',
]

# Centres gouvernorats
GOV_CENTRES = {
    1:(36.865,10.160),2:(36.730,9.182),3:(36.752,10.228),4:(37.274,9.865),
    5:(33.883,10.097),6:(34.425,8.784),7:(36.501,8.727),8:(35.678,10.100),
    9:(35.167,8.833),10:(33.705,8.971),11:(36.182,8.710),12:(35.504,11.062),
    13:(36.831,10.037),14:(33.354,10.500),15:(35.767,10.812),16:(36.456,10.735),
    17:(34.740,10.760),18:(35.040,9.485),19:(36.085,9.370),20:(35.825,10.636),
    21:(32.929,10.452),22:(33.919,8.139),23:(36.818,10.180),24:(36.403,10.143),
}

def haversine(lat1,lon1,lat2,lon2):
    R=6371; dlat=math.radians(lat2-lat1); dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R*2*math.asin(math.sqrt(max(0,min(1,a))))

print("=" * 60)
print("  ENTRAINEMENT XGBoost + BERT — Architecture unifiée")
print("  Résidentiel : dataset_residentiel.csv (1 modèle)")
print("  Foncier     : dataset_terrain.csv")
print("=" * 60)

os.makedirs(SAVED_DIR, exist_ok=True)

print("\nChargement BERT...")
st_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
print("BERT prêt.")

all_results = {}


# ================================================================
# FEATURE ENGINEERING
# ================================================================
def build_features(v, segment):
    """Construit toutes les features pour un segment."""

    # ── Micro-zones ───────────────────────────────────────────────
    v['micro_lat']  = v['lat'].fillna(36.8).round(2)
    v['micro_lon']  = v['lon'].fillna(10.2).round(2)
    v['micro_zone'] = (v['micro_lat']*1000 + v['micro_lon']).round(1)

    # ── Prix/m² par zone ─────────────────────────────────────────
    v['prix_m2_ville'] = v.groupby('ville_enc')['prix_m2'].transform('median').fillna(v['prix_m2'].median())
    v['prix_m2_gov']   = v.groupby('gouvernorat_enc')['prix_m2'].transform('median').fillna(v['prix_m2'].median())
    v['prix_m2_micro'] = v.groupby('micro_zone')['prix_m2'].transform('median').fillna(v['prix_m2_ville'])
    print(f"  prix_m2_ville : {v['ville_enc'].nunique()} villes | corr={v['prix_m2_ville'].corr(np.log1p(v['prix_m2'])):+.4f}")

    # ── Features de base ─────────────────────────────────────────
    v['surface_log']  = np.log1p(v['surface_m2'].fillna(50))
    v['market_score'] = v['score_attractivite'].fillna(0.5) * v['market_tension'].fillna(0.5)
    v['ville_x_surf'] = v['ville_enc'].fillna(0) * v['surface_log']

    has_cycle = 'cycle_marche' in v.columns
    v['cycle_marche']  = v['cycle_marche'].fillna(2) if has_cycle else 2
    v['cycle_factor']  = v['cycle_marche'].map(CYCLE_MULT).fillna(1.0)
    v['pm2_x_cycle']   = v['prix_m2_ville'] * v['cycle_factor']

    # ── Interactions surface × localisation ───────────────────────
    v['surf_x_ville']   = v['surface_log'] * (v['prix_m2_ville'] / (v['prix_m2_ville'].max()+1))
    v['surf_x_micro']   = v['surface_log'] * (v['prix_m2_micro'] / (v['prix_m2_micro'].max()+1))
    v['surf_x_attract'] = v['surface_log'] * v['score_attractivite'].fillna(0.5)

    # ── Score équipements composite ───────────────────────────────
    has_cols = [c for c in HAS_FEATURES if c in v.columns]
    weights = {
        'has_piscine':3,'has_vue_mer':3,'has_jardin':2,'has_garage':2,
        'has_standing':2,'has_concierge':2,'has_cheminee':2,
        'has_ascenseur':1,'has_meuble':1,'has_terrasse':1,'has_securite':1,
        'has_climatisation':1,'has_cuisine_equip':1,'has_chauffage':1,
        'has_double_vitrage':1,'has_porte_blindee':1,'has_vue_montagne':1,
    }
    if has_cols:
        total_w = sum(weights.get(c,1) for c in has_cols)
        v['score_equipements'] = sum(
            v[c].fillna(0)*weights.get(c,1) for c in has_cols
        ) / max(total_w, 1)

    # ── FEATS de base ─────────────────────────────────────────────
    FEATS = [
        'gouvernorat_enc','ville_enc','lat','lon',
        'micro_zone','micro_lat','micro_lon',
        'surface_m2','surface_log',
        'score_attractivite','market_tension','market_score',
        'prix_region_median','text_embedding_score',
        'prix_m2_ville','prix_m2_gov','prix_m2_micro','pm2_x_cycle',
        'surf_x_ville','surf_x_micro','surf_x_attract','ville_x_surf',
        'cycle_marche','cycle_factor',
    ]

    if 'score_equipements' in v.columns:
        FEATS.append('score_equipements')
    FEATS += has_cols

    if 'nb_images' in v.columns:
        FEATS.append('nb_images')

    # ── RESIDENTIEL : features spécifiques ────────────────────────
    if segment == 'residentiel':
        # type_bien et type_transaction comme features
        v['type_bien_f']        = v['type_bien'].fillna(1).astype(int)
        v['type_transaction_f'] = v['type_transaction'].fillna(2).astype(int)

        # Interactions type_bien × features
        v['type_x_surface']  = v['type_bien_f'] * v['surface_log']
        v['type_x_pm2ville'] = v['type_bien_f'] * (v['prix_m2_ville']/(v['prix_m2_ville'].max()+1))
        v['tx_x_pm2']        = v['type_transaction_f'] * v['pm2_x_cycle']

        # Pièces
        if 'nb_pieces' in v.columns:
            v['nb_pieces_f']   = v['nb_pieces'].fillna(3)
            v['nb_chambres_f'] = v['nb_chambres'].fillna(2) if 'nb_chambres' in v.columns else v['nb_pieces_f']-1
            v['pieces_surf']   = v['surface_log'] * v['nb_pieces_f']
            v['pm2_x_pieces']  = v['prix_m2_ville'] / np.maximum(v['nb_pieces_f'], 1)
            FEATS += ['nb_pieces_f','nb_chambres_f','pieces_surf','pm2_x_pieces']

        FEATS += ['type_bien_f','type_transaction_f',
                  'type_x_surface','type_x_pm2ville','tx_x_pm2']

    # ── FONCIER : features spécifiques ───────────────────────────
    elif segment == 'foncier':
        # type_terrain
        v['type_terrain'] = pd.cut(
            v['surface_m2'].fillna(500),
            bins=[0,200,1000,999_999], labels=[1,2,3]
        ).astype(float).fillna(2)

        # Distance centre
        v['dist_centre'] = v.apply(
            lambda r: haversine(float(r['lat']),float(r['lon']),
                                GOV_CENTRES.get(int(r['gouvernorat_enc']),(36.8,10.2))[0],
                                GOV_CENTRES.get(int(r['gouvernorat_enc']),(36.8,10.2))[1])
            if not pd.isna(r.get('lat')) else 10.0, axis=1).round(3)

        # Densité zone
        v['densite_zone'] = v.groupby('micro_zone')['micro_zone'].transform('count').fillna(1)

        # score_urbanisation composite
        pm2_max_f = v['prix_m2_micro'].max()+1
        den_max_f = v['densite_zone'].max()+1
        v['pm2_micro_norm']    = (v['prix_m2_micro']/pm2_max_f).clip(0,1)
        v['density_norm']      = (v['densite_zone'].astype(float)/den_max_f).clip(0,1)
        v['inv_dist']          = 1/(1+v['dist_centre'])
        v['log_dist']          = np.log1p(v['dist_centre'])
        v['score_urbanisation']= (
            0.5*v['pm2_micro_norm'] +
            0.3*v['density_norm']   +
            0.2*v['inv_dist']
        ).clip(0,1)

        # Interactions foncier
        v['urban_x_surface']   = v['score_urbanisation']*v['surface_log']
        v['urban_x_pm2']       = v['score_urbanisation']*v['pm2_micro_norm']
        v['density_x_surface'] = v['density_norm']*v['surface_log']
        v['dist_x_pm2']        = v['log_dist']*v['pm2_micro_norm']
        v['type_x_pm2']        = v['type_terrain'].astype(float)*v['pm2_micro_norm']
        v['surf_x_pm2micro']   = v['surface_log']*v['pm2_micro_norm']

        # Forcer float
        for _fc in ['type_terrain','dist_centre','densite_zone','score_urbanisation',
                    'pm2_micro_norm','density_norm','inv_dist','log_dist',
                    'surf_x_attract','urban_x_surface','urban_x_pm2',
                    'density_x_surface','dist_x_pm2','type_x_pm2','surf_x_pm2micro']:
            if _fc in v.columns:
                v[_fc] = pd.to_numeric(v[_fc], errors='coerce').fillna(0.0)

        FEATS += ['type_terrain','dist_centre','densite_zone','score_urbanisation',
                  'pm2_micro_norm','density_norm','inv_dist','log_dist',
                  'surf_x_attract','urban_x_surface','urban_x_pm2',
                  'density_x_surface','dist_x_pm2','type_x_pm2','surf_x_pm2micro']

        print(f"  type_terrain={v['type_terrain'].value_counts().to_dict()}")
        print(f"  score_urbanisation corr={v['score_urbanisation'].corr(np.log1p(v['prix_m2'])):+.4f}")

    return v, FEATS


# ================================================================
# ENTRAINEMENT
# ================================================================
def train_segment(df, segment, params):
    print(f"\n{'='*60}")
    print(f"  SEGMENT : {segment.upper()}")
    print(f"{'='*60}")

    v = df.copy()

    # Filtre surface foncier
    if segment == 'foncier':
        v = v[v['surface_m2'].fillna(0) < 50000].copy()

    # Prix/m²
    v['prix_m2'] = v['prix'] / np.maximum(v['surface_m2'].fillna(50), 1)
    print(f"  {len(v):,} lignes | prix médian={v['prix'].median():,.0f} | pm2={v['prix_m2'].median():,.0f}")

    # Filtre outliers
    p2,p98  = v['prix'].quantile(0.02), v['prix'].quantile(0.98)
    v = v[(v['prix']>=p2)&(v['prix']<=p98)].copy()
    pm2_p2,pm2_p98 = v['prix_m2'].quantile(0.02), v['prix_m2'].quantile(0.98)
    v = v[(v['prix_m2']>=pm2_p2)&(v['prix_m2']<=pm2_p98)].copy()
    v = v[v['surface_m2'].fillna(0)>5].copy().reset_index(drop=True)
    print(f"  Après filtrage : {len(v):,} lignes")

    if len(v) < 50:
        print("  [SKIP] Pas assez de données")
        return None, None

    # Feature engineering
    v, FEATS = build_features(v, segment)

    # ── BERT ──────────────────────────────────────────────────────
    def build_text(row):
        parts = []
        if segment == 'residentiel':
            tb = TYPE_BIEN_LABELS.get(int(row.get('type_bien',1)), 'bien')
            tt = TYPE_TX_LABELS.get(int(row.get('type_transaction',2)), 'vente')
            parts.append(f"{tb} {tt}")
        # Équipements
        equip = [c.replace('has_','').replace('_',' ')
                 for c in HAS_FEATURES if row.get(c,0)==1]
        if equip:
            parts.append(' '.join(equip))
        parts.append(f"surface {float(row.get('surface_m2',100)):.0f}m2")
        if segment == 'foncier':
            terrain = {1:'urbain',2:'mixte',3:'agricole'}.get(int(row.get('type_terrain',2)),'terrain')
            parts.append(terrain)
        return ' '.join(parts) if parts else 'bien immobilier tunisie'

    texts = v.apply(build_text, axis=1).tolist()
    embs  = st_model.encode(texts, batch_size=64, show_progress_bar=False, convert_to_numpy=True)

    n_comp   = min(N_BERT, len(v)-1, embs.shape[1])
    pca      = PCA(n_components=n_comp, random_state=42)
    bert_pca = pca.fit_transform(embs)
    variance = float(pca.explained_variance_ratio_.sum()*100)
    bert_cols= [f'bert_{i}' for i in range(n_comp)]
    df_bert  = pd.DataFrame(bert_pca, columns=bert_cols)

    # Dédupliquer FEATS
    FEATS_ALL = list(dict.fromkeys(FEATS + bert_cols))
    print(f"  BERT : {embs.shape[1]} → {n_comp} dims ({variance:.1f}%) | Total={len(FEATS_ALL)} features")

    v = pd.concat([v.reset_index(drop=True), df_bert], axis=1)

    # ── X / y ─────────────────────────────────────────────────────
    X = v[FEATS_ALL].copy()
    for col in FEATS_ALL:
        col_data = X[col]
        if isinstance(col_data, pd.DataFrame):
            col_data = col_data.iloc[:,0]
        if hasattr(col_data, 'cat'):
            col_data = col_data.astype(float)
        X[col] = pd.to_numeric(col_data, errors='coerce')
        med = X[col].median()
        X[col] = X[col].fillna(float(med) if pd.notna(med) else 0.0)

    # Sample weights équilibrés par gouvernorat
    gov_counts = v['gouvernorat_enc'].value_counts()
    n, n_govs  = len(v), v['gouvernorat_enc'].nunique()
    w = v['gouvernorat_enc'].apply(
        lambda g: float(np.clip((1/max(n_govs,1))/(gov_counts.get(g,1)/n), 0.2, 4.0))
    )
    w = w/w.mean()

    y_log  = np.log1p(v['prix_m2'])
    y_surf = v['surface_m2'].values
    y_raw  = v['prix']

    try:
        strat = pd.cut(y_log, bins=min(5,len(v)//10), labels=False)
        X_tr,X_te,yl_tr,yl_te,ys_tr,ys_te,yr_tr,yr_te,w_tr,_ = train_test_split(
            X,y_log,pd.Series(y_surf),y_raw,w,
            test_size=0.20,random_state=42,stratify=strat)
    except Exception:
        X_tr,X_te,yl_tr,yl_te,ys_tr,ys_te,yr_tr,yr_te,w_tr,_ = train_test_split(
            X,y_log,pd.Series(y_surf),y_raw,w,
            test_size=0.20,random_state=42)

    print(f"  Train={len(X_tr):,} | Test={len(X_te):,}")

    # ── XGBoost ───────────────────────────────────────────────────
    model = XGBRegressor(
        objective='reg:squarederror', subsample=0.8, colsample_bytree=0.7,
        reg_alpha=0.05, reg_lambda=1.5, random_state=42, n_jobs=-1, verbosity=0,
        **params
    )
    model.fit(X_tr, yl_tr, sample_weight=w_tr.values,
              eval_set=[(X_te,yl_te)], verbose=False)

    # ── Métriques ─────────────────────────────────────────────────
    pred_pm2   = np.expm1(model.predict(X_te))
    pred_total = pred_pm2 * ys_te.values
    r2_pm2     = float(r2_score(np.expm1(yl_te), pred_pm2))
    r2_total   = float(r2_score(yr_te, pred_total))
    mae_total  = float(mean_absolute_error(yr_te, pred_total))
    mape       = float(np.mean(np.abs((yr_te.values-pred_total)/np.clip(yr_te.values,1,None)))*100)

    fi   = model.feature_importances_
    top5 = dict(sorted({c:round(float(fi[i]/fi.sum()),3)
                for i,c in enumerate(FEATS_ALL)}.items(),key=lambda x:-x[1])[:5])

    print(f"\n  R² prix/m²   : {r2_pm2:.4f}")
    print(f"  R² prix total: {r2_total:.4f}  ← métrique principale")
    print(f"  MAE          : {mae_total:,.0f} TND")
    print(f"  MAPE         : {mape:.2f}%")
    print(f"  Top5         : {top5}")

    # ── Sauvegarde ────────────────────────────────────────────────
    model.save_model(os.path.join(SAVED_DIR, f'xgb_{segment}.json'))

    meta = {
        'mae':float(mae_total),'r2':float(r2_total),'mape':float(mape),
        'r2_pm2':float(r2_pm2),'n_train':int(len(X_tr)),'n_test':int(len(X_te)),
        'bert_dims':int(n_comp),'segment':segment,'target_mode':'prix_m2',
        'model':f'XGBoost+BERT_{segment}'
    }
    with open(os.path.join(SAVED_DIR,f'xgb_metrics_{segment}.json'),'w') as f:
        json.dump(meta,f,indent=2)
    with open(os.path.join(SAVED_DIR,f'feature_cols_{segment}.json'),'w') as f:
        json.dump(FEATS_ALL,f,indent=2)
    with open(os.path.join(SAVED_DIR,f'bert_pca_{segment}.pkl'),'wb') as f:
        pickle.dump(pca,f)

    # Stats prix/m²
    pm2_stats = {
        'median_pm2':        float(v['prix_m2'].median()),
        'prix_m2_ville_map': {str(k):float(val) for k,val in
                              v.groupby('ville_enc')['prix_m2'].median().items()},
        'prix_m2_gov_map':   {str(k):float(val) for k,val in
                              v.groupby('gouvernorat_enc')['prix_m2'].median().items()},
        'prix_m2_micro_map': {str(round(k,1)):float(val) for k,val in
                              v.groupby('micro_zone')['prix_m2'].median().items()},
    }
    with open(os.path.join(SAVED_DIR,f'pm2_stats_{segment}.json'),'w') as f:
        json.dump(pm2_stats,f,indent=2)

    print(f"  Sauvegardé : xgb_{segment}.json ✔")
    return model, {
        'r2_pm2':round(r2_pm2,4),'r2_total':round(r2_total,4),
        'mae':round(mae_total,2),'mape':round(mape,4),
        'n_train':len(X_tr),'n_test':len(X_te),'features':len(FEATS_ALL)
    }


# ================================================================
# 1. RESIDENTIEL — dataset_residentiel.csv
# ================================================================
res_path = os.path.join(DATA_DIR, 'dataset_residentiel.csv')
if not os.path.exists(res_path):
    raise FileNotFoundError(
        f"dataset_residentiel.csv introuvable dans {DATA_DIR}\n"
        f"Lancez d'abord : python merge_datasets.py"
    )

df_res = pd.read_csv(res_path)
print(f"\nDataset résidentiel : {len(df_res):,} lignes")
print(f"  Vente    : {(df_res['type_transaction']==2).sum():,}")
print(f"  Location : {(df_res['type_transaction']==1).sum():,}")

_, res = train_segment(
    df_res, 'residentiel',
    params={'n_estimators':1500,'learning_rate':0.02,'max_depth':6,'min_child_weight':5}
)
if res:
    all_results['residentiel'] = res

# ================================================================
# 2. FONCIER — dataset_terrain.csv
# ================================================================
ter_path = os.path.join(DATA_DIR, 'dataset_terrain.csv')
df_ter   = pd.read_csv(ter_path)
print(f"\nDataset terrain : {len(df_ter):,} lignes")

_, res = train_segment(
    df_ter, 'foncier',
    params={'n_estimators':1500,'learning_rate':0.015,'max_depth':6,'min_child_weight':2}
)
if res:
    all_results['foncier'] = res

# ================================================================
# RAPPORT FINAL
# ================================================================
print(f"\n{'='*60}")
print(f"  RAPPORT FINAL")
print(f"{'='*60}")
print(f"\n  {'Segment':<16} {'R²/m²':>8} {'R² total':>10} {'MAE':>12} {'MAPE':>8} {'N':>6}")
print(f"  {'-'*62}")
for seg, res in all_results.items():
    print(f"  {seg:<16} {res.get('r2_pm2',0):>8.4f} {res['r2_total']:>10.4f} "
          f"{res['mae']:>12,.0f} {res['mape']:>7.1f}% {res['n_train']:>6}")

print(f"\n  Segments non ML :")
print(f"  {'commercial':<16} → heuristique (données insuffisantes)")
print(f"  {'divers':<16} → fallback (non ML — exclu selon consigne prof)")

with open(os.path.join(SAVED_DIR,'all_metrics.json'),'w') as f:
    json.dump(all_results,f,indent=2)
with open(os.path.join(SAVED_DIR,'log_transform.json'),'w') as f:
    json.dump({'enabled':True,'target_mode':'prix_m2','multi_model':True},f)

print(f"\n  Modèles → {SAVED_DIR}/")
print(f"\nTestez : python -m BO2.valuation_agent.agent")
