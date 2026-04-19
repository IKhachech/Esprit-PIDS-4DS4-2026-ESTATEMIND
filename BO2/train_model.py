"""
train_model.py — XGBoost + BERT — Architecture unifiee
EstateMind BO2

Datasets :
    dataset_residentiel.csv  → Appartements + Maisons + Villas
    dataset_terrain.csv      → Terrains

Lance :
    python train_model.py

Produit dans agent/ :
    xgb_residentiel.json
    xgb_foncier.json
    + fichiers meta JSON
"""

import pandas as pd
import numpy as np
import os, json, pickle, math, warnings
warnings.filterwarnings('ignore')

from xgboost import XGBRegressor
try:
    import lightgbm as lgb
    from lightgbm import LGBMRegressor
    USE_LGBM = True
    print('  LightGBM disponible ✔')
except ImportError:
    USE_LGBM = False
    print('  LightGBM absent → XGBoost utilise')
    print('  Pour installer : pip install lightgbm')
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.metrics import r2_score, mean_absolute_error
from sentence_transformers import SentenceTransformer

# ================================================================
# CONFIG
# ================================================================

DATA_DIR  = '.'
SAVED_DIR = os.path.join('.', 'agent')
os.makedirs(SAVED_DIR, exist_ok=True)

N_BERT     = 8

DEFAULTS_SC = {
    'etat_score': 1, 'etat_prob_renove': 0.9,
    'standing_score': 2, 'standing_prob_luxe': 0.5,
}
CYCLE_MULT = {0:1.0, 1:0.90, 2:1.05, 3:1.10, 4:0.95, 5:0.85}

TYPE_BIEN_LABELS = {1:'appartement', 2:'maison', 3:'villa'}
TYPE_TX_LABELS   = {1:'location', 2:'vente'}

HAS_FEATURES = [
    'has_ascenseur','has_meuble','has_terrasse','has_piscine',
    'has_vue_mer','has_garage','has_securite','has_climatisation',
    'has_chauffage','has_cuisine_equip','has_double_vitrage',
    'has_porte_blindee','has_concierge','has_jardin','has_standing',
    'has_vue_montagne','has_cheminee',
]

# Features EfficientNet (produites par score_dataset.py)
EFFICIENTNET_FEATURES = [
    'etat_score',         # 0=ancien | 1=renove
    'etat_prob_renove',   # probabilite renove (0-1)
    'standing_score',     # 1=modeste | 2=standard | 3=luxe
    'standing_prob_luxe', # probabilite luxe (0-1)
]

GOV_CENTRES = {
    1:(36.865,10.160), 2:(36.730,9.182),  3:(36.752,10.228), 4:(37.274,9.865),
    5:(33.883,10.097), 6:(34.425,8.784),  7:(36.501,8.727),  8:(35.678,10.100),
    9:(35.167,8.833),  10:(33.705,8.971), 11:(36.182,8.710), 12:(35.504,11.062),
    13:(36.831,10.037),14:(33.354,10.500),15:(35.767,10.812),16:(36.456,10.735),
    17:(34.740,10.760),18:(35.040,9.485), 19:(36.085,9.370), 20:(35.825,10.636),
    21:(32.929,10.452),22:(33.919,8.139), 23:(36.818,10.180),24:(36.403,10.143),
}

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')


# ================================================================
# HELPERS
# ================================================================

def haversine(lat1, lon1, lat2, lon2):
    R    = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a    = (math.sin(dlat/2)**2 +
            math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
            math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(max(0, min(1, a))))


def ensure_columns(df):
    """
    Garantit que toutes les colonnes necessaires sont presentes.
    Compatible avec anciens (6 CSV) et nouveaux (2 CSV) fichiers.
    """
    # Colonnes BO2 optionnelles — mettre a 0 si absentes
    bo2_cols = {
        'lat':                  36.8,
        'lon':                  10.1,
        'score_attractivite':   0.5,
        'market_tension':       0.5,
        'cycle_marche':         2.0,
        'prix_region_median':   300000.0,
        'text_embedding_score': 0.5,
        'ville_enc':            0.3,
        'source_enc':           0,
        'nb_images':            1,
    }
    for col, default in bo2_cols.items():
        if col not in df.columns:
            df[col] = default
            log(f'  Colonne {col} absente → defaut={default}')

    # Colonnes has_ → 0 si absentes
    for col in HAS_FEATURES:
        if col not in df.columns:
            df[col] = 0

    # Colonnes EfficientNet → 0 si absentes (dataset non score)
    efficientnet_defaults = {
        'etat_score': 1, 'etat_prob_renove': 0.9,
        'standing_score': 2, 'standing_prob_luxe': 0.5,
        'quartier_enc': 0.3,
        'nb_sdb': 0,
    }
    for col, default in efficientnet_defaults.items():
        if col not in df.columns:
            df[col] = default

    # type_transaction : anciens fichiers ont 0=vente 1=location
    # nouveaux fichiers ont 2=vente 1=location
    if 'type_transaction' in df.columns:
        unique_tt = df['type_transaction'].dropna().unique()
        if 0 in unique_tt and 2 not in unique_tt:
            # Ancien format : 0→2, 1→1
            df['type_transaction'] = df['type_transaction'].map({0:2, 1:1}).fillna(2)
            log('  type_transaction converti : 0→2(vente), 1→1(location)')

    return df


# ================================================================
# FEATURE ENGINEERING
# ================================================================

def build_features(v, segment, ville_map_key=None):
    # Micro-zones GPS
    v['micro_lat']  = v['lat'].fillna(36.8).round(2)
    v['micro_lon']  = v['lon'].fillna(10.2).round(2)
    v['micro_zone'] = (v['micro_lat'] * 1000 + v['micro_lon']).round(1)

    # Features de base — sans leakage prix/m2
    v['surface_log']    = np.log1p(v['surface_m2'].fillna(50))
    v['market_score']   = v['score_attractivite'].fillna(0.5) * v['market_tension'].fillna(0.5)
    v['ville_x_surf']   = v['ville_enc'].fillna(0) * v['surface_log']
    v['cycle_marche']   = v['cycle_marche'].fillna(2)
    v['cycle_factor']   = v['cycle_marche'].map(CYCLE_MULT).fillna(1.0)
    v['surf_x_attract'] = v['surface_log'] * v['score_attractivite'].fillna(0.5)
    v['ville_x_attract']= v['ville_enc'].fillna(0) * v['score_attractivite'].fillna(0.5)
    v['log_prix_region']= np.log1p(v['prix_region_median'].fillna(300000))

    # Score equipements composite
    has_cols = [c for c in HAS_FEATURES if c in v.columns]
    weights = {
        'has_piscine':3, 'has_vue_mer':3, 'has_jardin':2, 'has_garage':2,
        'has_standing':2,'has_concierge':2,'has_cheminee':2,
        'has_ascenseur':1,'has_meuble':1,'has_terrasse':1,'has_securite':1,
        'has_climatisation':1,'has_cuisine_equip':1,'has_chauffage':1,
        'has_double_vitrage':1,'has_porte_blindee':1,'has_vue_montagne':1,
    }
    if has_cols:
        total_w = sum(weights.get(c, 1) for c in has_cols)
        v['score_equipements'] = sum(
            v[c].fillna(0) * weights.get(c, 1) for c in has_cols
        ) / max(total_w, 1)

    # quartier_enc — plus precis que ville_enc
    if 'quartier_enc' in v.columns:
        v['quartier_x_attract'] = v['quartier_enc'].fillna(0) * v['score_attractivite'].fillna(0.5)
        v['quartier_x_surf']    = v['quartier_enc'].fillna(0) * v['surface_log']

    FEATS = [
        'gouvernorat_enc', 'ville_enc', 'lat', 'lon',
        'micro_zone', 'micro_lat', 'micro_lon',
        'surface_m2', 'surface_log',
        'score_attractivite', 'market_tension', 'market_score',
        'prix_region_median', 'log_prix_region', 'text_embedding_score',
        'surf_x_attract', 'ville_x_surf', 'ville_x_attract',
        'cycle_marche', 'cycle_factor',
    ]
    # Ajouter quartier_enc si disponible
    if 'quartier_enc' in v.columns:
        FEATS += ['quartier_enc', 'quartier_x_attract', 'quartier_x_surf']
    # Ajouter nb_sdb si disponible
    if 'nb_sdb' in v.columns:
        FEATS.append('nb_sdb')
    if 'score_equipements' in v.columns:
        FEATS.append('score_equipements')
    FEATS += has_cols
    if 'nb_images' in v.columns:
        FEATS.append('nb_images')
    for col in EFFICIENTNET_FEATURES:
        if col in v.columns:
            FEATS.append(col)

    # RESIDENTIEL
    if segment in ['residentiel', 'residentiel_vente', 'residentiel_location']:
        v['type_bien_f']    = v['type_bien'].fillna(1).astype(int)
        v['type_x_surface'] = v['type_bien_f'] * v['surface_log']
        v['type_x_ville']   = v['type_bien_f'] * v['ville_enc'].fillna(0)
        v['surf_x_ville']   = v['surface_log'] * v['ville_enc'].fillna(0)

        if 'nb_pieces' in v.columns:
            v['nb_pieces_f']    = v['nb_pieces'].fillna(3)
            v['nb_chambres_f']  = v['nb_chambres'].fillna(2) if 'nb_chambres' in v.columns                                   else v['nb_pieces_f'] - 1
            v['pieces_surf']    = v['surface_log'] * v['nb_pieces_f']
            v['surf_per_piece'] = v['surface_m2'].fillna(50) / np.maximum(v['nb_pieces_f'], 1)
            FEATS += ['nb_pieces_f', 'nb_chambres_f', 'pieces_surf', 'surf_per_piece']

        # type_transaction seulement pour modele unifie
        if segment == 'residentiel':
            v['type_transaction_f'] = v['type_transaction'].fillna(2).astype(int)
            FEATS.append('type_transaction_f')

        FEATS += ['type_bien_f', 'type_x_surface', 'type_x_ville', 'surf_x_ville']

    # FONCIER
    elif segment == 'foncier':
        v['type_terrain'] = pd.cut(
            v['surface_m2'].fillna(500),
            bins=[0, 200, 1000, 999_999], labels=[1, 2, 3]
        ).astype(float).fillna(2)

        v['dist_centre'] = v.apply(
            lambda r: haversine(
                float(r['lat']) if pd.notna(r.get('lat')) else 36.8,
                float(r['lon']) if pd.notna(r.get('lon')) else 10.1,
                GOV_CENTRES.get(int(r['gouvernorat_enc']), (36.8, 10.1))[0],
                GOV_CENTRES.get(int(r['gouvernorat_enc']), (36.8, 10.1))[1]
            ), axis=1
        ).round(3)

        v['densite_zone']       = v.groupby('micro_zone')['micro_zone'].transform('count').fillna(1)
        den_max_f               = v['densite_zone'].max() + 1
        v['density_norm']       = (v['densite_zone'].astype(float) / den_max_f).clip(0, 1)
        v['inv_dist']           = 1 / (1 + v['dist_centre'])
        v['log_dist']           = np.log1p(v['dist_centre'])
        v['score_urbanisation'] = (
            0.5 * v['ville_enc'].fillna(0.3) +
            0.3 * v['density_norm'] +
            0.2 * v['inv_dist']
        ).clip(0, 1)
        v['urban_x_surface']   = v['score_urbanisation'] * v['surface_log']
        v['density_x_surface'] = v['density_norm'] * v['surface_log']
        v['surf_x_ville_fon']  = v['surface_log'] * v['ville_enc'].fillna(0)

        for fc in ['type_terrain','dist_centre','densite_zone','score_urbanisation',
                   'density_norm','inv_dist','log_dist',
                   'urban_x_surface','density_x_surface','surf_x_ville_fon']:
            if fc in v.columns:
                v[fc] = pd.to_numeric(v[fc], errors='coerce').fillna(0.0)

        FEATS += ['type_terrain','dist_centre','densite_zone','score_urbanisation',
                  'density_norm','inv_dist','log_dist','surf_x_attract',
                  'urban_x_surface','density_x_surface','surf_x_ville_fon']
        log(f'  score_urbanisation corr={v["score_urbanisation"].corr(np.log1p(v["prix_m2"])):+.4f}')

    return v, FEATS

def train_segment(df, segment, params, ville_map_key=None):
    section(f'SEGMENT : {segment.upper()}')

    v = df.copy()
    v = ensure_columns(v)

    # Surface foncier max
    if segment == 'foncier':
        v = v[v['surface_m2'].fillna(0) < 50000].copy()

    # Prix/m²
    v['prix_m2'] = v['prix'] / np.maximum(v['surface_m2'].fillna(50), 1)
    log(f'{len(v):,} lignes | prix median={v["prix"].median():,.0f} | pm2 median={v["prix_m2"].median():,.0f}')

    # Filtre outliers 2%-98%
    p2,  p98  = v['prix'].quantile(0.02),    v['prix'].quantile(0.98)
    pm2, pm98 = v['prix_m2'].quantile(0.02), v['prix_m2'].quantile(0.98)
    v = v[(v['prix']    >= p2)  & (v['prix']    <= p98)].copy()
    v = v[(v['prix_m2'] >= pm2) & (v['prix_m2'] <= pm98)].copy()
    v = v[v['surface_m2'].fillna(0) > 5].reset_index(drop=True)
    log(f'Apres filtrage : {len(v):,} lignes')

    if len(v) < 50:
        log('[SKIP] Pas assez de donnees')
        return None, None

    # Feature engineering
    v, FEATS = build_features(v, segment, ville_map_key)

    # ── BERT ────────────────────────────────────────────────────
    log('Construction textes BERT...')
    def build_text(row):
        parts = []
        if segment in ['residentiel', 'residentiel_vente', 'residentiel_location']:
            tb = TYPE_BIEN_LABELS.get(int(row.get('type_bien', 1)), 'bien')
            tt = TYPE_TX_LABELS.get(int(row.get('type_transaction', 2)), 'vente')
            parts.append(f'{tb} {tt}')
        equip = [c.replace('has_', '').replace('_', ' ')
                 for c in HAS_FEATURES if row.get(c, 0) == 1]
        if equip:
            parts.append(' '.join(equip))
        parts.append(f"surface {float(row.get('surface_m2', 100)):.0f}m2")
        if segment == 'foncier':
            terrain = {1:'urbain', 2:'mixte', 3:'agricole'}.get(
                int(row.get('type_terrain', 2)), 'terrain')
            parts.append(terrain)
        return ' '.join(parts) if parts else 'bien immobilier tunisie'

    texts    = v.apply(build_text, axis=1).tolist()
    embs     = st_model.encode(texts, batch_size=64, show_progress_bar=False, convert_to_numpy=True)
    n_comp   = min(N_BERT, len(v) - 1, embs.shape[1])
    pca      = PCA(n_components=n_comp, random_state=42)
    bert_pca = pca.fit_transform(embs)
    variance = float(pca.explained_variance_ratio_.sum() * 100)
    bert_cols= [f'bert_{i}' for i in range(n_comp)]
    df_bert  = pd.DataFrame(bert_pca, columns=bert_cols)
    FEATS_ALL= list(dict.fromkeys(FEATS + bert_cols))
    log(f'BERT : {embs.shape[1]} → {n_comp} dims ({variance:.1f}%) | Total initial={len(FEATS_ALL)} features')

    v = pd.concat([v.reset_index(drop=True), df_bert], axis=1)

    # ── Features CNN (ResNet50) ──────────────────────────────────
    cnn_cols = []
    # Mapping segment → fichier npy
    npy_map  = {
        'residentiel_vente':    'image_features_residentiel.npy',
        'residentiel_location': 'image_features_residentiel.npy',
        'foncier':              'image_features_terrain.npy',
    }
    npy_file = npy_map.get(segment, f'image_features_{segment}.npy')
    if os.path.exists(npy_file):
        img_all  = np.load(npy_file)
        # Aligner sur les indices originaux du dataframe
        orig_idx = v.index.tolist() if hasattr(v, 'index') else list(range(len(v)))
        try:
            # Les indices peuvent depasser la taille du npy si subset
            valid_idx = [i for i in orig_idx if i < len(img_all)]
            if len(valid_idx) == len(orig_idx):
                X_img = img_all[orig_idx]
            else:
                X_img = np.zeros((len(v), img_all.shape[1]))
                for pos, idx in enumerate(orig_idx):
                    if idx < len(img_all):
                        X_img[pos] = img_all[idx]
        except Exception:
            X_img = np.zeros((len(v), img_all.shape[1]))
        n_img_valid = (X_img.any(axis=1)).sum()
        n_img_feat  = X_img.shape[1]
        cnn_cols    = [f'cnn_{i}' for i in range(n_img_feat)]
        df_cnn      = pd.DataFrame(X_img, columns=cnn_cols)
        v           = pd.concat([v.reset_index(drop=True), df_cnn.reset_index(drop=True)], axis=1)
        FEATS_ALL   = list(dict.fromkeys(FEATS_ALL + cnn_cols))
        log(f'CNN : shape={X_img.shape} | images valides={n_img_valid} ({n_img_valid/len(v)*100:.0f}%)')
    else:
        log(f'⚠ {npy_file} absent → CNN non utilise')

    # ── X / y ───────────────────────────────────────────────────
    X = v[FEATS_ALL].copy()
    for col in FEATS_ALL:
        col_data = X[col]
        if isinstance(col_data, pd.DataFrame): col_data = col_data.iloc[:, 0]
        if hasattr(col_data, 'cat'):           col_data = col_data.astype(float)
        X[col] = pd.to_numeric(col_data, errors='coerce')
        med = X[col].median()
        X[col] = X[col].fillna(float(med) if pd.notna(med) else 0.0)

    # Sample weights equilibres par gouvernorat
    gov_counts = v['gouvernorat_enc'].value_counts()
    n, n_govs  = len(v), v['gouvernorat_enc'].nunique()
    w = v['gouvernorat_enc'].apply(
        lambda g: float(np.clip((1 / max(n_govs, 1)) / (gov_counts.get(g, 1) / n), 0.2, 4.0))
    )
    w = w / w.mean()

    y_log  = np.log1p(v['prix_m2'])
    y_surf = v['surface_m2'].values
    y_raw  = v['prix']

    try:
        strat = pd.cut(y_log, bins=min(5, len(v) // 10), labels=False)
        X_tr, X_te, yl_tr, yl_te, ys_tr, ys_te, yr_tr, yr_te, w_tr, _ = train_test_split(
            X, y_log, pd.Series(y_surf), y_raw, w,
            test_size=0.20, random_state=42, stratify=strat)
    except Exception:
        X_tr, X_te, yl_tr, yl_te, ys_tr, ys_te, yr_tr, yr_te, w_tr, _ = train_test_split(
            X, y_log, pd.Series(y_surf), y_raw, w,
            test_size=0.20, random_state=42)

    log(f'Train={len(X_tr):,} | Test={len(X_te):,}')

    # ── Modele : LightGBM si disponible sinon XGBoost ───────────
    if USE_LGBM:
        lgbm_params = {
            'n_estimators':    params.get('n_estimators', 1500),
            'learning_rate':   params.get('learning_rate', 0.02),
            'max_depth':       params.get('max_depth', 6),
            'min_child_samples': params.get('min_child_weight', 5),
            'num_leaves':      63,
            'subsample':       0.8,
            'colsample_bytree':0.7,
            'reg_alpha':       0.05,
            'reg_lambda':      1.5,
            'random_state':    42,
            'n_jobs':         -1,
            'verbose':        -1,
        }
        model = LGBMRegressor(**lgbm_params)
        model.fit(
            X_tr, yl_tr,
            sample_weight=w_tr.values,
            eval_set=[(X_te, yl_te)],
            callbacks=[lgb.early_stopping(50, verbose=False),
                       lgb.log_evaluation(-1)]
        )
        log('Modele : LightGBM')
    else:
        model = XGBRegressor(
            objective='reg:squarederror',
            subsample=0.8, colsample_bytree=0.7,
            reg_alpha=0.05, reg_lambda=1.5,
            random_state=42, n_jobs=-1, verbosity=0,
            **params
        )
        model.fit(X_tr, yl_tr, sample_weight=w_tr.values,
                  eval_set=[(X_te, yl_te)], verbose=False)
        log('Modele : XGBoost')

    # ── Metriques ────────────────────────────────────────────────
    pred_pm2   = np.expm1(model.predict(X_te))
    pred_total = pred_pm2 * ys_te.values
    r2_pm2     = float(r2_score(np.expm1(yl_te), pred_pm2))
    r2_total   = float(r2_score(yr_te, pred_total))
    mae_total  = float(mean_absolute_error(yr_te, pred_total))
    mape       = float(np.mean(np.abs((yr_te.values - pred_total) /
                                      np.clip(yr_te.values, 1, None))) * 100)

    fi   = model.feature_importances_
    top5 = dict(sorted({c: round(float(fi[i] / fi.sum()), 3)
                        for i, c in enumerate(FEATS_ALL)}.items(),
                       key=lambda x: -x[1])[:5])

    log(f'R2 prix/m2   : {r2_pm2:.4f}')
    log(f'R2 prix total: {r2_total:.4f}  <- metrique principale')
    log(f'MAE          : {mae_total:,.0f} TND')
    log(f'MAPE         : {mape:.2f}%')
    log(f'Top5         : {top5}')

    # ── Sauvegarde ───────────────────────────────────────────────
    if USE_LGBM:
        model.booster_.save_model(os.path.join(SAVED_DIR, f'lgbm_{segment}.txt'))
        # Aussi sauvegarder en format compatible pour agent
        import pickle
        with open(os.path.join(SAVED_DIR, f'lgbm_{segment}.pkl'), 'wb') as f:
            pickle.dump(model, f)
    else:
        model.save_model(os.path.join(SAVED_DIR, f'xgb_{segment}.json'))

    meta = {
        'mae': float(mae_total), 'r2': float(r2_total), 'mape': float(mape),
        'r2_pm2': float(r2_pm2), 'n_train': int(len(X_tr)), 'n_test': int(len(X_te)),
        'bert_dims': int(n_comp), 'segment': segment,
        'target_mode': 'prix_m2', 'model': f'XGBoost+BERT_{segment}'
    }
    with open(os.path.join(SAVED_DIR, f'xgb_metrics_{segment}.json'), 'w') as f:
        json.dump(meta, f, indent=2)
    with open(os.path.join(SAVED_DIR, f'feature_cols_{segment}.json'), 'w') as f:
        json.dump(FEATS_ALL, f, indent=2)
    with open(os.path.join(SAVED_DIR, f'bert_pca_{segment}.pkl'), 'wb') as f:
        pickle.dump(pca, f)

    pm2_stats = {
        'median_pm2':      float(v['prix_m2'].median()),
        'pm2_by_gov':      {str(k): float(val) for k, val in
                            v.groupby('gouvernorat_enc')['prix_m2'].median().items()},
        'pm2_by_ville_enc':{str(round(k,4)): float(val) for k, val in
                            v.groupby('ville_enc')['prix_m2'].median().items()},
    }
    with open(os.path.join(SAVED_DIR, f'pm2_stats_{segment}.json'), 'w') as f:
        json.dump(pm2_stats, f, indent=2)

    fname = f'lgbm_{segment}.pkl' if USE_LGBM else f'xgb_{segment}.json'
    log(f'Sauvegarde : agent/{fname} ✔')

    return model, {
        'r2_pm2':   round(r2_pm2, 4),
        'r2_total': round(r2_total, 4),
        'mae':      round(mae_total, 2),
        'mape':     round(mape, 4),
        'n_train':  len(X_tr),
        'n_test':   len(X_te),
        'features': len(FEATS_ALL),
    }


# ================================================================
# MAIN
# ================================================================

section('TRAIN_MODEL.PY — XGBoost + BERT + CLIP + EfficientNet')
log('EstateMind BO2 — Architecture unifiee')

# Charger BERT
log('Chargement BERT (paraphrase-multilingual-MiniLM-L12-v2)...')
st_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
log('BERT pret.')

all_results = {}

# ── 1. RESIDENTIEL VENTE ────────────────────────────────────────
res_path = os.path.join(DATA_DIR, 'dataset_residentiel.csv')
if not os.path.exists(res_path):
    print(f'\nERREUR : dataset_residentiel.csv introuvable.')
    print('Lancez d\'abord : python prepare.py')
else:
    df_res = pd.read_csv(res_path)
    log(f'Dataset residentiel : {len(df_res):,} lignes')

    if 'type_bien' not in df_res.columns:
        log('ERREUR : colonne type_bien absente')
    else:
            # 2 modeles separes vente et location
            # ville_enc correct par transaction depuis prepare.py

            # ── VENTE ────────────────────────────────────────────
            vente_path = os.path.join(DATA_DIR, 'dataset_residentiel_vente.csv')
            df_vente = pd.read_csv(vente_path) if os.path.exists(vente_path)                        else df_res[df_res['type_transaction'] == 2].copy()
            log(f'Vente : {len(df_vente):,} lignes')
            _, res = train_segment(
                df_vente, 'residentiel_vente',
                params={'n_estimators': 1500, 'learning_rate': 0.02,
                        'max_depth': 6, 'min_child_weight': 5},
                ville_map_key='residentiel_vente'
            )
            if res: all_results['residentiel_vente'] = res

            # ── LOCATION ─────────────────────────────────────────
            loc_path = os.path.join(DATA_DIR, 'dataset_residentiel_location.csv')
            df_loc = pd.read_csv(loc_path) if os.path.exists(loc_path)                      else df_res[df_res['type_transaction'] == 1].copy()
            log(f'Location : {len(df_loc):,} lignes')
            _, res = train_segment(
                df_loc, 'residentiel_location',
                params={'n_estimators': 1500, 'learning_rate': 0.02,
                        'max_depth': 6, 'min_child_weight': 5},
                ville_map_key='residentiel_location'
            )
            if res: all_results['residentiel_location'] = res

# ── 2. FONCIER ──────────────────────────────────────────────────
ter_path = os.path.join(DATA_DIR, 'dataset_terrain.csv')
if not os.path.exists(ter_path):
    print(f'\nERREUR : dataset_terrain.csv introuvable.')
    print('Lancez d\'abord : python prepare.py')
else:
    df_ter = pd.read_csv(ter_path)
    log(f'Dataset terrain : {len(df_ter):,} lignes')
    _, res = train_segment(
        df_ter, 'foncier',
        params={'n_estimators': 1500, 'learning_rate': 0.015,
                'max_depth': 6, 'min_child_weight': 2}
    )
    if res: all_results['foncier'] = res

# ── RAPPORT FINAL ────────────────────────────────────────────────
section('RAPPORT FINAL')
print(f'\n  {"Segment":<16} {"R2/m2":>8} {"R2 total":>10} '
      f'{"MAE":>12} {"MAPE":>8} {"N train":>8}')
print('  ' + '-'*65)
for seg, res in all_results.items():
    print(f'  {seg:<16} {res["r2_pm2"]:>8.4f} {res["r2_total"]:>10.4f} '
          f'{res["mae"]:>12,.0f} {res["mape"]:>7.1f}% {res["n_train"]:>8,}')

with open(os.path.join(SAVED_DIR, 'all_metrics.json'), 'w') as f:
    json.dump(all_results, f, indent=2)

log(f'\nModeles sauvegardes dans : agent/')
log('Prochaine etape : python agent_bo2.py')