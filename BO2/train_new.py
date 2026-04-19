"""
train_new.py — Entraînement XGBoost multimodal + Régression Bayésienne
EstateMind BO2 — Tunisian Real Estate

Modèles par segment :
    1. XGBoost tabulaire seul
    2. XGBoost image CNN seule
    3. XGBoost tabulaire + CNN (fusion)
    4. Régression Bayésienne → fourchette de confiance

Outputs :
    models_new/xgb_{segment}.pkl
    models_new/results_new.json
"""

import os, json, pickle, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split
from sklearn.linear_model import BayesianRidge
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor

# ================================================================
# CONFIG
# ================================================================

SEGMENTS = {
    'appart_vente':    'dataset_appart_vente.csv',
    'appart_location': 'dataset_appart_location.csv',
    'maison_vente':    'dataset_maison_vente.csv',
    'maison_location': 'dataset_maison_location.csv',
    'villa':           'dataset_villa.csv',
    'terrain':         'dataset_terrain.csv',
}

MODELS_DIR = 'models_new'
os.makedirs(MODELS_DIR, exist_ok=True)

XGB_PARAMS = dict(
    n_estimators         = 500,
    learning_rate        = 0.05,
    max_depth            = 6,
    subsample            = 0.8,
    colsample_bytree     = 0.8,
    random_state         = 42,
    n_jobs               = -1,
    verbosity            = 0,
    early_stopping_rounds= 30,
)

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')


# ================================================================
# METRIQUES
# ================================================================

def compute_metrics(y_true, y_pred, label=''):
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    if label:
        log(f'  {label:<30} | R2={r2:>7.4f} | MAE={mae:>12,.0f} TND')
    return {'r2': round(r2, 4), 'mae': round(mae, 2), 'rmse': round(rmse, 2)}


# ================================================================
# CHARGEMENT
# ================================================================

def load_segment(seg_name, csv_file):
    df = pd.read_csv(csv_file)

    for col in df.columns:
        if col != 'image_url':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['prix'])

    # Features tabulaires = tout sauf prix et image_url
    tab_cols = [c for c in df.columns if c not in ['prix', 'image_url']]
    df[tab_cols] = df[tab_cols].fillna(df[tab_cols].median())

    y     = np.log1p(df['prix'].values)
    X_tab = df[tab_cols].values

    # Features CNN
    npy_file = f'image_features_{seg_name}.npy'
    if os.path.exists(npy_file):
        img_all = np.load(npy_file)
        X_img   = img_all[df.index]
        log(f'  Features CNN : {X_img.shape} | images valides : {X_img.any(axis=1).sum()}')
    else:
        log(f'  npy introuvable — zeros')
        X_img = np.zeros((len(df), 50))

    X_full = np.concatenate([X_tab, X_img], axis=1)

    log(f'  Lignes={len(df)} | X_tab={X_tab.shape} | X_full={X_full.shape}')
    return X_tab, X_img, X_full, y, tab_cols


# ================================================================
# ENTRAINEMENT
# ================================================================

def train_segment(seg_name, X_tab, X_img, X_full, y, tab_cols):
    n = len(y)
    if n < 50:
        log('  Trop peu de données')
        return None, None, None

    idx = np.arange(n)
    idx_tr, idx_te = train_test_split(idx, test_size=0.2, random_state=42)
    y_tr, y_te     = y[idx_tr], y[idx_te]
    log(f'  Train={len(y_tr)} | Test={len(y_te)}')

    results = {}
    models  = {}

    # ── 1. Tabulaire seul ────────────────────────────────────────
    log('\n  [1/4] Tabulaire seul...')
    m1 = XGBRegressor(**XGB_PARAMS)
    m1.fit(X_tab[idx_tr], y_tr,
           eval_set=[(X_tab[idx_te], y_te)], verbose=False)
    p1 = m1.predict(X_tab[idx_te])
    results['tabulaire'] = compute_metrics(
        np.expm1(y_te), np.expm1(p1), 'Tabulaire seul'
    )
    models['tabulaire'] = m1

    # ── 2. Image CNN seule ───────────────────────────────────────
    log('\n  [2/4] Image CNN seule...')
    valid_tr = X_img[idx_tr].any(axis=1)
    valid_te = X_img[idx_te].any(axis=1)
    if valid_tr.sum() > 50:
        m2 = XGBRegressor(**XGB_PARAMS)
        m2.fit(X_img[idx_tr][valid_tr], y_tr[valid_tr],
               eval_set=[(X_img[idx_te][valid_te], y_te[valid_te])],
               verbose=False)
        p2 = m2.predict(X_img[idx_te][valid_te])
        results['image_seule'] = compute_metrics(
            np.expm1(y_te[valid_te]), np.expm1(p2), 'Image CNN seule'
        )
        models['image_seule'] = m2
    else:
        log('  Pas assez d images valides')
        results['image_seule'] = {'r2': 0, 'mae': 999999, 'rmse': 999999}

    # ── 3. Fusion tabulaire + CNN ────────────────────────────────
    log('\n  [3/4] Fusion tabulaire + CNN...')
    m3 = XGBRegressor(**XGB_PARAMS)
    m3.fit(X_full[idx_tr], y_tr,
           eval_set=[(X_full[idx_te], y_te)], verbose=False)
    p3 = m3.predict(X_full[idx_te])
    results['fusion'] = compute_metrics(
        np.expm1(y_te), np.expm1(p3), 'Fusion tabulaire+CNN'
    )
    models['fusion'] = m3

    # ── 4. Régression Bayésienne — incertitude sur les residus ──
    log('\n  [4/4] Régression Bayésienne (incertitude)...')
    # BayesianRidge est entraine sur les residus XGBoost
    # pour estimer l incertitude de prediction
    use_fusion = results['fusion']['r2'] > results['tabulaire']['r2']
    X_bay_tr = X_full[idx_tr] if use_fusion else X_tab[idx_tr]
    X_bay_te = X_full[idx_te] if use_fusion else X_tab[idx_te]

    # Predictions XGBoost (meilleur modele)
    best_xgb = models['fusion'] if use_fusion else models['tabulaire']
    p_train  = best_xgb.predict(X_bay_tr)
    p_test   = best_xgb.predict(X_bay_te)

    # Residus = difference entre valeur reelle et prediction
    residus_train = y_tr - p_train

    # BayesianRidge apprend a predire les residus
    sc  = StandardScaler()
    bay = BayesianRidge(max_iter=500)
    bay.fit(sc.fit_transform(X_bay_tr), residus_train)

    # Predire les residus sur le test + incertitude
    residus_pred, residus_std = bay.predict(
        sc.transform(X_bay_te), return_std=True
    )

    # Prix final = prediction XGBoost + correction residus
    y_final = p_test + residus_pred
    r2_bay  = r2_score(np.expm1(y_te), np.expm1(np.clip(y_final, 0, None)))
    mae_bay = mean_absolute_error(np.expm1(y_te), np.expm1(np.clip(y_final, 0, None)))
    log(f'  Bayesien (residus)             | R2={r2_bay:>7.4f} | MAE={mae_bay:>12,.0f} TND')

    # Fourchette de confiance 90% depuis l incertitude bayesienne
    std_mean       = float(np.mean(residus_std))
    prix_median    = float(np.median(np.expm1(y_te)))
    # Convertir std log → TND
    fourchette_tnd = float(prix_median * std_mean * 1.645)
    fourchette_pct = round(std_mean * 1.645 * 100, 1)
    log(f'  Fourchette 90% : +/-{fourchette_tnd:,.0f} TND ({fourchette_pct:.1f}% du prix median)')

    # Fallback : si bayesien pire que XGBoost seul → utiliser MAE comme fourchette
    best_mae = results['fusion']['mae'] if use_fusion else results['tabulaire']['mae']
    if r2_bay < 0:
        fourchette_tnd = best_mae * 1.0  # fourchette = 1x MAE
        fourchette_pct = round(fourchette_tnd / prix_median * 100, 1)
        log(f'  Fallback MAE : +/-{fourchette_tnd:,.0f} TND ({fourchette_pct:.1f}%)')

    results['bayesien'] = {
        'r2':            round(r2_bay, 4),
        'mae':           round(mae_bay, 2),
        'std_moyenne':   round(std_mean, 6),
        'fourchette_tnd':round(fourchette_tnd, 0),
        'fourchette_pct':round(fourchette_pct, 1),
    }
    models['bayesien'] = (bay, sc, best_xgb, use_fusion)

    # ── Gain images ──────────────────────────────────────────────
    r2_gain  = results['fusion']['r2'] - results['tabulaire']['r2']
    mae_gain = results['tabulaire']['mae'] - results['fusion']['mae']
    log(f'\n  Gain R2 images : {r2_gain:+.4f} | Gain MAE : {mae_gain:+,.0f} TND')
    results['image_contribution'] = {
        'r2_gain':       round(r2_gain, 4),
        'mae_gain':      round(mae_gain, 2),
        'images_aident': bool(r2_gain > 0.005),
    }

    # ── Importance features ──────────────────────────────────────
    imp     = m3.feature_importances_
    n_tab   = X_tab.shape[1]
    tab_pct = float(imp[:n_tab].sum()) * 100
    img_pct = float(imp[n_tab:].sum()) * 100
    log(f'  Importance : tabulaire={tab_pct:.1f}% | image={img_pct:.1f}%')
    results['feature_importance'] = {
        'tabulaire_pct': round(tab_pct, 1),
        'image_pct':     round(img_pct, 1),
    }
    top5 = sorted(zip(tab_cols, imp[:n_tab]),
                  key=lambda x: x[1], reverse=True)[:5]
    log('  Top 5 : ' + ' | '.join(f'{f}={s:.3f}' for f, s in top5))

    # ── Meilleur modele XGBoost ──────────────────────────────────
    best_name = max(['tabulaire', 'fusion'],
                    key=lambda k: results[k]['r2'])
    r2_best = results[best_name]['r2']
    log(f'\n  Meilleur XGBoost : {best_name} (R2={r2_best:.4f})')

    best_X = X_tab if best_name == 'tabulaire' else X_full
    return results, (best_name, models[best_name], best_X, tab_cols), models


# ================================================================
# RAPPORT
# ================================================================

def print_report(all_results):
    section('RAPPORT FINAL')

    print(f'\n  {"Segment":<20} {"Tab R2":>8} {"Img R2":>8} '
          f'{"Fus R2":>8} {"Bay R2":>8} {"Gain":>7}  Modele')
    print('  ' + '-'*75)
    for seg, res in all_results.items():
        if res is None: continue
        r2t = res.get('tabulaire',   {}).get('r2', 0)
        r2i = res.get('image_seule', {}).get('r2', 0)
        r2f = res.get('fusion',      {}).get('r2', 0)
        r2b = res.get('bayesien',    {}).get('r2', 0)
        g   = res.get('image_contribution', {}).get('r2_gain', 0)
        best = 'fusion' if res.get('image_contribution', {}).get('images_aident') else 'tabulaire'
        print(f'  {seg:<20} {r2t:>8.4f} {r2i:>8.4f} '
              f'{r2f:>8.4f} {r2b:>8.4f} {g:>+7.4f}  {best}')

    print()
    print('  FOURCHETTE DE CONFIANCE (Regression Bayesienne sur residus) :')
    print(f'  {"Segment":<20} {"R2 Bay":>8} {"Fourchette +/-":>16} {"% prix":>8}')
    print('  ' + '-'*57)
    for seg, res in all_results.items():
        if res is None: continue
        b   = res.get('bayesien', {})
        r2b = b.get('r2', 0)
        fch = b.get('fourchette_tnd', 0)
        pct = b.get('fourchette_pct', 0)
        print(f'  {seg:<20} {r2b:>8.4f} {fch:>16,.0f} TND {pct:>7.1f}%')

    print()
    print(f'  {"Segment":<20} {"Modele":<15} {"MAE":>12} {"R2":>8}')
    print('  ' + '-'*58)
    for seg, res in all_results.items():
        if res is None: continue
        best  = 'fusion' if res.get('image_contribution', {}).get('images_aident') else 'tabulaire'
        m     = res.get(best, {})
        print(f'  {seg:<20} {best:<15} {m.get("mae",0):>12,.0f} {m.get("r2",0):>8.4f}')


# ================================================================
# MAIN
# ================================================================

def run():
    section('TRAIN_NEW.PY — XGBoost + Regression Bayesienne')
    log('EstateMind BO2 — Tunisian Real Estate Valuation')

    all_results = {}

    for seg_name, csv_file in SEGMENTS.items():
        section(f'SEGMENT : {seg_name.upper()}')

        if not os.path.exists(csv_file):
            log(f'introuvable : {csv_file}')
            continue

        X_tab, X_img, X_full, y, tab_cols = load_segment(seg_name, csv_file)
        results, best, seg_models = train_segment(
            seg_name, X_tab, X_img, X_full, y, tab_cols
        )

        if results is None:
            continue

        all_results[seg_name] = results

        # Sauvegarder bundle complet
        best_name, best_model, best_X, tab_cols = best
        bay_tuple  = seg_models.get('bayesien', (None, None, None, False))
        bay_model, bay_scaler, bay_xgb, bay_use_fusion = bay_tuple
        bundle = {
            'model':          best_model,
            'model_type':     best_name,
            'tab_cols':       tab_cols,
            'use_images':     best_name == 'fusion',
            'seg_name':       seg_name,
            'bay_model':      bay_model,
            'bay_scaler':     bay_scaler,
            'bay_xgb':        bay_xgb,
            'bay_use_fusion': bay_use_fusion,
            'bay_std':        results.get('bayesien', {}).get('std_moyenne', 0.1),
            'fourchette_tnd': results.get('bayesien', {}).get('fourchette_tnd', 0),
            'fourchette_pct': results.get('bayesien', {}).get('fourchette_pct', 15.0),
        }
        path = os.path.join(MODELS_DIR, f'xgb_{seg_name}.pkl')
        with open(path, 'wb') as f:
            pickle.dump(bundle, f)
        log(f'  Sauvegarde : {path}')

    print_report(all_results)

    path = os.path.join(MODELS_DIR, 'results_new.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    log(f'\n  JSON : {path}')
    log('  Termine !')


if __name__ == '__main__':
    run()