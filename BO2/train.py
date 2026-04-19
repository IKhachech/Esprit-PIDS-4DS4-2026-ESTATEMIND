"""
train.py v3 — Entraînement XGBoost multimodal
BO2 — EstateMind : Tunisian Real Estate

Corrections v3 :
    - Suppression outliers prix (percentile 1-99) avant entraînement
    - Fusion segments trop petits (<200 lignes) avec la categorie parente
    - Ridge stabilise par clip des predictions
    - Un modele par segment : xgb_residentiel_vente.pkl, etc.
"""

import os, json, pickle, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

# ================================================================
# CONFIG
# ================================================================

DATASETS = {
    'residentiel': 'residentiel_BO2.xlsx',
    'foncier':     'foncier_BO2.xlsx',
    'commercial':  'commercial_BO2.xlsx',
    'divers':      'divers_BO2.xlsx',
}

TABULAR_FEATURES = [
    'gouvernorat', 'ville_encoded', 'lat', 'lon', 'type_bien',
    'surface_m2', 'score_attractivite', 'market_tension',
    'cycle_marche', 'prix_region_median', 'text_embedding_score',
]

TARGET              = 'prix_transaction_estimated'
IMAGE_FEATURES_PATH = 'image_features_resnet.npy'
MODELS_DIR          = 'models'
TRANSACTION_LABELS  = {1: 'location', 2: 'vente'}
MIN_SEGMENT_SIZE    = 200   # segments < 200 lignes → fusionnes avec la categorie

DATASET_OFFSETS = {
    'residentiel': 0,
    'foncier':     23104,
    'commercial':  27282,
    'divers':      28106,
}

os.makedirs(MODELS_DIR, exist_ok=True)

def section(t): print("\n" + "="*65 + f"\n   {t}\n" + "="*65)
def log(m):     print(f"  {m}")

def compute_metrics(y_true, y_pred, label=""):
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    if label:
        log(f"  {label:<30} | MAE={mae:>12,.0f} TND | R²={r2:.4f}")
    return {'mae': round(mae,2), 'rmse': round(rmse,2), 'r2': round(r2,4)}


# ================================================================
# NETTOYAGE OUTLIERS
# ================================================================

def remove_outliers(df, col=TARGET, low=0.01, high=0.99):
    """Supprime les outliers prix hors percentile [low, high]."""
    q_low  = df[col].quantile(low)
    q_high = df[col].quantile(high)
    mask   = (df[col] >= q_low) & (df[col] <= q_high)
    n_removed = (~mask).sum()
    if n_removed > 0:
        log(f"  Outliers supprimés : {n_removed} lignes "
            f"(prix < {q_low:,.0f} ou > {q_high:,.0f} TND)")
    return df[mask].copy()


# ================================================================
# PREPARATION
# ================================================================

def prepare_segment(df_seg, name, img_features, offset):
    df_seg = df_seg.dropna(subset=[TARGET]).copy()

    # Supprimer outliers prix
    df_seg = remove_outliers(df_seg)
    df_seg = df_seg.reset_index(drop=True)

    tab_cols = [c for c in TABULAR_FEATURES if c in df_seg.columns]
    if name == 'residentiel' and 'nb_pieces' in df_seg.columns:
        tab_cols = tab_cols + ['nb_pieces']

    X_tab = df_seg[tab_cols].copy()
    for col in X_tab.columns:
        if X_tab[col].isna().any():
            X_tab[col] = X_tab[col].fillna(X_tab[col].median())

    y = np.log1p(df_seg[TARGET].values)

    # Features image
    original_indices = df_seg['_original_idx'].values
    img_rows = []
    for orig_idx in original_indices:
        global_idx = offset + int(orig_idx)
        if global_idx < len(img_features):
            img_rows.append(img_features[global_idx])
        else:
            img_rows.append(np.zeros(img_features.shape[1]))
    X_img  = np.array(img_rows)
    X_full = np.concatenate([X_tab.values, X_img], axis=1)

    n_nonzero = (X_img.any(axis=1)).sum()
    log(f"  Lignes={len(df_seg)} | Images={n_nonzero} | "
        f"X_tab={X_tab.shape} | X_full={X_full.shape}")

    return {
        'X_tab':    X_tab.values,
        'X_full':   X_full,
        'y':        y,
        'tab_cols': tab_cols,
        'n':        len(df_seg),
    }


# ================================================================
# ENTRAINEMENT
# ================================================================

def train_segment(data, segment_name):
    X_tab, X_full, y = data['X_tab'], data['X_full'], data['y']
    if data['n'] < 50:
        log(f"  Trop peu de données ({data['n']}) — ignoré")
        return None, None

    X_tab_tr,  X_tab_te,  y_tr, y_te = train_test_split(
        X_tab,  y, test_size=0.2, random_state=42)
    X_full_tr, X_full_te, _,    _    = train_test_split(
        X_full, y, test_size=0.2, random_state=42)

    log(f"  Train={len(y_tr)} | Test={len(y_te)}")
    results = {}

    # 1. Baseline Ridge — avec clipping pour eviter les explosions numeriques
    log("\n  [1/3] Baseline Ridge...")
    sc    = StandardScaler()
    ridge = Ridge(alpha=10.0)
    ridge.fit(sc.fit_transform(X_tab_tr), y_tr)
    pred_r = ridge.predict(sc.transform(X_tab_te))
    # Clip les predictions dans la plage observee
    pred_r = np.clip(pred_r, y.min(), y.max())
    results['baseline_ridge'] = compute_metrics(
        np.expm1(y_te), np.expm1(pred_r), "Baseline Ridge"
    )

    # 2. XGBoost sans image
    log("\n  [2/3] XGBoost sans image...")
    xgb_tab = XGBRegressor(
        n_estimators=500, learning_rate=0.05, max_depth=6,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, n_jobs=-1, verbosity=0,
    )
    xgb_tab.fit(X_tab_tr, y_tr,
                eval_set=[(X_tab_te, y_te)], verbose=False)
    pred_tab = xgb_tab.predict(X_tab_te)
    results['xgb_sans_image'] = compute_metrics(
        np.expm1(y_te), np.expm1(pred_tab), "XGBoost sans image"
    )

    # 3. XGBoost avec image
    log("\n  [3/3] XGBoost avec image...")
    xgb_full = XGBRegressor(
        n_estimators=500, learning_rate=0.05, max_depth=6,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, n_jobs=-1, verbosity=0,
    )
    xgb_full.fit(X_full_tr, y_tr,
                 eval_set=[(X_full_te, y_te)], verbose=False)
    pred_full = xgb_full.predict(X_full_te)
    results['xgb_avec_image'] = compute_metrics(
        np.expm1(y_te), np.expm1(pred_full), "XGBoost avec image"
    )

    # Gain
    r2_gain  = results['xgb_avec_image']['r2'] - results['xgb_sans_image']['r2']
    mae_gain = results['xgb_sans_image']['mae'] - results['xgb_avec_image']['mae']
    log(f"\n  Gain R² image : {r2_gain:+.4f} | Gain MAE : {mae_gain:+,.0f} TND")
    results['image_contribution'] = {
        'r2_gain':  round(r2_gain, 4),
        'mae_gain': round(mae_gain, 2),
    }

    # Importance features
    imp     = xgb_full.feature_importances_
    n_tab   = X_tab.shape[1]
    tab_pct = float(imp[:n_tab].sum()) * 100
    img_pct = float(imp[n_tab:].sum()) * 100
    log(f"  Importance : tabulaire={tab_pct:.1f}% | image={img_pct:.1f}%")
    results['feature_importance_split'] = {
        'tabulaire_pct': round(tab_pct, 1),
        'image_pct':     round(img_pct, 1),
    }

    top5 = sorted(zip(data['tab_cols'], imp[:n_tab]),
                  key=lambda x: x[1], reverse=True)[:5]
    log("  Top 5 : " + " | ".join(f"{f}={s:.3f}" for f, s in top5))

    return results, xgb_full


# ================================================================
# RAPPORT FINAL
# ================================================================

def print_final_report(all_results):
    section("RAPPORT FINAL")

    print(f"\n  {'Segment':<28} {'Modele':<22} {'MAE (TND)':>12} {'R2':>8}")
    print("  " + "-"*73)
    for seg, res in sorted(all_results.items()):
        if res is None: continue
        for mname in ['baseline_ridge', 'xgb_sans_image', 'xgb_avec_image']:
            if mname not in res: continue
            m = res[mname]
            print(f"  {seg:<28} {mname:<22} {m['mae']:>12,.0f} {m['r2']:>8.4f}")
        print()

    print("\n  CONTRIBUTION FEATURES IMAGE :")
    print(f"  {'Segment':<28} {'Gain R2':>10} {'Gain MAE':>14} "
          f"{'Tab%':>6} {'Img%':>6}  OK?")
    print("  " + "-"*70)
    for seg, res in sorted(all_results.items()):
        if res is None: continue
        c   = res.get('image_contribution', {})
        s   = res.get('feature_importance_split', {})
        r2g = c.get('r2_gain', 0)
        mag = c.get('mae_gain', 0)
        tp  = s.get('tabulaire_pct', 0)
        ip  = s.get('image_pct', 0)
        ok  = "✔ OK" if r2g > 0 else "✗"
        print(f"  {seg:<28} {r2g:>+10.4f} {mag:>+14,.0f} "
              f"{tp:>5.1f}% {ip:>5.1f}%  {ok}")


# ================================================================
# MAIN
# ================================================================

def run_training():
    section("TRAIN.PY v3 — SEPARATION VENTE/LOCATION + NETTOYAGE OUTLIERS")
    log("EstateMind BO2 — Tunisian Real Estate Valuation")

    if not os.path.exists(IMAGE_FEATURES_PATH):
        raise FileNotFoundError(
            f"{IMAGE_FEATURES_PATH} introuvable. Lance d'abord : python vision.py"
        )

    img_features = np.load(IMAGE_FEATURES_PATH)
    log(f"Features image : {img_features.shape}")

    all_results = {}

    for name, filename in DATASETS.items():
        if not os.path.exists(filename):
            continue

        df = pd.read_excel(filename)
        df['_original_idx'] = df.index
        offset = DATASET_OFFSETS[name]

        for tt_code, tt_label in TRANSACTION_LABELS.items():
            df_seg = df[df['type_transaction'] == tt_code].copy()

            # Fusionner les petits segments avec la categorie complete
            if len(df_seg) < MIN_SEGMENT_SIZE:
                log(f"\n  ⚠ {name}_{tt_label} : {len(df_seg)} lignes "
                    f"< {MIN_SEGMENT_SIZE} → fusionne avec categorie complete")
                df_seg = df.copy()
                seg_name = f"{name}_all"
            else:
                seg_name = f"{name}_{tt_label}"

            # Eviter les doublons si les deux transactions sont petites
            if seg_name in all_results:
                continue

            section(f"SEGMENT : {seg_name.upper()} ({len(df_seg)} lignes)")

            data           = prepare_segment(df_seg, name, img_features, offset)
            results, model = train_segment(data, seg_name)

            if model is not None:
                bundle = {
                    'model':          model,
                    'tab_cols':       data['tab_cols'],
                    'n_img_features': data['X_full'].shape[1] - data['X_tab'].shape[1],
                }
                path = os.path.join(MODELS_DIR, f'xgb_{seg_name}.pkl')
                with open(path, 'wb') as f:
                    pickle.dump(bundle, f)
                log(f"  Modele sauvegarde : {path}")
                all_results[seg_name] = results

    print_final_report(all_results)

    path = os.path.join(MODELS_DIR, 'results_comparison.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    log(f"\n  Resultats sauvegardes : {path}")
    log("  Entrainement termine !")


if __name__ == '__main__':
    run_training()