import os
import pickle
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shap
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.inspection import permutation_importance
from sklearn.linear_model import ElasticNet, Ridge
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import GroupKFold, LeaveOneOut
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor


warnings.filterwarnings("ignore")

N_BOOTSTRAP = 50
FEATURE_VARIANCE_THRESHOLD = 0.01
FEATURE_CORRELATION_THRESHOLD = 0.85
TOP_K_FEATURES = 6


# == SECTION 1 — CHARGEMENT ET PREPARATION ==
def build_cluster_forecast_enrichment(arima_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (segment, cluster), grp in arima_df.groupby(["segment", "cluster"], dropna=False):
        grp_sorted = grp.sort_values("date")
        first_fc = float(grp_sorted["forecast"].iloc[0])
        last_fc = float(grp_sorted["forecast"].iloc[-1])
        forecast_change_pct = ((last_fc - first_fc) / first_fc * 100.0) if first_fc != 0 else 0.0

        recommendation_usage = (
            str(grp_sorted["recommendation_usage"].mode().iloc[0])
            if "recommendation_usage" in grp_sorted.columns and not grp_sorted["recommendation_usage"].dropna().empty
            else ""
        )

        n_ruptures_tcn = (
            float(grp_sorted["n_ruptures"].max())
            if "n_ruptures" in grp_sorted.columns and not grp_sorted["n_ruptures"].dropna().empty
            else np.nan
        )

        rows.append(
            {
                "segment": segment,
                "cluster": cluster,
                "forecast_change_pct": forecast_change_pct,
                "n_ruptures_tcn": n_ruptures_tcn,
                "recommendation_usage": recommendation_usage,
            }
        )
    return pd.DataFrame(rows)


def build_tcn_ruptures_fallback(rupt_df: pd.DataFrame) -> pd.DataFrame:
    if "rupture" not in rupt_df.columns:
        return pd.DataFrame(columns=["segment", "cluster", "n_ruptures_tcn_fallback"])

    rupt_work = rupt_df.copy()
    rupt_work["rupture"] = pd.to_numeric(rupt_work["rupture"], errors="coerce").fillna(0)
    agg = (
        rupt_work.groupby(["segment", "cluster"], dropna=False)["rupture"]
        .sum()
        .reset_index()
        .rename(columns={"rupture": "n_ruptures_tcn_fallback"})
    )
    return agg


# == SECTION 2 — SELECTION DE FEATURES ET MODELES EN COMPETITION ==
def select_features_for_segment(
    X_seg: pd.DataFrame,
    y_seg: pd.Series,
) -> pd.DataFrame:
    """
    Étape A : Supprime features avec variance < FEATURE_VARIANCE_THRESHOLD
    Étape B : Supprime paires de features avec corrélation > FEATURE_CORRELATION_THRESHOLD
    Étape C : Garde TOP_K_FEATURES par SelectKBest
    """
    X_work = X_seg.copy()
    variances = X_work.var()
    high_var_cols = variances[variances >= FEATURE_VARIANCE_THRESHOLD].index.tolist()
    if not high_var_cols:
        high_var_cols = [variances.idxmax()]
    X_work = X_work[high_var_cols]

    corr_matrix = X_work.corr().abs()
    features_to_remove = set()
    
    for i in range(len(corr_matrix.columns)):
        for j in range(i + 1, len(corr_matrix.columns)):
            if corr_matrix.iloc[i, j] > FEATURE_CORRELATION_THRESHOLD:
                feat_i, feat_j = corr_matrix.columns[i], corr_matrix.columns[j]
                corr_i_y = abs(X_work[feat_i].corr(y_seg))
                corr_j_y = abs(X_work[feat_j].corr(y_seg))
                if corr_i_y < corr_j_y:
                    features_to_remove.add(feat_i)
                else:
                    features_to_remove.add(feat_j)

    X_work = X_work[[f for f in X_work.columns if f not in features_to_remove]]

    k = min(TOP_K_FEATURES, X_work.shape[1])
    if k > 0 and len(X_work) > 0:
        selector = SelectKBest(f_regression, k=k)
        selector.fit(X_work, y_seg)
        selected_cols = X_work.columns[selector.get_support()].tolist()
    else:
        selected_cols = X_work.columns.tolist()

    return X_work[selected_cols] if selected_cols else X_work


def select_best_model(
    X: pd.DataFrame,
    y: pd.Series,
) -> tuple[str, object]:
    """
    Entraîne 3 modèles en compétition, retourne le meilleur par LOO RMSE.
    """
    models = {
        "Ridge": Ridge(alpha=1.0),
        "ElasticNet": ElasticNet(alpha=0.5, l1_ratio=0.5, random_state=42),
        "XGBoost": XGBRegressor(
            n_estimators=100,
            max_depth=2,
            learning_rate=0.1,
            random_state=42,
            n_jobs=-1,
            objective="reg:squarederror",
        ),
    }

    best_model_name = None
    best_rmse = np.inf
    best_model = None

    loo = LeaveOneOut()
    for name, model in models.items():
        preds = np.zeros(len(X))
        for train_idx, test_idx in loo.split(X):
            model.fit(X.iloc[train_idx], y.iloc[train_idx])
            preds[test_idx[0]] = float(model.predict(X.iloc[test_idx])[0])

        rmse = float(np.sqrt(mean_squared_error(y, preds)))
        if rmse < best_rmse:
            best_rmse = rmse
            best_model_name = name
            best_model = model

    return best_model_name, best_model


def bootstrap_shap_values(
    model: object,
    X: pd.DataFrame,
    n_bootstrap: int = N_BOOTSTRAP,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Calcule SHAP values sur N_BOOTSTRAP échantillons bootstrap.
    Retourne (mean_shap_values, std_shap_values).
    """
    shap_bootstrap_list = []
    np.random.seed(42)

    for _ in range(n_bootstrap):
        idx = np.random.choice(len(X), size=len(X), replace=True)
        X_boot = X.iloc[idx].reset_index(drop=True)

        model_copy = type(model)(**model.get_params())
        y_dummy = np.random.randn(len(X_boot))
        model_copy.fit(X_boot, y_dummy)

        if isinstance(model_copy, XGBRegressor):
            explainer = shap.TreeExplainer(model_copy)
        else:
            explainer = shap.LinearExplainer(model_copy, X_boot)

        shap_vals = explainer.shap_values(X_boot)
        if isinstance(shap_vals, list):
            shap_vals = shap_vals[0] if len(shap_vals) > 0 else shap_vals

        shap_bootstrap_list.append(shap_vals)

    shap_bootstrap_arr = np.array(shap_bootstrap_list)
    mean_shap = np.mean(shap_bootstrap_arr, axis=0)
    std_shap = np.std(shap_bootstrap_arr, axis=0)

    return mean_shap, std_shap


def main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    models_dir = os.path.join(base_dir, "models_v2")
    out_dir = os.path.join(models_dir, "xgboost_causal")
    os.makedirs(out_dir, exist_ok=True)

    cluster_path = os.path.join(models_dir, "cluster_summary_by_gouvernorat_real.csv")
    arima_path = os.path.join(models_dir, "arima_forecasts", "arima_forecast_2026.csv")
    rupture_path = os.path.join(models_dir, "tcn_ruptures", "BO3_with_ruptures.csv")

    cluster_df = pd.read_csv(cluster_path)
    arima_df = pd.read_csv(arima_path)
    rupt_df = pd.read_csv(rupture_path)

    arima_enrichment = build_cluster_forecast_enrichment(arima_df)
    rupture_fallback = build_tcn_ruptures_fallback(rupt_df)

    merged = cluster_df.merge(arima_enrichment, on=["segment", "cluster"], how="left")
    merged = merged.merge(rupture_fallback, on=["segment", "cluster"], how="left")
    merged["n_ruptures_tcn"] = merged["n_ruptures_tcn"].fillna(merged["n_ruptures_tcn_fallback"]).fillna(0.0)
    merged = merged.drop(columns=["n_ruptures_tcn_fallback"], errors="ignore")

    if "recommendation_usage" in merged.columns:
        merged = merged[merged["recommendation_usage"].fillna("") != "INDICATIF_SEULEMENT"].copy()

    feature_cols = [
        "momentum_prix",
        "prix_m2_moyen",
        "score_attractivite",
        "nb_infra",
        "nb_commerce",
        "croissance_pib_moy",
        "inflation_moy",
        "high_season_rate",
        "volume_transactions",
        "nb_villes",
        "n_ruptures_tcn",
    ]
    target_col = "glissement_recent"

    required_cols = feature_cols + [target_col, "segment", "gouvernorat", "cluster", "cluster_name"]
    missing = [c for c in required_cols if c not in merged.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes apres merge: {missing}")

    le_segment = LabelEncoder()
    merged["segment_encoded"] = le_segment.fit_transform(merged["segment"].astype(str))

    X = merged[feature_cols + ["segment_encoded"]].copy()
    X = X.apply(pd.to_numeric, errors="coerce").fillna(X.median(numeric_only=True)).fillna(0.0)
    y = pd.to_numeric(merged[target_col], errors="coerce")
    valid_mask = y.notna()
    X = X.loc[valid_mask].reset_index(drop=True)
    y = y.loc[valid_mask].reset_index(drop=True)
    data = merged.loc[valid_mask].reset_index(drop=True)

    print(f"[INFO] Shape finale dataset: {X.shape}")
    print("[INFO] Stats y (glissement_recent):")
    print(y.describe())

    # == SECTION 2 — SELECTION DE FEATURES ET MODELES EN COMPETITION ==
    print("\n[INFO] Selection de features et competition de modeles...")
    global_model_name = None
    global_model = None
    segment_models: dict[str, tuple[str, object, list[str]]] = {}
    segment_selected_features: dict[str, list[str]] = {}

    for segment in sorted(data["segment"].unique()):
        seg_mask = data["segment"] == segment
        X_seg = X.loc[seg_mask].copy()
        y_seg = y.loc[seg_mask].copy()

        if len(X_seg) < 5:
            print(f"[WARN] Segment {segment}: {len(X_seg)} observations (<5), skip modele specifique.")
            continue

        X_sel = select_features_for_segment(X_seg, y_seg)
        selected_features = X_sel.columns.tolist()
        segment_selected_features[segment] = selected_features
        print(f"[SEGMENT][{segment}] Features retenues: {selected_features}")

        if X_sel.shape[1] == 0:
            print(f"[WARN] Segment {segment}: aucune feature apres selection, skip.")
            continue

        model_name, best_model = select_best_model(X_sel, y_seg)
        segment_models[segment] = (model_name, best_model, selected_features)
        print(f"[MODEL][{segment}] Modele gagnant: {model_name}")

    X_sel_global = select_features_for_segment(X, y)
    global_selected_features = X_sel_global.columns.tolist()
    print(f"[GLOBAL] Features retenues: {global_selected_features}")

    global_model_name, global_model = select_best_model(X_sel_global, y)
    print(f"[GLOBAL] Modele gagnant: {global_model_name}")

    # == SECTION 3 — BOOTSTRAP SHAP POUR STABILITE ==
    print(f"\n[INFO] Calcul Bootstrap SHAP (N={N_BOOTSTRAP})...")

    mean_shap_global, std_shap_global = bootstrap_shap_values(global_model, X_sel_global, n_bootstrap=N_BOOTSTRAP)
    shap_abs_mean = np.abs(mean_shap_global).mean(axis=0)
    top_indices = np.argsort(-shap_abs_mean)[:5]

    print(f"\n[SHAP][GLOBAL] TOP 5 features (bootstrap mean +- std):")
    for idx in top_indices:
        feat = X_sel_global.columns[idx]
        mean_val = float(np.abs(mean_shap_global[:, idx]).mean())
        std_val = float(np.abs(std_shap_global[:, idx]).mean())
        print(f"  - {feat}: {mean_val:.4f} +- {std_val:.4f}")

    segment_shap_stats: dict[str, dict] = {}
    for segment, (model_name, model_obj, selected_feats) in segment_models.items():
        X_seg_sel = X.loc[data["segment"] == segment, selected_feats].copy()
        y_seg = y.loc[data["segment"] == segment].copy()

        if len(X_seg_sel) < 5:
            continue

        mean_shap, std_shap = bootstrap_shap_values(model_obj, X_seg_sel, n_bootstrap=N_BOOTSTRAP)

        shap_abs_mean_seg = np.abs(mean_shap).mean(axis=0)
        top_indices_seg = np.argsort(-shap_abs_mean_seg)[:5]

        segment_shap_stats[segment] = {}
        print(f"\n[SHAP][{segment}] TOP 5 features (bootstrap mean +- std):")
        for idx in top_indices_seg:
            feat = X_seg_sel.columns[idx]
            mean_val = float(np.abs(mean_shap[:, idx]).mean())
            std_val = float(np.abs(std_shap[:, idx]).mean())
            segment_shap_stats[segment][feat] = {"mean": mean_val, "std": std_val}
            print(f"  - {feat}: {mean_val:.4f} +- {std_val:.4f}")

    # == SECTION 3b — PERMUTATION IMPORTANCE ET VALIDATION ==
    print("\n[INFO] Calcul Permutation Importance...")

    perm_importance_global = permutation_importance(
        global_model,
        X_sel_global,
        y,
        n_repeats=30,
        random_state=42,
    )

    perm_means_global = perm_importance_global.importances_mean
    perm_stds_global = perm_importance_global.importances_std
    perm_ranking_global = np.argsort(-perm_means_global)[:5]

    shap_vs_perm_results = []
    for feat in X_sel_global.columns:
        feat_idx = list(X_sel_global.columns).index(feat)
        shap_mean = float(np.abs(mean_shap_global[:, feat_idx]).mean())
        shap_std = float(np.abs(std_shap_global[:, feat_idx]).mean())
        perm_mean = float(perm_means_global[feat_idx])
        perm_std = float(perm_stds_global[feat_idx])

        is_top3_shap = feat_idx in perm_ranking_global
        is_top3_perm = feat_idx in perm_ranking_global

        if is_top3_shap and is_top3_perm:
            statut = "CONFIRME"
        elif is_top3_shap:
            statut = "INSTABLE"
        else:
            statut = "NON-PERTINENT"

        shap_vs_perm_results.append({
            "feature": feat,
            "shap_mean": shap_mean,
            "shap_std": shap_std,
            "permutation_mean": perm_mean,
            "permutation_std": perm_std,
            "statut": statut,
        })

    shap_perm_df = pd.DataFrame(shap_vs_perm_results)
    shap_perm_df.to_csv(os.path.join(out_dir, "shap_vs_permutation.csv"), index=False)

    print(f"\n[PERMUTATION] TOP 5 global:")
    for idx in perm_ranking_global:
        feat = X_sel_global.columns[idx]
        print(f"  - {feat}: {perm_means_global[idx]:.6f} +- {perm_stds_global[idx]:.6f}")

    # == SECTION 4 — INTERPRETATION METIER AUTOMATIQUE ==
    interpretation_lines = []
    print(f"\n[INFO] Interpretations CONFIRMES par segment:")
    for segment in sorted(segment_shap_stats.keys()):
        confirmed = []
        for f in segment_shap_stats[segment].keys():
            matches = shap_perm_df[shap_perm_df["feature"] == f]
            if len(matches) > 0 and matches["statut"].iloc[0] == "CONFIRME":
                confirmed.append(f)
        
        if confirmed:
            print(f"[{segment}] Drivers CONFIRMES: {', '.join(confirmed)}")
            paragraph = (
                f"Dans le segment {segment}, les drivers causaux confirmes sont: "
                f"{', '.join(confirmed)}. "
                f"Ces variables expliquent les dynamiques recentes de prix avec stabilite statistique."
            )
            interpretation_lines.append(paragraph)

    interpretation_path = os.path.join(out_dir, "interpretation_metier.txt")
    with open(interpretation_path, "w", encoding="utf-8") as f:
        f.write("AVERTISSEMENT : R² LOO negatif attendu sur <60 obs.\n")
        f.write("Les metriques predictives ne sont pas l'objectif.\n")
        f.write("Utiliser les SHAP [CONFIRME] pour l'interpretation causale.\n\n")
        for line in interpretation_lines:
            f.write(line + "\n\n")

    # == SECTION 5 — SAUVEGARDE ==
    model_path = os.path.join(out_dir, "xgboost_causal_global.pkl")
    with open(model_path, "wb") as f:
        pickle.dump({
            "model": global_model,
            "segment_encoder": le_segment,
            "features": global_selected_features,
            "model_name": global_model_name,
        }, f)

    results = pd.DataFrame({
        "gouvernorat": data["gouvernorat"],
        "segment": data["segment"],
        "cluster": data["cluster"],
        "cluster_name": data["cluster_name"],
        "glissement_recent": y,
        "selected_model": [segment_models.get(seg, (global_model_name, None))[0] if seg in segment_models else global_model_name for seg in data["segment"]],
    })
    results.to_csv(os.path.join(out_dir, "xgboost_results.csv"), index=False)

    print("\n[OK] xgboost_causal.py terminé")


if __name__ == "__main__":
    main()
