"""
arima_forecast_v2.py — Séries Temporelles par Cluster (Données Enrichies)
==========================================================================
Pipeline : segmentation -> détection ruptures (TCN) -> prévision par cluster (ETS enrichi)

POURQUOI ETS et non ARIMA :
  Les séries par cluster ont 6 à 16 points trimestriels maximum (2022-2026).
  ARIMA sur <20 points -> instabilité numérique (résultats +1955% ou -66%
  observés dans les runs précédents). ETS/Holt fonctionne dès 6 points.

DONNÉES ENRICHIES UTILISÉES :
  Depuis BO3_with_ruptures.csv (sortie TCN) :
    - rupture_flag       -> choc structurel détecté par TCN
    - soft_rupture       -> choc partiel (confiance 70-99%)
    - rupture_direction  -> hausse/baisse post-rupture

  Depuis cluster_summary_by_gouvernorat_real.csv (sortie segmentation) :
    - momentum_prix      -> ratio prix_recent/prix_moyen -> accélération
    - variation_annuelle -> calculée depuis prix_m2_moyen et prix_m2_recent
    - glissement_recent  -> glissement observé par cluster
    - score_attractivite -> attractivité structurelle

  Ces features enrichissent la prévision ETS via une correction post-modèle
  (on ne les injecte pas comme exogènes ARIMA car trop peu de points).

STRUCTURE DE SORTIE :
  - arima_forecast_2026.csv   -> prévisions mensuelles par cluster (12 mois)
  - arima_summary.csv         -> résumé par cluster avec métriques
  - arima_models.pkl          -> modèles sauvegardés
"""

import os
import pickle
import warnings
import numpy as np
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing, SimpleExpSmoothing

warnings.filterwarnings("ignore")

# --- Chemins (identiques au pipeline existant) --------------------------------
BASE_DIR             = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH           = os.path.join(BASE_DIR, "models_v2", "tcn_ruptures", "BO3_with_ruptures.csv")
CLUSTER_SUMMARY_PATH = os.path.join(BASE_DIR, "models_v2", "cluster_summary_by_gouvernorat_real.csv")
OUTPUT_DIR           = os.path.join(BASE_DIR, "models_v2", "arima_forecasts")

HORIZON_QUARTERS = 4    # 4 trimestres = ~1 an de prévision
HORIZON_MONTHS   = 12   # sortie finale interpolée en mois
TEST_QUARTERS    = 4    # réservés à la validation MAPE (out-of-sample)
MIN_POINTS       = 6    # minimum absolu pour fitter ETS

RECO_CENTRAL_MAX_MAPE = 12.0
RECO_CENTRAL_MAX_ROLLING_MAPE = 15.0
RECO_CENTRAL_MAX_CI_RATIO = 0.45

WEAK_CLUSTER_MAPE_THRESHOLD = 20.0
WEAK_CLUSTER_ROLLING_THRESHOLD = 20.0
FALLBACK_SEGMENT_BLEND_WEIGHT = 0.65

GOV_NAMES = {
    1:"Ariana", 2:"Beja", 3:"Ben Arous", 4:"Bizerte", 5:"Gabes", 6:"Gafsa",
    7:"Jendouba", 8:"Kairouan", 9:"Kasserine", 10:"Kebili", 11:"Le Kef", 12:"Mahdia",
    13:"Manouba", 14:"Medenine", 15:"Monastir", 16:"Nabeul", 17:"Sfax", 18:"Sidi Bouzid",
    19:"Siliana", 20:"Sousse", 21:"Tataouine", 22:"Tozeur", 23:"Tunis", 24:"Zaghouan",
}


# ==============================================================================
# 1. CHARGEMENT ET PRÉPARATION DES DONNÉES
# ==============================================================================

def load_and_prepare_data():
    """
    Charge BO3_with_ruptures.csv (sortie TCN) et cluster_summary (sortie segmentation).
    Applique double winsorizing p5/p95 par segment pour neutraliser les anomalies.
    """
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            f"Fichier manquant : {INPUT_PATH}\n"
            f"-> Lance d'abord tcn_ruptures.py pour générer ce fichier."
        )
    if not os.path.exists(CLUSTER_SUMMARY_PATH):
        raise FileNotFoundError(
            f"Fichier manquant : {CLUSTER_SUMMARY_PATH}\n"
            f"-> Lance d'abord segmentation.py pour générer ce fichier."
        )

    df = pd.read_csv(INPUT_PATH)
    cluster_summary = pd.read_csv(CLUSTER_SUMMARY_PATH)

    # Typage
    df["date"]    = pd.to_datetime(df["date"], errors="coerce")
    df["cluster"] = pd.to_numeric(df["cluster"], errors="coerce")
    df["sample_weight_temporal"] = pd.to_numeric(df["sample_weight_temporal"], errors="coerce")
    df["annee"]   = pd.to_numeric(df["annee"], errors="coerce")
    df["rupture"]      = pd.to_numeric(df.get("rupture", 0), errors="coerce").fillna(0)
    df["soft_rupture"] = pd.to_numeric(df.get("soft_rupture", 0), errors="coerce").fillna(0)

    # Filtrage : observations réelles uniquement (exclut projections 2026)
    df = df.dropna(subset=["date", "cluster", "sample_weight_temporal",
                            "annee", "indice_prix_m2_regional"])
    df = df[(df["sample_weight_temporal"] > 0.12) & (df["annee"] <= 2026)].copy()

    # Double winsorizing p5/p95 par segment
    for seg in df["segment"].unique():
        mask = df["segment"] == seg
        p5   = df.loc[mask, "indice_prix_m2_regional"].quantile(0.05)
        p95  = df.loc[mask, "indice_prix_m2_regional"].quantile(0.95)
        df.loc[mask, "indice_prix_m2_regional"] = (
            df.loc[mask, "indice_prix_m2_regional"].clip(lower=p5, upper=p95)
        )

    # Lookup cluster_name
    cluster_lookup = (
        cluster_summary[["segment", "cluster", "cluster_name"]]
        .dropna(subset=["segment", "cluster"]).copy()
    )
    cluster_lookup["cluster"] = pd.to_numeric(cluster_lookup["cluster"], errors="coerce")
    cluster_lookup = (cluster_lookup
        .dropna(subset=["cluster"])
        .drop_duplicates(["segment", "cluster"]))

    return df, cluster_summary, cluster_lookup


# ==============================================================================
# 2. EXTRACTION DES DONNÉES ENRICHIES PAR CLUSTER
# ==============================================================================

def extract_cluster_enriched_features(cluster_summary: pd.DataFrame,
                                       segment: str, cluster: int) -> dict:
    """
    Extrait les features enrichies du cluster depuis cluster_summary_by_gouvernorat_real.csv.
    Ces features (momentum_prix, variation_annuelle, glissement_recent, score_attractivite)
    servent à corriger/enrichir la prévision ETS après modélisation.

    Retourne un dict avec les moyennes par cluster.
    """
    mask = (
        (cluster_summary["segment"] == segment) &
        (pd.to_numeric(cluster_summary["cluster"], errors="coerce") == cluster)
    )
    sub = cluster_summary[mask]

    if sub.empty:
        return {
            "momentum_prix"      : 1.0,
            "variation_annuelle" : 0.0,
            "glissement_recent"  : 0.0,
            "score_attractivite" : 0.5,
            "n_gouvernorats"     : 0,
        }

    # Variation annuelle calculée depuis prix_m2_moyen et prix_m2_recent
    prix_moy    = pd.to_numeric(sub.get("prix_m2_moyen",    pd.Series([np.nan])), errors="coerce").mean()
    prix_recent = pd.to_numeric(sub.get("prix_m2_recent",   pd.Series([np.nan])), errors="coerce").mean()
    if pd.notna(prix_moy) and pd.notna(prix_recent) and prix_moy > 0:
        variation_annuelle = (prix_recent - prix_moy) / prix_moy * 100
    else:
        variation_annuelle = 0.0

    # FIX-2: clip extreme momentum values and log a warning.
    raw_momentum = float(pd.to_numeric(
        sub.get("momentum_prix", pd.Series([1.0])), errors="coerce"
    ).mean())
    if np.isfinite(raw_momentum) and raw_momentum > 2.0:
        print(f"  [WARN] momentum_prix={raw_momentum:.2f} pour {segment} C{cluster} -> clippé à 2.0")
    momentum = float(np.clip(raw_momentum if np.isfinite(raw_momentum) else 1.0, 0.50, 2.00))

    return {
        "momentum_prix"      : momentum,
        "variation_annuelle" : float(np.clip(variation_annuelle, -30, 50)),
        "glissement_recent"  : pd.to_numeric(sub.get("glissement_recent",  pd.Series([0.0])), errors="coerce").mean(),
        "score_attractivite" : pd.to_numeric(sub.get("score_attractivite", pd.Series([0.5])), errors="coerce").mean(),
        "n_gouvernorats"     : int(len(sub)),
    }


def extract_rupture_features(df_sc: pd.DataFrame) -> dict:
    """
    Extrait les informations de rupture issues de TCN pour ce cluster.
    Utilisé pour détecter si une rupture a eu lieu récemment et ajuster
    la prévision en conséquence.
    """
    tmp = df_sc.copy()
    tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce")
    tmp = tmp.dropna(subset=["date"]).sort_values("date")

    # Build true rupture events from rupture_count level changes over time.
    # This avoids counting all post-rupture rows repeatedly.
    tmp["rupture_count"] = pd.to_numeric(tmp.get("rupture_count", 0), errors="coerce").fillna(0)
    monthly_level = tmp.groupby("date", as_index=True)["rupture_count"].max().sort_index()
    monthly_delta = monthly_level.diff().fillna(monthly_level)
    rupture_event_dates = monthly_delta[monthly_delta > 0].index

    n_ruptures = int(len(rupture_event_dates))
    n_soft = int(tmp.groupby("date", as_index=True)["soft_rupture"].max().gt(0).sum())
    last_rupture = rupture_event_dates.max() if n_ruptures > 0 else pd.NaT

    # Direction dominante des ruptures
    direction = "stable"
    if "rupture_direction" in tmp.columns and n_ruptures > 0:
        dirs = tmp[tmp["date"].isin(rupture_event_dates)]["rupture_direction"].dropna()
        if not dirs.empty:
            direction = dirs.mode().iloc[0]

    # Mois depuis la dernière rupture
    max_date = df_sc["date"].max()
    months_since_rupture = 0
    if pd.notna(last_rupture):
        months_since_rupture = max(0, (max_date - last_rupture).days // 30)

    return {
        "n_ruptures"           : n_ruptures,
        "n_soft_ruptures"      : n_soft,
        "last_rupture"         : last_rupture,
        "rupture_direction"    : direction,
        "months_since_rupture" : months_since_rupture,
        "has_recent_rupture"   : months_since_rupture <= 12 and n_ruptures > 0,
    }


# ==============================================================================
# 3. CONSTRUCTION DE LA SÉRIE TEMPORELLE TRIMESTRIELLE
# ==============================================================================

def build_quarterly_series(df_sc: pd.DataFrame) -> pd.Series:
    """
    Agrège les données mensuelles en série trimestrielle par cluster.
    L'agrégation trimestrielle réduit le bruit de 4x vs mensuelle,
    ce qui est essentiel pour ETS sur des séries courtes.
    """
    df_sc = df_sc.copy()
    df_sc["quarter"] = df_sc["date"].dt.to_period("Q").dt.start_time

    y = (
        df_sc.groupby("quarter")["indice_prix_m2_regional"]
        .mean()
        .sort_index()
    )

    # Remplir les trimestres manquants par interpolation linéaire
    full_qtr = pd.date_range(y.index.min(), y.index.max(), freq="QS")
    y = y.reindex(full_qtr).interpolate(method="linear").ffill().bfill()

    return y.astype(float)


# ==============================================================================
# 4. MODÉLISATION ETS (EXPONENTIAL SMOOTHING)
# ==============================================================================

def fit_ets_variant(y_train: pd.Series, variant: str):
    """Fit one explicit ETS variant and return (model, meta)."""
    if variant == "Holt-damped":
        model = ExponentialSmoothing(
            y_train,
            trend="add",
            damped_trend=True,
            initialization_method="estimated",
        ).fit(optimized=True, remove_bias=True)
    elif variant == "Holt":
        model = ExponentialSmoothing(
            y_train,
            trend="add",
            initialization_method="estimated",
        ).fit(optimized=True)
    elif variant == "SES":
        model = SimpleExpSmoothing(
            y_train,
            initialization_method="estimated",
        ).fit(optimized=True)
    else:
        raise ValueError(f"Variant ETS inconnue: {variant}")

    return model, {
        "model_type": variant,
        "aic": float(model.aic) if hasattr(model, "aic") else np.nan,
        "alpha": float(model.params.get("smoothing_level", np.nan)),
        "note": "",
    }


def forecast_seasonal_naive(y_train: pd.Series, horizon: int, season: int = 4) -> np.ndarray:
    vals = np.asarray(y_train, dtype=float)
    if len(vals) == 0:
        return np.zeros(horizon, dtype=float)
    if len(vals) < season:
        pred = np.repeat(vals[-1], horizon)
    else:
        tail = vals[-season:]
        pred = np.array([tail[i % season] for i in range(horizon)], dtype=float)
    return np.clip(pred, 1e-3, None)


def forecast_drift(y_train: pd.Series, horizon: int) -> np.ndarray:
    vals = np.asarray(y_train, dtype=float)
    if len(vals) == 0:
        return np.zeros(horizon, dtype=float)
    if len(vals) < 2:
        pred = np.repeat(vals[-1], horizon)
    else:
        slope = (vals[-1] - vals[0]) / max(1, len(vals) - 1)
        pred = np.array([vals[-1] + slope * (i + 1) for i in range(horizon)], dtype=float)
    return np.clip(pred, 1e-3, None)


def forecast_with_model_name(y_train: pd.Series, model_name: str, horizon: int) -> np.ndarray:
    if model_name in {"Holt-damped", "Holt", "SES"}:
        model, _ = fit_ets_variant(y_train, model_name)
        pred = np.asarray(model.forecast(horizon), dtype=float)
        return np.clip(pred, 1e-3, None)
    if model_name == "SEASONAL_NAIVE":
        return forecast_seasonal_naive(y_train, horizon=horizon, season=4)
    if model_name == "DRIFT":
        return forecast_drift(y_train, horizon=horizon)
    raise ValueError(f"Model inconnu: {model_name}")


def estimate_residuals(y_train: pd.Series, model_name: str) -> np.ndarray:
    """Estimate in-sample 1-step residuals for CI fallback."""
    vals = np.asarray(y_train, dtype=float)
    errors = []
    min_start = 6 if model_name in {"Holt", "Holt-damped", "SES", "DRIFT"} else 4
    for t in range(min_start, len(vals)):
        hist = pd.Series(vals[:t])
        y_true = vals[t]
        if y_true <= 1e-3:
            continue
        try:
            y_pred = float(forecast_with_model_name(hist, model_name, horizon=1)[0])
            errors.append(y_true - y_pred)
        except Exception:
            continue
    if not errors:
        return np.array([0.0], dtype=float)
    return np.asarray(errors, dtype=float)


def fit_ets_model(y_train: pd.Series, preferred_variant: str | None = None):
    """
    Sélection automatique du meilleur modèle ETS selon la longueur de la série :

    n >= 8 points -> Holt avec tendance amortie (damped)
      - Capture la tendance sans l'extrapoler à l'infini
      - Optimal pour séries courtes avec tendance
      - Evite les explosions (+1955%) vues avec ARIMA

    n >= 6 points -> Holt simple (sans amortissement)
      - Si damped échoue sur très peu de points

    n < 6         -> SES (Simple Exponential Smoothing)
      - Pas de tendance, moyenne lissée uniquement

    Retourne (model_fitted, meta_dict)
    """
    n    = len(y_train)
    meta = {"model_type": None, "aic": np.nan, "alpha": np.nan, "note": ""}

    if preferred_variant:
        return fit_ets_variant(y_train, preferred_variant)

    # Tentative 1 : Holt avec tendance amortie
    if n >= 8:
        try:
            model = ExponentialSmoothing(
                y_train,
                trend="add",
                damped_trend=True,
                initialization_method="estimated",
            ).fit(optimized=True, remove_bias=True)

            meta["model_type"] = "Holt-damped"
            meta["aic"]        = float(model.aic) if hasattr(model, "aic") else np.nan
            meta["alpha"]      = float(model.params.get("smoothing_level", np.nan))
            return model, meta
        except Exception as e:
            meta["note"] = f"Holt-damped échoué: {e}"

    # Tentative 2 : Holt simple
    if n >= 6:
        try:
            model = ExponentialSmoothing(
                y_train,
                trend="add",
                initialization_method="estimated",
            ).fit(optimized=True)

            meta["model_type"] = "Holt"
            meta["aic"]        = float(model.aic) if hasattr(model, "aic") else np.nan
            meta["alpha"]      = float(model.params.get("smoothing_level", np.nan))
            meta["note"]      += " | Holt sans damping"
            return model, meta
        except Exception as e:
            meta["note"] += f" | Holt échoué: {e}"

    # Fallback : SES
    model = SimpleExpSmoothing(
        y_train, initialization_method="estimated"
    ).fit(optimized=True)

    meta["model_type"] = "SES"
    meta["aic"]        = float(model.aic) if hasattr(model, "aic") else np.nan
    meta["alpha"]      = float(model.params.get("smoothing_level", np.nan))
    meta["note"]      += " | fallback SES"
    return model, meta


def select_best_variant_rolling(
    y_q: pd.Series,
    test_quarters: int = TEST_QUARTERS,
    min_rolling_steps: int = 3,
) -> tuple[str, float]:
    """Select best model via rolling-origin 1-step validation."""
    # FIX-1: enforce a minimum number of rolling validation steps.
    variants = ["Holt-damped", "Holt", "SES", "SEASONAL_NAIVE", "DRIFT"]
    errors_by_variant = {v: [] for v in variants}
    rolling_steps = 0

    start = max(MIN_POINTS, len(y_q) - test_quarters)
    for t in range(start, len(y_q)):
        y_train = y_q.iloc[:t]
        y_true = float(y_q.iloc[t])
        if len(y_train) < MIN_POINTS or y_true <= 1e-3:
            continue
        rolling_steps += 1

        for variant in variants:
            if variant == "Holt-damped" and len(y_train) < 8:
                continue
            if variant == "Holt" and len(y_train) < 6:
                continue
            if variant == "SEASONAL_NAIVE" and len(y_train) < 4:
                continue
            if variant == "DRIFT" and len(y_train) < 2:
                continue
            try:
                pred = float(forecast_with_model_name(y_train, variant, horizon=1)[0])
                err = abs((y_true - pred) / y_true) * 100.0
                errors_by_variant[variant].append(err)
            except Exception:
                continue

    if rolling_steps < min_rolling_steps:
        return "SES", np.nan

    avg_mape = {
        variant: (float(np.mean(errs)) if errs else np.inf)
        for variant, errs in errors_by_variant.items()
    }
    best_variant = min(avg_mape, key=avg_mape.get)
    best_mape = avg_mape[best_variant]
    if not np.isfinite(best_mape):
        return "SES", np.nan
    return best_variant, best_mape


def compute_mape_for_model(y_train: pd.Series, y_test: pd.Series, model_name: str) -> float:
    """Compute out-of-sample MAPE using a specific model family."""
    try:
        pred = forecast_with_model_name(y_train, model_name, horizon=len(y_test))
        actual = np.asarray(y_test, dtype=float)
        mask = actual > 1e-3
        if not mask.any():
            return np.nan
        return float(np.mean(np.abs((actual[mask] - pred[mask]) / actual[mask])) * 100.0)
    except Exception:
        return np.nan


# ==============================================================================
# 5. VALIDATION MAPE (OUT-OF-SAMPLE)
# ==============================================================================

def compute_mape(model, y_test: pd.Series) -> float:
    """
    MAPE calculé sur les 4 derniers trimestres (validation out-of-sample).
    Donne une mesure réelle de la qualité prédictive du modèle.
    """
    # FIX-1: avoid unstable MAPE values when test horizon is too short.
    if len(y_test) < 3:
        return np.nan

    try:
        fcst    = np.clip(np.asarray(model.forecast(len(y_test)), float), 1e-3, None)
        actual  = y_test.values
        mask    = actual > 1e-3
        if mask.sum() == 0:
            return np.nan
        return float(np.mean(np.abs((actual[mask] - fcst[mask]) / actual[mask])) * 100)
    except Exception:
        return np.nan


# ==============================================================================
# 6. CORRECTION ENRICHIE DE LA PRÉVISION
# ==============================================================================

def apply_enrichment_correction(ets_forecast: np.ndarray,
                                 enriched: dict,
                                 rupture_info: dict) -> np.ndarray:
    """
    Enrichit la prévision ETS avec les données cluster (segmentation + TCN).

    Logique métier :
    +-----------------------------------------------------------------+
    |  ETS prédit la trajectoire de base (60% du poids final)         |
    |  Correction enrichie = 40% basée sur :                          |
    |    • momentum_prix     -> accélération récente des prix           |
    |    • variation_annuelle-> tendance YoY observée                   |
    |    • rupture récente   -> choc TCN -> amplifie hausse/baisse       |
    +-----------------------------------------------------------------+

    Pourquoi 60/40 et non pas 100% ETS ?
    -> Sur des séries de 6-16 points, ETS n'a pas assez d'historique
      pour capturer la tendance structurelle. Les features enrichies
      issues du clustering (48 mois, 22 gouvernorats) apportent un
      signal complémentaire plus robuste.

    Caps de sécurité : +-25% max par rapport à ETS
    -> évite les corrections irréalistes même si les features sont extrêmes.
    """
    mom      = float(enriched.get("momentum_prix", 1.0) or 1.0)
    var_ann  = float(enriched.get("variation_annuelle", 0.0) or 0.0)

    # FIX-2: reduce momentum influence and tighten caps.
    # Facteur momentum : amplifie si marché en accélération
    # mom > 1 -> prix récents > prix historiques -> tendance haussière
    momentum_factor = 1.0 + 0.20 * (mom - 1.0)
    momentum_factor = float(np.clip(momentum_factor, 0.85, 1.20))

    # Facteur variation annuelle : ancrage sur la tendance réelle observée
    annual_factor = 1.0 + (var_ann / 100.0) * 0.40
    annual_factor = float(np.clip(annual_factor, 0.85, 1.25))

    # Facteur rupture TCN : si rupture récente détectée, on l'intègre
    rupture_factor = 1.0
    if rupture_info.get("has_recent_rupture", False):
        direction = rupture_info.get("rupture_direction", "stable")
        months_ago = rupture_info.get("months_since_rupture", 12)
        # L'effet de la rupture s'atténue avec le temps (décroissance exponentielle)
        decay = np.exp(-months_ago / 12.0)  # demi-vie = ~8 mois
        if direction == "hausse":
            rupture_factor = 1.0 + 0.08 * decay   # +8% max si rupture haussière récente
        elif direction == "baisse":
            rupture_factor = 1.0 - 0.06 * decay   # -6% max si rupture baissière récente
        rupture_factor = float(np.clip(rupture_factor, 0.88, 1.12))

    # Correction combinée
    enrichment_multiplier = momentum_factor * annual_factor * rupture_factor
    enrichment_multiplier = float(np.clip(enrichment_multiplier, 0.75, 1.25))  # cap global +-25%

    # Prévision finale : 60% ETS pur + 40% ETS corrigé par les données enrichies
    corrected = ets_forecast * 0.60 + ets_forecast * enrichment_multiplier * 0.40

    return np.clip(corrected, 1e-3, None)


def apply_forecast_guardrail(
    forecast_q: np.ndarray,
    last_price: float,
    rolling_mape: float,
) -> tuple[np.ndarray, dict]:
    """
    Apply adaptive caps to quarterly forecast levels based on model reliability.
    Higher rolling-MAPE => tighter caps to prevent unrealistic jumps.
    """
    if not np.isfinite(last_price) or last_price <= 1e-3:
        return np.clip(forecast_q, 1e-3, None), {
            "applied": False,
            "cap_up": np.nan,
            "cap_down": np.nan,
            "reason": "invalid_last_price",
        }

    if pd.isna(rolling_mape):
        cap_up, cap_down = 0.35, -0.35
    elif rolling_mape <= 10:
        cap_up, cap_down = 0.50, -0.40
    elif rolling_mape <= 20:
        cap_up, cap_down = 0.30, -0.25
    elif rolling_mape <= 35:
        cap_up, cap_down = 0.20, -0.18
    else:
        cap_up, cap_down = 0.12, -0.12

    lower = last_price * (1.0 + cap_down)
    upper = last_price * (1.0 + cap_up)
    guarded = np.clip(np.asarray(forecast_q, dtype=float), lower, upper)
    applied = bool(np.any(np.abs(guarded - forecast_q) > 1e-9))

    return np.clip(guarded, 1e-3, None), {
        "applied": applied,
        "cap_up": cap_up,
        "cap_down": cap_down,
        "reason": "adaptive_rolling_mape",
    }


def recommend_usage(mape: float, rolling_mape: float, ci_ratio: float) -> str:
    """Return automatic operational recommendation for forecast usage."""
    # FIX-1: separate missing-validation cases from true scenario usage.
    if pd.isna(mape) and pd.isna(rolling_mape):
        return "INSUFFICIENT_DATA"
    if pd.isna(mape) or pd.isna(rolling_mape) or pd.isna(ci_ratio):
        return "SCENARIO"
    if (
        mape <= RECO_CENTRAL_MAX_MAPE
        and rolling_mape <= RECO_CENTRAL_MAX_ROLLING_MAPE
        and ci_ratio <= RECO_CENTRAL_MAX_CI_RATIO
    ):
        return "CENTRAL"
    return "SCENARIO"


def build_segment_fallback_cache(df: pd.DataFrame) -> dict:
    """Build one stabilized segment-level forecast per segment for weak-cluster fallback."""
    cache = {}
    for segment, df_seg in df.groupby("segment", dropna=False):
        if pd.isna(segment):
            continue
        try:
            y_seg = build_quarterly_series(df_seg)
        except Exception:
            continue
        if len(y_seg) < MIN_POINTS + TEST_QUARTERS:
            continue
        best_variant, rolling_mape = select_best_variant_rolling(y_seg, test_quarters=TEST_QUARTERS)
        try:
            fcst_q = forecast_with_model_name(y_seg, best_variant, horizon=HORIZON_QUARTERS)
        except Exception:
            continue
        last_segment_price = float(y_seg.iloc[-1])
        fcst_q, _ = apply_forecast_guardrail(fcst_q, last_segment_price, rolling_mape)
        cache[segment] = {
            "forecast_q": fcst_q,
            "last_segment_price": last_segment_price,
            "model_type": best_variant,
            "rolling_mape": rolling_mape,
        }
    return cache


# ==============================================================================
# 7. INTERVALLES DE CONFIANCE PAR BOOTSTRAP
# ==============================================================================

def compute_bootstrap_ci(model, forecast_corrected: np.ndarray,
                          n_bootstrap: int = 300, ci_level: float = 0.95) -> np.ndarray:
    """
    Intervalles de confiance à 95% par bootstrap sur les résidus du modèle.
    ETS ne fournit pas d'IC analytique -> simulation Monte Carlo sur les résidus.
    Retourne array (n_horizons, 2) : [lower, upper]
    """
    try:
        residuals = np.asarray(model.resid, float) if model is not None else np.array([])
        residuals = residuals[np.isfinite(residuals)]

        if len(residuals) < 3:
            pct = (1 - ci_level) / 2
            return np.stack([
                forecast_corrected * (1 - pct * 2),
                forecast_corrected * (1 + pct * 2)
            ], axis=1)

        np.random.seed(42)
        sims = []
        for _ in range(n_bootstrap):
            boot = np.random.choice(residuals, size=len(forecast_corrected), replace=True)
            sims.append(np.clip(forecast_corrected + boot, 1e-3, None))

        sims  = np.array(sims)
        alpha = (1 - ci_level) / 2
        lower = np.percentile(sims, alpha * 100, axis=0)
        upper = np.percentile(sims, (1 - alpha) * 100, axis=0)

        return np.clip(np.stack([lower, upper], axis=1), 1e-3, None)

    except Exception:
        return np.stack([
            forecast_corrected * 0.85,
            forecast_corrected * 1.15
        ], axis=1)


# ==============================================================================
# 8. INTERPOLATION TRIMESTRIELLE -> MENSUELLE
# ==============================================================================

def quarterly_to_monthly(fcst_q: np.ndarray,
                          conf_q: np.ndarray,
                          last_quarterly_date: pd.Timestamp) -> tuple:
    """
    Interpole les prévisions trimestrielles en valeurs mensuelles (12 mois).
    Utilise une interpolation linéaire entre les points trimestriels.
    """
    q_start = last_quarterly_date + pd.offsets.QuarterBegin(1)
    q_dates = pd.date_range(q_start, periods=len(fcst_q), freq="QS")
    m_dates = pd.date_range(q_dates[0], periods=len(fcst_q) * 3, freq="MS")

    def interp_series(values):
        s = pd.Series(values, index=q_dates).reindex(m_dates).interpolate("linear")
        return s

    return (
        interp_series(fcst_q),
        interp_series(conf_q[:, 0]),
        interp_series(conf_q[:, 1]),
    )


# ==============================================================================
# 9. PIPELINE PRINCIPAL
# ==============================================================================

def run_pipeline():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    np.random.seed(42)

    print("=" * 65)
    print("  SÉRIES TEMPORELLES PAR CLUSTER — DONNÉES ENRICHIES")
    print("  Modèle : ETS/Holt-damped + Correction TCN + Segmentation")
    print("=" * 65)

    df, cluster_summary, cluster_lookup = load_and_prepare_data()
    segment_fallback_cache = build_segment_fallback_cache(df)

    forecast_rows = []
    summary_rows  = []
    model_store   = {}

    for (segment, cluster), df_sc in df.groupby(["segment", "cluster"], dropna=False):
        if pd.isna(cluster):
            continue
        cluster = int(cluster)

        # -- Nom du cluster ----------------------------------------------------
        cluster_name_vals = df_sc["cluster_name"].dropna().astype(str)
        if not cluster_name_vals.empty:
            cluster_name = cluster_name_vals.mode().iloc[0]
        else:
            ref = cluster_lookup[
                (cluster_lookup["segment"] == segment) &
                (cluster_lookup["cluster"] == cluster)
            ]
            cluster_name = (str(ref["cluster_name"].iloc[0])
                            if not ref.empty else f"Cluster {cluster}")

        # -- Données enrichies (segmentation + TCN) ---------------------------
        enriched     = extract_cluster_enriched_features(cluster_summary, segment, cluster)
        rupture_info = extract_rupture_features(df_sc)

        # -- Série trimestrielle -----------------------------------------------
        try:
            y_q = build_quarterly_series(df_sc)
        except Exception as e:
            print(f"  X {segment} C{cluster}: série non construite ({e})")
            continue

        n_total = len(y_q)
        if n_total < MIN_POINTS + TEST_QUARTERS:
            # FIX-3: preserve short clusters using segment-level fallback instead of skipping.
            print(f"  X {segment} C{cluster} ({cluster_name}): "
                  f"trop peu de points ({n_total} trimestres, min={MIN_POINTS + TEST_QUARTERS})")

            seg_fb = segment_fallback_cache.get(segment)
            if seg_fb is None or seg_fb["last_segment_price"] <= 1e-3:
                continue

            last_price = float(y_q.iloc[-1]) if len(y_q) > 0 else float(seg_fb["last_segment_price"])
            scale = float(np.clip(last_price / seg_fb["last_segment_price"], 0.15, 1.80))
            if scale < 0.10:
                print(f"  [SKIP] {segment} C{cluster}: scale={scale:.3f} trop faible "
                      f"(last_price={last_price:.0f} vs segment={seg_fb['last_segment_price']:.0f}) "
                      f"-> prévision non fiable, cluster ignoré")
                continue
            fcst_guarded = np.asarray(seg_fb["forecast_q"], dtype=float) * scale
            conf_q = np.column_stack([
                np.clip(fcst_guarded * 0.80, 1e-3, None),
                np.clip(fcst_guarded * 1.20, 1e-3, None),
            ])
            last_q_date = y_q.index[-1] if len(y_q) > 0 else pd.Timestamp("2025-10-01")
            fcst_m, lower_m, upper_m = quarterly_to_monthly(fcst_guarded, conf_q, last_q_date)

            forecast_mean = float(fcst_m.mean())
            change_pct = ((forecast_mean - last_price) / last_price * 100) if abs(last_price) > 1e-3 else np.nan
            ci_ratio = float(((upper_m - lower_m) / np.maximum(fcst_m, 1e-3)).mean())

            best_variant = str(seg_fb["model_type"])
            rolling_mape = float(seg_fb["rolling_mape"]) if pd.notna(seg_fb["rolling_mape"]) else np.nan
            mape = np.nan
            model_aic = np.nan
            guard_info = {
                "applied": False,
                "cap_up": np.nan,
                "cap_down": np.nan,
                "reason": "short_series_segment_fallback",
            }
            fallback_applied = True
            fallback_segment_model = best_variant
            fallback_blend_weight = 1.0
            recommendation = "INDICATIF_SEULEMENT"
            quality = "[!] TRES_FAIBLE"

            print(f"  [WARN] fallback court-terme appliqué pour {segment} C{cluster} (model={fallback_segment_model})")
            print(f"  Prévision  : {last_price:.0f} -> {forecast_mean:.0f} TND ({change_pct:+.1f}%)")
            print(f"  Reco usage : {recommendation}")

            for dt in fcst_m.index:
                forecast_rows.append({
                    "date"              : dt,
                    "segment"           : segment,
                    "cluster"           : cluster,
                    "cluster_name"      : cluster_name,
                    "forecast"          : float(fcst_m[dt]),
                    "lower_95"          : float(lower_m[dt]),
                    "upper_95"          : float(upper_m[dt]),
                    "model_type"        : best_variant,
                    "mape_pct"          : mape,
                    "rolling_mape_pct"  : rolling_mape,
                    "guardrail_applied" : guard_info["applied"],
                    "guardrail_cap_up"  : guard_info["cap_up"],
                    "guardrail_cap_down": guard_info["cap_down"],
                    "fallback_segment_applied": fallback_applied,
                    "fallback_segment_model": fallback_segment_model,
                    "fallback_blend_weight": fallback_blend_weight,
                    "model_aic"         : model_aic,
                    "quality"           : quality.split()[1],
                    "recommendation_usage": recommendation,
                    # Données enrichies tracées dans le CSV pour analyse
                    "momentum_prix"     : enriched["momentum_prix"],
                    "variation_annuelle": enriched["variation_annuelle"],
                    "n_ruptures"        : rupture_info["n_ruptures"],
                    "rupture_direction" : rupture_info["rupture_direction"],
                })

            summary_rows.append({
                "segment"             : segment,
                "cluster"             : cluster,
                "cluster_name"        : cluster_name,
                "model_type"          : best_variant,
                "aic"                 : model_aic,
                "mape_pct"            : mape,
                "rolling_mape_pct"    : rolling_mape,
                "guardrail_applied"   : guard_info["applied"],
                "guardrail_cap_up"    : guard_info["cap_up"],
                "guardrail_cap_down"  : guard_info["cap_down"],
                "fallback_segment_applied": fallback_applied,
                "fallback_segment_model": fallback_segment_model,
                "fallback_blend_weight": fallback_blend_weight,
                "last_known_price"    : last_price,
                "forecast_mean_12m"   : forecast_mean,
                "forecast_change_pct" : change_pct,
                "ci_width_ratio"      : ci_ratio,
                "quality"             : quality.split()[1],
                "recommendation_usage": recommendation,
                "n_quarters_train"    : n_total,
                "n_gouvernorats"      : enriched["n_gouvernorats"],
                "momentum_prix"       : enriched["momentum_prix"],
                "variation_annuelle"  : enriched["variation_annuelle"],
                "glissement_recent"   : enriched["glissement_recent"],
                "score_attractivite"  : enriched["score_attractivite"],
                "n_ruptures_tcn"      : rupture_info["n_ruptures"],
                "rupture_direction"   : rupture_info["rupture_direction"],
                "has_recent_rupture"  : rupture_info["has_recent_rupture"],
            })

            model_store[(segment, cluster)] = {
                "model"       : None,
                "meta"        : {
                    "model_type": best_variant,
                    "aic": np.nan,
                    "alpha": np.nan,
                    "note": "fallback short series via segment cache",
                },
                "cluster_name": cluster_name,
                "enriched"    : enriched,
                "rupture_info": rupture_info,
                "fallback_segment_applied": fallback_applied,
                "fallback_segment_model": fallback_segment_model,
                "fallback_blend_weight": fallback_blend_weight,
                "recommendation_usage": recommendation,
            }
            continue

        # -- Train/Test split --------------------------------------------------
        split   = n_total - TEST_QUARTERS
        y_train = y_q.iloc[:split]
        y_test  = y_q.iloc[split:]

        # -- Sélection variante ETS via rolling-origin ------------------------
        best_variant, rolling_mape = select_best_variant_rolling(y_q, test_quarters=TEST_QUARTERS)

        # -- Entraînement sur train -> MAPE -------------------------------------
        model_val = None
        if best_variant in {"Holt-damped", "Holt", "SES"}:
            try:
                model_val, _ = fit_ets_model(y_train, preferred_variant=best_variant)
            except Exception as e:
                print(f"  X {segment} C{cluster}: entraînement échoué ({e})")
                continue

        mape = compute_mape_for_model(y_train, y_test, best_variant)

        # -- Ré-entraînement sur toute la série -> forecast final ---------------
        if best_variant in {"Holt-damped", "Holt", "SES"}:
            try:
                model_full, meta_full = fit_ets_model(y_q, preferred_variant=best_variant)
            except Exception:
                model_full = model_val
                meta_full = {
                    "model_type": best_variant,
                    "aic": np.nan,
                    "alpha": np.nan,
                    "note": "fallback model_val",
                }
        else:
            model_full = None
            meta_full = {
                "model_type": best_variant,
                "aic": np.nan,
                "alpha": np.nan,
                "note": "baseline model selected by rolling validation",
            }

        # -- Prévision ETS brute -----------------------------------------------
        try:
            fcst_raw = forecast_with_model_name(y_q, best_variant, horizon=HORIZON_QUARTERS)
        except Exception as e:
            print(f"  X {segment} C{cluster}: forecast échoué ({e})")
            continue

        last_price = float(y_q.iloc[-1])

        # -- Correction enrichie (données segmentation + TCN) ------------------
        fcst_corrected = apply_enrichment_correction(fcst_raw, enriched, rupture_info)

        # -- Guardrail adaptatif selon fiabilité (rolling-MAPE) ----------------
        fcst_guarded, guard_info = apply_forecast_guardrail(
            fcst_corrected,
            last_price=last_price,
            rolling_mape=rolling_mape,
        )

        # -- Fallback segment-level pour clusters faibles ----------------------
        fallback_applied = False
        fallback_segment_model = ""
        fallback_blend_weight = 0.0
        if (
            (pd.notna(mape) and mape >= WEAK_CLUSTER_MAPE_THRESHOLD)
            or (pd.notna(rolling_mape) and rolling_mape >= WEAK_CLUSTER_ROLLING_THRESHOLD)
        ):
            seg_fb = segment_fallback_cache.get(segment)
            if seg_fb is not None and seg_fb["last_segment_price"] > 1e-3:
                scale = float(np.clip(last_price / seg_fb["last_segment_price"], 0.15, 1.80))
                segment_scaled_fcst = np.asarray(seg_fb["forecast_q"], dtype=float) * scale
                fcst_guarded = (
                    (1.0 - FALLBACK_SEGMENT_BLEND_WEIGHT) * np.asarray(fcst_guarded, dtype=float)
                    + FALLBACK_SEGMENT_BLEND_WEIGHT * segment_scaled_fcst
                )
                fcst_guarded, guard_info = apply_forecast_guardrail(
                    fcst_guarded,
                    last_price=last_price,
                    rolling_mape=rolling_mape,
                )
                fallback_applied = True
                fallback_segment_model = str(seg_fb["model_type"])
                fallback_blend_weight = float(FALLBACK_SEGMENT_BLEND_WEIGHT)

        # -- Intervalles de confiance (bootstrap) ------------------------------
        conf_q = compute_bootstrap_ci(model_full, fcst_guarded)
        if model_full is None:
            residuals = estimate_residuals(y_q, best_variant)
            resid_scale = float(np.nanstd(residuals, ddof=1)) if residuals.size > 1 else float(np.nanstd(residuals))
            if np.isfinite(resid_scale) and resid_scale > 1e-9:
                conf_q = np.column_stack([
                    np.clip(fcst_guarded - 1.96 * resid_scale, 1e-3, None),
                    np.clip(fcst_guarded + 1.96 * resid_scale, 1e-3, None),
                ])

        # -- Interpolation -> mensuelle -----------------------------------------
        fcst_m, lower_m, upper_m = quarterly_to_monthly(fcst_guarded, conf_q, y_q.index[-1])

        # -- Métriques ---------------------------------------------------------
        forecast_mean = float(fcst_m.mean())
        change_pct    = ((forecast_mean - last_price) / last_price * 100) if abs(last_price) > 1e-3 else np.nan
        ci_ratio      = float(((upper_m - lower_m) / np.maximum(fcst_m, 1e-3)).mean())
        model_aic     = float(meta_full["aic"]) if pd.notna(meta_full.get("aic")) else np.nan
        recommendation = recommend_usage(mape, rolling_mape, ci_ratio)

        quality = (
            "[!] TRES_FAIBLE" if np.isnan(mape) and np.isnan(rolling_mape) else
            "[OK] BON" if not np.isnan(mape) and mape < 8 and ci_ratio < 0.30 else
            "[!] MOYEN" if not np.isnan(mape) and mape < 15 else
            "[FAIL] FAIBLE"
        )

        # -- Affichage ---------------------------------------------------------
        mape_str = f"{mape:.1f}%" if not np.isnan(mape) else "N/A"
        aic_str  = f"{model_aic:.1f}" if not np.isnan(model_aic) else "N/A"
        rup_str  = (f"rupture {rupture_info['rupture_direction']} "
                    f"({rupture_info['months_since_rupture']}m ago)"
                    if rupture_info["has_recent_rupture"] else "aucune rupture récente")

        print(f"\n{segment} · C{cluster} · {cluster_name}")
        print(f"  Modèle     : {meta_full['model_type']} | AIC: {aic_str} | MAPE: {mape_str} | {quality}")
        if pd.notna(rolling_mape):
            print(f"  Validation : rolling-MAPE={rolling_mape:.1f}% ({best_variant})")
        print(f"  Prévision  : {last_price:.0f} -> {forecast_mean:.0f} TND ({change_pct:+.1f}%)")
        if guard_info["applied"]:
            print(f"  Guardrail  : activé (cap {guard_info['cap_down']*100:+.0f}% / {guard_info['cap_up']*100:+.0f}%)")
        if fallback_applied:
            print(f"  Fallback   : segment-level blend ({fallback_blend_weight:.2f}, model={fallback_segment_model})")
        print(f"  Enrichi    : momentum={enriched['momentum_prix']:.2f} | "
              f"var_ann={enriched['variation_annuelle']:+.1f}% | {rup_str}")
        print(f"  Reco usage : {recommendation}")
        if meta_full.get("note"):
            print(f"  Note       : {meta_full['note']}")

        # -- Enregistrement prévisions mensuelles ------------------------------
        for dt in fcst_m.index:
            forecast_rows.append({
                "date"              : dt,
                "segment"           : segment,
                "cluster"           : cluster,
                "cluster_name"      : cluster_name,
                "forecast"          : float(fcst_m[dt]),
                "lower_95"          : float(lower_m[dt]),
                "upper_95"          : float(upper_m[dt]),
                "model_type"        : meta_full["model_type"],
                "mape_pct"          : mape,
                "rolling_mape_pct"  : rolling_mape,
                "guardrail_applied" : guard_info["applied"],
                "guardrail_cap_up"  : guard_info["cap_up"],
                "guardrail_cap_down": guard_info["cap_down"],
                "fallback_segment_applied": fallback_applied,
                "fallback_segment_model": fallback_segment_model,
                "fallback_blend_weight": fallback_blend_weight,
                "model_aic"         : model_aic,
                "quality"           : quality.split()[1],
                "recommendation_usage": recommendation,
                # Données enrichies tracées dans le CSV pour analyse
                "momentum_prix"     : enriched["momentum_prix"],
                "variation_annuelle": enriched["variation_annuelle"],
                "n_ruptures"        : rupture_info["n_ruptures"],
                "rupture_direction" : rupture_info["rupture_direction"],
            })

        summary_rows.append({
            "segment"             : segment,
            "cluster"             : cluster,
            "cluster_name"        : cluster_name,
            "model_type"          : meta_full["model_type"],
            "aic"                 : model_aic,
            "mape_pct"            : mape,
            "rolling_mape_pct"    : rolling_mape,
            "guardrail_applied"   : guard_info["applied"],
            "guardrail_cap_up"    : guard_info["cap_up"],
            "guardrail_cap_down"  : guard_info["cap_down"],
            "fallback_segment_applied": fallback_applied,
            "fallback_segment_model": fallback_segment_model,
            "fallback_blend_weight": fallback_blend_weight,
            "last_known_price"    : last_price,
            "forecast_mean_12m"   : forecast_mean,
            "forecast_change_pct" : change_pct,
            "ci_width_ratio"      : ci_ratio,
            "quality"             : quality.split()[1],
            "recommendation_usage": recommendation,
            "n_quarters_train"    : n_total,
            "n_gouvernorats"      : enriched["n_gouvernorats"],
            "momentum_prix"       : enriched["momentum_prix"],
            "variation_annuelle"  : enriched["variation_annuelle"],
            "glissement_recent"   : enriched["glissement_recent"],
            "score_attractivite"  : enriched["score_attractivite"],
            "n_ruptures_tcn"      : rupture_info["n_ruptures"],
            "rupture_direction"   : rupture_info["rupture_direction"],
            "has_recent_rupture"  : rupture_info["has_recent_rupture"],
        })

        model_store[(segment, cluster)] = {
            "model"       : model_full,
            "meta"        : meta_full,
            "cluster_name": cluster_name,
            "enriched"    : enriched,
            "rupture_info": rupture_info,
            "fallback_segment_applied": fallback_applied,
            "fallback_segment_model": fallback_segment_model,
            "fallback_blend_weight": fallback_blend_weight,
            "recommendation_usage": recommendation,
        }

    # -- Sauvegarde ------------------------------------------------------------
    forecast_df = (pd.DataFrame(forecast_rows)
                   .sort_values(["segment", "cluster", "date"])
                   .reset_index(drop=True))
    summary_df  = (pd.DataFrame(summary_rows)
                   .sort_values(["segment", "cluster"])
                   .reset_index(drop=True))

    forecast_df.to_csv(os.path.join(OUTPUT_DIR, "arima_forecast_2026.csv"), index=False)
    summary_df.to_csv( os.path.join(OUTPUT_DIR, "arima_summary.csv"),       index=False)
    with open(os.path.join(OUTPUT_DIR, "arima_models.pkl"), "wb") as f:
        pickle.dump(model_store, f)

    # -- Résumé final ----------------------------------------------------------
    print("\n" + "=" * 65)
    print("  RÉSUMÉ FINAL — PRÉVISIONS 12 MOIS PAR CLUSTER")
    print("=" * 65)
    for _, row in summary_df.iterrows():
        chg_str = f"{row['forecast_change_pct']:+.1f}%" if pd.notna(row["forecast_change_pct"]) else "N/A"
        mape_str = f"{row['mape_pct']:.1f}%" if pd.notna(row["mape_pct"]) else "N/A"
        print(f"\n  {row['segment']} · C{row['cluster']} · {row['cluster_name']}")
        print(
            f"  {row['last_known_price']:.0f} -> {row['forecast_mean_12m']:.0f} TND "
            f"({chg_str}) | MAPE={mape_str} | {row['quality']}"
        )
        print(f"  reco={row['recommendation_usage']} | fallback={row['fallback_segment_applied']}")
        print(
            f"  momentum={row['momentum_prix']:.2f} | var_ann={row['variation_annuelle']:+.1f}% | "
            f"ruptures_TCN={row['n_ruptures_tcn']}"
        )

    print(f"\n[OK] Prévisions : {os.path.join(OUTPUT_DIR, 'arima_forecast_2026.csv')}")
    print(f"[OK] Résumé     : {os.path.join(OUTPUT_DIR, 'arima_summary.csv')}")
    print(f"[OK] Modèles    : {os.path.join(OUTPUT_DIR, 'arima_models.pkl')}")


if __name__ == "__main__":
    run_pipeline()