"""
tcn_ruptures.py - Detection de ruptures par TCN sur les clusters de segmentation
=================================================================================
Utilise les sorties de segmentation.py pour detecter les ruptures par segment
et par cluster, avec un focus prioritaire sur les clusters emergents.

FIXES appliqués :
  - train_tcn() : epochs=300 -> epochs=80 (était ignoré par l'appelant)
  - consensus_threshold : max(1, ...) -> max(2, ...) (évite les faux positifs sur 3 govs.)
"""

import os
import pickle
from collections import Counter, defaultdict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(BASE_DIR, 'models_v2', 'tcn_ruptures')
os.makedirs(MODELS_DIR, exist_ok=True)

CLUSTER_SUMMARY_PATH = os.path.join(BASE_DIR, 'models_v2', 'cluster_summary_by_gouvernorat_real.csv')
RAW_BO3_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', '..', 'BO3'))
RAW_BO3_FILES = {
    'Residentiel': 'residentiel_BO3.xlsx',
    'Foncier': 'foncier_BO3.xlsx',
    'Commercial': 'commercial_BO3.xlsx',
}

FOCUS_EMERGING_ONLY = False
MIN_SERIES_MONTHS = 24
MIN_GAP_BETWEEN_RUPTURES = 2
EMERGING_KEYWORDS = (
    'emergent',
    'emergente',
    'fort potentiel',
    'sous-exploite',
    'sous exploite',
)

# Mapping des gouvernorats
GOV_NAMES = {
    1: 'Ariana', 2: 'Béja', 3: 'Ben Arous', 4: 'Bizerte', 5: 'Gabès', 6: 'Gafsa',
    7: 'Jendouba', 8: 'Kairouan', 9: 'Kasserine', 10: 'Kébili', 11: 'Le Kef', 12: 'Mahdia',
    13: 'Manouba', 14: 'Médenine', 15: 'Monastir', 16: 'Nabeul', 17: 'Sfax', 18: 'Sidi Bouzid',
    19: 'Siliana', 20: 'Sousse', 21: 'Tataouine', 22: 'Tozeur', 23: 'Tunis', 24: 'Zaghouan'
}

CLUSTER_NAMES = {
    'Residentiel': {},
    'Foncier': {},
    'Commercial': {},
}


class CausalConv1d(nn.Module):
    """Convolution causale pour TCN."""
    def __init__(self, in_ch, out_ch, kernel_size, dilation):
        super().__init__()
        self.padding = (kernel_size - 1) * dilation
        self.conv = nn.Conv1d(in_ch, out_ch, kernel_size, dilation=dilation)
        self.bn = nn.BatchNorm1d(out_ch)

    def forward(self, x):
        x = F.pad(x, (self.padding, 0))
        return F.relu(self.bn(self.conv(x)))


class TCN(nn.Module):
    """Temporal Convolutional Network."""
    def __init__(self, input_dim=1, hidden_dim=32, num_layers=4, kernel_size=3):
        super().__init__()
        layers = []
        in_dim = input_dim
        for i in range(num_layers):
            dilation = 2 ** i
            layers.append(CausalConv1d(in_dim, hidden_dim, kernel_size, dilation))
            in_dim = hidden_dim
        self.tcn = nn.Sequential(*layers)
        self.output = nn.Conv1d(hidden_dim, input_dim, 1)

    def forward(self, x):
        return self.output(self.tcn(x))


def normalize_segmented_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise les colonnes issues de segmentation.py."""
    df = df.copy()

    if 'gouvernorat' not in df.columns and 'xgouvernorat' in df.columns:
        df = df.rename(columns={'xgouvernorat': 'gouvernorat'})

    if 'segment' not in df.columns:
        raise KeyError("Colonne 'segment' introuvable dans le fichier segmenté")
    if 'cluster' not in df.columns:
        raise KeyError("Colonne 'cluster' introuvable dans le fichier segmenté")

    if 'cluster_name' not in df.columns:
        df['cluster_name'] = df['cluster'].map(lambda x: f'Cluster {x}')

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    elif {'annee', 'mois'}.issubset(df.columns):
        df['date'] = pd.to_datetime(
            df['annee'].astype(int).astype(str) + '-' +
            df['mois'].astype(int).astype(str) + '-01'
        )
    else:
        raise KeyError("Colonne 'date' ou 'annee/mois' non trouvée")

    if 'source' in df.columns:
        df['source'] = df['source'].astype(str)

    if 'indice_prix_m2_regional' not in df.columns:
        raise KeyError("Colonne 'indice_prix_m2_regional' introuvable")

    df = df.sort_values(['segment', 'cluster', 'date']).reset_index(drop=True)
    return df


def load_cluster_summary() -> pd.DataFrame:
    if not os.path.exists(CLUSTER_SUMMARY_PATH):
        raise FileNotFoundError(f"Fichier de resume cluster introuvable: {CLUSTER_SUMMARY_PATH}")

    summary = pd.read_csv(CLUSTER_SUMMARY_PATH)
    if 'gouvernorat' not in summary.columns and 'xgouvernorat' in summary.columns:
        summary = summary.rename(columns={'xgouvernorat': 'gouvernorat'})

    required = {'segment', 'gouvernorat', 'cluster', 'cluster_name'}
    missing = required.difference(summary.columns)
    if missing:
        raise KeyError(f"Colonnes manquantes dans le resume cluster: {sorted(missing)}")

    summary['gouvernorat'] = pd.to_numeric(summary['gouvernorat'], errors='coerce').astype('Int64')
    summary['cluster'] = pd.to_numeric(summary['cluster'], errors='coerce').astype('Int64')
    return summary[list(required)].dropna(subset=['gouvernorat', 'cluster']).copy()


def is_emerging_cluster_name(cluster_name: str) -> bool:
    name = str(cluster_name).casefold()
    return any(keyword in name for keyword in EMERGING_KEYWORDS)


def load_cluster_data() -> pd.DataFrame:
    summary = load_cluster_summary()
    frames = []

    for segment, filename in RAW_BO3_FILES.items():
        raw_path = os.path.join(RAW_BO3_DIR, filename)
        if not os.path.exists(raw_path):
            print(f"[!] Fichier brut introuvable: {raw_path}")
            continue

        raw_df = pd.read_excel(raw_path)
        if 'xgouvernorat' in raw_df.columns and 'gouvernorat' not in raw_df.columns:
            raw_df = raw_df.rename(columns={'xgouvernorat': 'gouvernorat'})
        if 'gouvernorat' not in raw_df.columns:
            raise KeyError(f"Colonne gouvernorat introuvable dans {raw_path}")

        raw_df['gouvernorat'] = pd.to_numeric(raw_df['gouvernorat'], errors='coerce').astype('Int64')
        raw_df['segment'] = segment

        seg_map = summary[summary['segment'] == segment].copy()
        seg_map['gouvernorat'] = pd.to_numeric(seg_map['gouvernorat'], errors='coerce').astype('Int64')
        seg_map['cluster'] = pd.to_numeric(seg_map['cluster'], errors='coerce').astype('Int64')

        raw_df = raw_df.merge(
            seg_map[['gouvernorat', 'cluster', 'cluster_name']],
            on='gouvernorat',
            how='left',
        )
        frames.append(raw_df)
        print(f"[OK] {segment}: {len(raw_df):,} lignes chargees")

    if not frames:
        raise FileNotFoundError("Aucune donnée brute BO3 n'a pu être chargée")

    df = pd.concat(frames, ignore_index=True)

    if 'sample_weight_temporal' in df.columns:
        real_mask = df['sample_weight_temporal'] > 0.12
        before = len(df)
        df = df[real_mask].copy()
        print(f"   Filtrage real observations: {before:,} -> {len(df):,} lignes")

    df = normalize_segmented_frame(df)
    print(f"   {len(df):,} lignes | {df['date'].min().date()} -> {df['date'].max().date()}")
    print(f"   Segments: {', '.join(sorted(df['segment'].dropna().unique()))}")
    return df


def build_cluster_series(df: pd.DataFrame, segment: str, cluster: int) -> pd.Series | None:
    cluster_df = df[(df['segment'] == segment) & (df['cluster'] == cluster)].copy()
    if cluster_df.empty:
        return None
    monthly = cluster_df.groupby('date')['indice_prix_m2_regional'].mean().sort_index()
    if len(monthly) < 2:
        return monthly
    full_index = pd.date_range(monthly.index.min(), monthly.index.max(), freq='MS')
    monthly = monthly.reindex(full_index).interpolate(method='linear').ffill().bfill()
    monthly.index.name = 'date'
    return monthly


def build_gouvernorat_series(df: pd.DataFrame, segment: str, gouvernorat: int) -> pd.Series | None:
    gov_df = df[(df['segment'] == segment) & (df['gouvernorat'] == gouvernorat)].copy()
    if gov_df.empty:
        return None
    monthly = gov_df.groupby('date')['indice_prix_m2_regional'].mean().sort_index()
    if len(monthly) < 2:
        return monthly
    full_index = pd.date_range(monthly.index.min(), monthly.index.max(), freq='MS')
    monthly = monthly.reindex(full_index).interpolate(method='linear').ffill().bfill()
    monthly.index.name = 'date'
    return monthly


def prepare_series_for_tcn(ts: pd.Series, lower: float | None = None, upper: float | None = None) -> tuple[np.ndarray, float, float]:
    values = ts.values.astype(np.float32)
    if len(values) == 0:
        raise ValueError("Serie vide pour TCN")

    if lower is None or upper is None:
        lower = float(np.percentile(values, 5))
        upper = float(np.percentile(values, 95))
        if not np.isfinite(lower) or not np.isfinite(upper) or upper <= lower:
            lower = float(np.min(values))
            upper = float(np.max(values))

    clipped = np.clip(values, lower, upper)
    scale = upper - lower
    if scale <= 1e-8:
        normalized = np.zeros_like(clipped, dtype=np.float32)
    else:
        normalized = (clipped - lower) / (scale + 1e-8)
    return normalized.astype(np.float32), lower, upper


def get_cluster_label(cluster_df: pd.DataFrame, cluster: int) -> str:
    if 'cluster_name' in cluster_df.columns:
        names = cluster_df['cluster_name'].dropna().astype(str)
        if not names.empty:
            return names.mode().iat[0]
    return CLUSTER_NAMES.get(str(cluster_df['segment'].iloc[0]), {}).get(cluster, f'Cluster {cluster}')


def get_adaptive_num_layers(n_points: int) -> int:
    """Select num_layers so receptive field <= 60% of series length."""
    kernel_size = 3
    for n_layers in [4, 3, 2]:
        dilation_sum = sum(2 ** i for i in range(n_layers))
        receptive_field = (kernel_size - 1) * dilation_sum + 1
        if receptive_field <= int(n_points * 0.6):
            return n_layers
    return 2


def train_tcn_on_series(ts: pd.Series, epochs: int = 80, hidden_dim: int = 32):
    """Entraîne un TCN sur les 80% premiers points de la série et retourne le modèle + params de normalisation."""
    values_norm, v_min, v_max = prepare_series_for_tcn(ts)

    if len(values_norm) < 4:
        raise ValueError("Série trop courte pour un TCN")

    train_len = max(4, int(len(values_norm) * 0.8))
    X_train = torch.tensor(values_norm[:train_len]).view(1, 1, -1)

    torch.manual_seed(42)
    num_layers = get_adaptive_num_layers(len(ts))
    model = TCN(input_dim=1, hidden_dim=hidden_dim, num_layers=num_layers)
    print(f"         TCN layers={num_layers} (receptive_field fits {len(ts)} pts)")
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.001, weight_decay=1e-4)
    criterion = nn.SmoothL1Loss(beta=0.05)

    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad()
        output = model(X_train)
        loss   = criterion(output, X_train)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()

        if (epoch + 1) % 20 == 0:
            print(f"      Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}")

    return model, v_min, v_max


def compute_errors_with_model(model, ts: pd.Series, v_min: float, v_max: float):
    """Calcule les erreurs de reconstruction d'une série avec un modèle déjà entraîné."""
    values_norm, _, _ = prepare_series_for_tcn(ts, lower=v_min, upper=v_max)
    X_full = torch.tensor(values_norm).view(1, 1, -1)

    model.eval()
    with torch.no_grad():
        reconstructed = model(X_full).squeeze().cpu().numpy()

    error = (values_norm - reconstructed) ** 2
    return error


def detect_ruptures_cross_val(
    model, ts: pd.Series, v_min: float, v_max: float,
    splits=(0.70, 0.75, 0.80),
    threshold_factor: float = 3,
    min_gap: int = MIN_GAP_BETWEEN_RUPTURES,
    stability_threshold: int = 2,
) -> list[str]:
    """
    Run detect_ruptures on the same model across multiple train-split views.
    A rupture date is considered stable if it appears in >= stability_threshold splits.
    Returns only the stable rupture dates.
    """
    date_counts = Counter()
    effective_stability_threshold = stability_threshold if len(ts) >= 60 else 1
    error = compute_errors_with_model(model, ts, v_min, v_max)
    for split in splits:
        train_len = max(4, int(len(ts) * split))
        masked_error = error.copy()
        masked_error[:train_len] = 0.0
        _, rupture_dates, _, _, _ = detect_ruptures(
            masked_error, ts, threshold_factor=threshold_factor, min_gap=min_gap
        )
        date_counts.update(set(rupture_dates))
    stable = sorted(d for d, c in date_counts.items() if c >= effective_stability_threshold)
    return stable


def compute_reconstruction_errors(ts: pd.Series, epochs: int = 80, hidden_dim: int = 32):
    """Backward-compatible wrapper: entraîne puis calcule les erreurs sur la série complète."""
    model, v_min, v_max = train_tcn_on_series(ts, epochs=epochs, hidden_dim=hidden_dim)
    error = compute_errors_with_model(model, ts, v_min, v_max)
    return model, error, (v_min, v_max)


# -- FIX CRITIQUE : epochs=80, pas 300 ----------------------------------------
def train_tcn(ts, epochs=80, hidden_dim=32):
    """Entraîne TCN et retourne erreur de reconstruction."""
    return compute_reconstruction_errors(ts, epochs=epochs, hidden_dim=hidden_dim)


def detect_ruptures(error, ts, threshold_factor=2.2, min_gap=MIN_GAP_BETWEEN_RUPTURES):
    """Détecte les ruptures par seuil statistique μ + threshold_factor × σ."""
    error = np.asarray(error, dtype=np.float32)
    smoothed_error = (
        pd.Series(error)
        .rolling(window=3, center=True, min_periods=1)
        .median()
        .to_numpy()
    )
    median_err = float(np.median(smoothed_error))
    mad = float(np.median(np.abs(smoothed_error - median_err)))
    threshold = median_err + threshold_factor * 1.4826 * (mad + 1e-8)
    mean_err = float(np.mean(smoothed_error))
    std_err  = float(np.std(smoothed_error))

    rupture_indices = np.where(smoothed_error > threshold)[0]

    if len(rupture_indices) > 0:
        filtered = [rupture_indices[0]]
        for i in range(1, len(rupture_indices)):
            if rupture_indices[i] - rupture_indices[i - 1] > min_gap:
                filtered.append(rupture_indices[i])
        rupture_indices = filtered

    rupture_dates = [ts.index[i].strftime('%Y-%m') for i in rupture_indices if i < len(ts)]
    return rupture_indices, rupture_dates, threshold, mean_err, std_err


def plot_ruptures(ts, error, rupture_indices, threshold, segment, cluster, cluster_name):
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))

    ax1 = axes[0]
    ax1.plot(ts.index, ts.values, color='steelblue', linewidth=2, label='Prix moyen')
    for idx in rupture_indices:
        if idx < len(ts):
            ax1.axvline(ts.index[idx], color='red', linestyle='--', linewidth=1.5, alpha=0.7)
            ax1.text(ts.index[idx], ts.values[idx], f'R{idx+1}', fontsize=8, color='red')
    ax1.set_title(f'{segment} - Cluster {cluster}: {cluster_name}\n{len(rupture_indices)} ruptures détectées')
    ax1.set_ylabel('Prix moyen (TND/m²)')
    ax1.legend()
    ax1.grid(alpha=0.3)

    ax2 = axes[1]
    ax2.plot(ts.index, error, color='darkorange', linewidth=1.5, label='Erreur de reconstruction')
    ax2.axhline(threshold, color='red', linestyle='--', alpha=0.7, label='Seuil (μ+3σ)')
    ax2.set_ylabel('Erreur de reconstruction')
    ax2.legend()
    ax2.grid(alpha=0.3)
    for idx in rupture_indices:
        if idx < len(ts):
            ax2.axvline(ts.index[idx], color='red', linestyle='--', linewidth=1, alpha=0.5)

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(MODELS_DIR, f'ruptures_{segment}_C{cluster}.png'), dpi=120)
    plt.close(fig)


def iter_target_clusters(df: pd.DataFrame, focus_emerging_only: bool = True):
    for (segment, cluster), cluster_df in df.groupby(['segment', 'cluster']):
        cluster_name = get_cluster_label(cluster_df, int(cluster))
        emerging     = is_emerging_cluster_name(cluster_name)
        if focus_emerging_only and not emerging:
            continue
        yield segment, int(cluster), cluster_name, cluster_df, emerging


def summarize_cluster_series(ts: pd.Series) -> str:
    return f"{len(ts)} points mensuels | {ts.min():.0f} -> {ts.max():.0f} TND (moy: {ts.mean():.0f})"


def infer_change_direction(ts: pd.Series, idx: int, window: int = 3) -> str:
    """Infer direction around a rupture index from local means before/after."""
    if len(ts) < 2 or idx <= 0 or idx >= len(ts):
        return 'stable'

    start_before = max(0, idx - window)
    before = ts.iloc[start_before:idx]
    end_after = min(len(ts), idx + window)
    after = ts.iloc[idx:end_after]

    if before.empty or after.empty:
        return 'stable'

    delta = float(after.mean() - before.mean())
    eps = max(1.0, float(before.mean()) * 0.005)
    if delta > eps:
        return 'hausse'
    if delta < -eps:
        return 'baisse'
    return 'stable'


def infer_directions_for_ruptures(ts: pd.Series, rupture_indices: list[int], rupture_dates: list[str]) -> dict[str, str]:
    """Return {YYYY-MM: direction} for detected rupture points."""
    directions = {}
    for idx, date in zip(rupture_indices, rupture_dates):
        directions[date] = infer_change_direction(ts, idx)
    return directions


def run_tcn_detection(threshold_factor=2.2):
    print("\n" + "="*70)
    print("  DETECTION DE RUPTURES PAR TCN (segments + clusters)")
    print("="*70)

    df      = load_cluster_data()
    results = {}

    target_count = 0
    for segment, cluster, cluster_name, cluster_df, emerging in iter_target_clusters(
        df, focus_emerging_only=FOCUS_EMERGING_ONLY
    ):
        if segment not in results:
            results[segment] = {}

        print(f"\n{'='*60}")
        focus_tag = " [EMERGING]" if emerging else ""
        print(f"  {segment} - cluster {cluster}: {cluster_name}{focus_tag}")
        print('='*60)

        cluster_ts = build_cluster_series(df, segment, cluster)
        if cluster_ts is None or len(cluster_ts) < 12:
            print(f"    [!] Serie trop courte ({len(cluster_ts) if cluster_ts is not None else 0} points) -> skip")
            results[segment][cluster] = {
                'cluster_name':           cluster_name,
                'is_emerging':            emerging,
                'skipped':                True,
                'skip_reason':            f"Serie trop courte ({len(cluster_ts) if cluster_ts is not None else 0} pts)",
                'rupture_dates':          [],
                'soft_rupture_dates':     [],
                'soft_rupture_weights':   {},
                'rupture_count_by_date':  {},
                'gouvernorat_results':    {},
                'consensus_threshold':    None,
                'n_gouvernorats':         0,
                'cluster_series_points':  len(cluster_ts) if cluster_ts is not None else 0,
            }
            continue

        print(f"    {summarize_cluster_series(cluster_ts)}")

        cluster_model = None
        cluster_v_min = None
        cluster_v_max = None
        if len(cluster_ts) >= MIN_SERIES_MONTHS:
            cluster_model, cluster_v_min, cluster_v_max = train_tcn_on_series(cluster_ts)

        gouvernorat_results   = {}
        rupture_weight_by_date = defaultdict(float)
        direction_weight_by_date = defaultdict(lambda: {'hausse': 0.0, 'baisse': 0.0, 'stable': 0.0})
        gouvernorats          = sorted(cluster_df['gouvernorat'].dropna().unique())

        for gouvernorat in gouvernorats:
            gov_name = GOV_NAMES.get(int(gouvernorat), str(gouvernorat))
            gov_ts   = build_gouvernorat_series(df, segment, int(gouvernorat))

            if gov_ts is None or len(gov_ts) < MIN_SERIES_MONTHS:
                n = len(gov_ts) if gov_ts is not None else 0
                print(f"    [!] {gov_name}: serie trop courte ({n} points) -> skip")
                continue

            if cluster_model is None:
                print(f"    [!] {gov_name}: modele cluster indisponible (cluster trop court) -> skip")
                continue

            print(f"    -> Gouvernorat {gov_name}: {len(gov_ts)} points")

            error = compute_errors_with_model(cluster_model, gov_ts, cluster_v_min, cluster_v_max)
            rupture_indices_raw, rupture_dates_raw, threshold, mean_err, std_err = detect_ruptures(
                error, gov_ts, threshold_factor=threshold_factor
            )

            stable_rupture_dates = detect_ruptures_cross_val(
                cluster_model,
                gov_ts,
                cluster_v_min,
                cluster_v_max,
                threshold_factor=threshold_factor,
                min_gap=MIN_GAP_BETWEEN_RUPTURES,
                stability_threshold=2,
            )

            filtered_count = max(0, len(rupture_dates_raw) - len(stable_rupture_dates))
            print(f"       Cross-val: {len(rupture_dates_raw)} -> {len(stable_rupture_dates)} ruptures stables (filtrees={filtered_count})")

            if stable_rupture_dates:
                stable_set = set(stable_rupture_dates)
                rupture_indices = [idx for idx in rupture_indices_raw if gov_ts.index[idx].strftime('%Y-%m') in stable_set]
            else:
                rupture_indices = []
            rupture_dates = stable_rupture_dates
            rupture_directions = infer_directions_for_ruptures(gov_ts, rupture_indices, rupture_dates)

            weight = min(1.0, len(gov_ts) / 48.0)
            for d in set(rupture_dates):
                rupture_weight_by_date[d] += weight
            for d, direction in rupture_directions.items():
                direction_weight_by_date[d][direction] += weight

            gouvernorat_results[int(gouvernorat)] = {
                'gouvernorat_name': gov_name,
                'ts_index':         gov_ts.index.tolist(),
                'ts_values':        gov_ts.values.tolist(),
                'error':            error.tolist(),
                'rupture_indices':  rupture_indices,
                'rupture_dates':    rupture_dates,
                'rupture_directions': rupture_directions,
                'threshold':        threshold,
                'mean_error':       mean_err,
                'std_error':        std_err,
                'n_points':         len(gov_ts),
            }

            print(f"       [OK] {len(rupture_dates)} ruptures detectees: {rupture_dates}")
            if rupture_directions:
                print(f"       Direction(s): {rupture_directions}")
            print(f"       Erreur: mu={mean_err:.4f}, sigma={std_err:.4f}, seuil={threshold:.4f}")

        if not gouvernorat_results:
            if len(cluster_ts) >= 12:
                print(f"    [FALLBACK] Cluster series used directly ({len(cluster_ts)} pts) — individual gov. series too short")
                if cluster_model is None:
                    cluster_model, cluster_v_min, cluster_v_max = train_tcn_on_series(cluster_ts)
                cluster_error = compute_errors_with_model(cluster_model, cluster_ts, cluster_v_min, cluster_v_max)
                cluster_rupture_dates = detect_ruptures_cross_val(
                    cluster_model,
                    cluster_ts,
                    cluster_v_min,
                    cluster_v_max,
                    threshold_factor=threshold_factor,
                    min_gap=MIN_GAP_BETWEEN_RUPTURES,
                    stability_threshold=2,
                )
                cluster_indices_raw, _, cluster_threshold, cluster_mean_err, cluster_std_err = detect_ruptures(
                    cluster_error, cluster_ts, threshold_factor=threshold_factor
                )
                cluster_set = set(cluster_rupture_dates)
                cluster_indices = [idx for idx in cluster_indices_raw if cluster_ts.index[idx].strftime('%Y-%m') in cluster_set]
                cluster_rupture_directions = infer_directions_for_ruptures(cluster_ts, cluster_indices, cluster_rupture_dates)
                print(f"       [OK] {len(cluster_rupture_dates)} ruptures detectees (cluster_level): {cluster_rupture_dates}")
                if cluster_rupture_directions:
                    print(f"       Direction(s): {cluster_rupture_directions}")
                print(f"       Erreur: mu={cluster_mean_err:.4f}, sigma={cluster_std_err:.4f}, seuil={cluster_threshold:.4f}")

                results[segment][cluster] = {
                    'cluster_name':          cluster_name,
                    'is_emerging':           emerging,
                    'gouvernorat_results':   {
                        'cluster_level': {
                            'gouvernorat_name': 'cluster_level',
                            'ts_index':         cluster_ts.index.tolist(),
                            'ts_values':        cluster_ts.values.tolist(),
                            'error':            cluster_error.tolist(),
                            'rupture_indices':  cluster_indices,
                            'rupture_dates':    cluster_rupture_dates,
                            'rupture_directions': cluster_rupture_directions,
                            'threshold':        cluster_threshold,
                            'mean_error':       cluster_mean_err,
                            'std_error':        cluster_std_err,
                            'n_points':         len(cluster_ts),
                        }
                    },
                    'rupture_count_by_date': {d: 1.0 for d in cluster_rupture_dates},
                    'rupture_dates':         cluster_rupture_dates,
                    'rupture_direction_by_date': cluster_rupture_directions,
                    'soft_rupture_dates':    [],
                    'soft_rupture_weights':  {},
                    'consensus_threshold':   1.0,
                    'n_gouvernorats':        1,
                    'cluster_series_points': len(cluster_ts),
                    'is_cluster_level_only': True,
                    'skipped':               False,
                    'skip_reason':           '',
                }
                target_count += 1
            else:
                print("    [!] Aucun gouvernorat valide pour ce cluster")
            continue

        n_govs = len(gouvernorat_results)
        gov_weights = {
            g: min(1.0, gouvernorat_results[g]['n_points'] / 48.0)
            for g in gouvernorat_results
        }
        total_weight = sum(gov_weights.values())
        sorted_weights = sorted(gov_weights.values(), reverse=True)
        top2_weight = sum(sorted_weights[:2]) if len(sorted_weights) >= 2 else sorted_weights[0]
        n_valid = len(gouvernorat_results)
        if n_valid == 1:
            consensus_weight_threshold = sorted_weights[0] * 0.60
        elif n_valid == 2:
            consensus_weight_threshold = sorted_weights[0] * 0.80
        else:
            consensus_weight_threshold = max(
                top2_weight * 0.80,
                total_weight * 0.25,
            )
        consensus_dates = sorted(
            d for d, w in rupture_weight_by_date.items()
            if w >= consensus_weight_threshold
        )

        soft_rupture_threshold = consensus_weight_threshold * 0.70
        soft_rupture_dates = sorted(
            d for d, w in rupture_weight_by_date.items()
            if soft_rupture_threshold <= w < consensus_weight_threshold
        )
        soft_rupture_weights = {
            d: round(w / consensus_weight_threshold, 4)
            for d, w in rupture_weight_by_date.items()
            if soft_rupture_threshold <= w < consensus_weight_threshold
        }
        rupture_direction_by_date = {}
        for d in consensus_dates:
            score = direction_weight_by_date.get(d, {'hausse': 0.0, 'baisse': 0.0, 'stable': 0.0})
            rupture_direction_by_date[d] = max(score.items(), key=lambda x: x[1])[0]
        soft_rupture_direction_by_date = {}
        for d in soft_rupture_dates:
            score = direction_weight_by_date.get(d, {'hausse': 0.0, 'baisse': 0.0, 'stable': 0.0})
            soft_rupture_direction_by_date[d] = max(score.items(), key=lambda x: x[1])[0]

        print(f"    Consensus pondéré (seuil={consensus_weight_threshold:.2f}/{total_weight:.2f}): {consensus_dates}")
        if rupture_direction_by_date:
            print(f"    Direction des change points: {rupture_direction_by_date}")
        print(f"    Poids par date: { {d: round(w,4) for d, w in sorted(rupture_weight_by_date.items())} }")
        if soft_rupture_dates:
            print(f"    Soft ruptures (70-99% seuil): {soft_rupture_dates}")
            print(f"    Confiance normalisée: {soft_rupture_weights}")

        results[segment][cluster] = {
            'cluster_name':          cluster_name,
            'is_emerging':           emerging,
            'gouvernorat_results':   gouvernorat_results,
            'rupture_count_by_date': {d: round(w, 4) for d, w in sorted(rupture_weight_by_date.items())},
            'rupture_dates':         consensus_dates,
            'rupture_direction_by_date': rupture_direction_by_date,
            'soft_rupture_dates':    soft_rupture_dates,
            'soft_rupture_direction_by_date': soft_rupture_direction_by_date,
            'soft_rupture_weights':  soft_rupture_weights,
            'consensus_threshold':   consensus_weight_threshold,
            'n_gouvernorats':        n_govs,
            'cluster_series_points': len(cluster_ts),
            'is_cluster_level_only': False,
            'skipped':               False,
            'skip_reason':           '',
        }
        target_count += 1

    print(f"\n   Clusters analyses: {target_count}")
    return results, df


def save_results(results):
    results_pkl  = {}
    summary_rows = []

    for segment, clusters in results.items():
        results_pkl[segment] = {}
        for cluster, data in clusters.items():
            results_pkl[segment][cluster] = {
                'cluster_name':          data['cluster_name'],
                'is_emerging':           data.get('is_emerging', False),
                'rupture_dates':         data['rupture_dates'],
                'rupture_direction_by_date': data.get('rupture_direction_by_date', {}),
                'soft_rupture_dates':    data.get('soft_rupture_dates', []),
                'soft_rupture_direction_by_date': data.get('soft_rupture_direction_by_date', {}),
                'soft_rupture_weights':  data.get('soft_rupture_weights', {}),
                'rupture_count_by_date': data.get('rupture_count_by_date', {}),
                'consensus_threshold':   data.get('consensus_threshold', None),
                'n_gouvernorats':        data.get('n_gouvernorats', 0),
                'gouvernorat_results':   data.get('gouvernorat_results', {}),
            }
            summary_rows.append({
                'segment':                  segment,
                'cluster':                  cluster,
                'cluster_name':             data['cluster_name'],
                'is_emerging':              data.get('is_emerging', False),
                'n_gouvernorats':           data.get('n_gouvernorats', 0),
                'n_points':                 data.get('cluster_series_points', 0),
                'rupture_count':            len(data['rupture_dates']),
                'rupture_dates_consensus':  ' | '.join(data['rupture_dates']),
                'rupture_directions_consensus': str(data.get('rupture_direction_by_date', {})),
                'soft_rupture_dates':       ' | '.join(data.get('soft_rupture_dates', [])),
                'soft_rupture_directions':  str(data.get('soft_rupture_direction_by_date', {})),
                'soft_rupture_count':       len(data.get('soft_rupture_dates', [])),
                'rupture_count_by_date':    str(data.get('rupture_count_by_date', {})),
                'consensus_threshold':      data.get('consensus_threshold', np.nan),
                'skipped':                  data.get('skipped', False),
                'skip_reason':              data.get('skip_reason', ''),
            })

    with open(os.path.join(MODELS_DIR, 'tcn_ruptures.pkl'), 'wb') as f:
        pickle.dump(results_pkl, f)
    print(f"\n[OK] Ruptures sauvegardées: {MODELS_DIR}/tcn_ruptures.pkl")

    if summary_rows:
        summary_df   = pd.DataFrame(summary_rows)
        summary_path = os.path.join(MODELS_DIR, 'tcn_ruptures_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        print(f"[OK] Résumé CSV: {summary_path}")


def create_dataset_with_ruptures(results, df):
    df_out = df.copy()
    df_out['rupture']         = 0
    df_out['rupture_date']    = None
    df_out['rupture_count']   = 0
    df_out['rupture_cluster'] = None
    df_out['rupture_segment'] = None
    df_out['rupture_direction'] = None
    df_out['soft_rupture'] = 0.0
    df_out['soft_rupture_date'] = None
    df_out['soft_rupture_direction'] = None

    for segment, clusters in results.items():
        for cluster, data in clusters.items():
            mask          = (df_out['segment'] == segment) & (df_out['cluster'] == cluster)
            rupture_dates = data['rupture_dates']
            rupture_directions = data.get('rupture_direction_by_date', {})

            for i, rupture_date in enumerate(rupture_dates, 1):
                rupture_dt   = pd.to_datetime(rupture_date)
                rupture_mask = mask & (df_out['date'] >= rupture_dt)
                df_out.loc[rupture_mask, 'rupture']         = 1
                df_out.loc[rupture_mask & df_out['rupture_date'].isna(), 'rupture_date'] = rupture_date
                df_out.loc[rupture_mask, 'rupture_count']   = i
                df_out.loc[rupture_mask, 'rupture_cluster'] = cluster
                df_out.loc[rupture_mask, 'rupture_segment'] = segment
                direction = rupture_directions.get(rupture_date, 'stable')
                df_out.loc[rupture_mask, 'rupture_direction'] = direction

            soft_dates = data.get('soft_rupture_dates', [])
            soft_weights = data.get('soft_rupture_weights', {})
            soft_directions = data.get('soft_rupture_direction_by_date', {})

            for soft_date in soft_dates:
                soft_dt = pd.to_datetime(soft_date)
                soft_mask = mask & (df_out['date'] >= soft_dt)
                confidence = soft_weights.get(soft_date, 0.7)
                df_out.loc[soft_mask & (df_out['soft_rupture'] == 0.0), 'soft_rupture'] = confidence
                df_out.loc[soft_mask & df_out['soft_rupture_date'].isna(), 'soft_rupture_date'] = soft_date
                soft_direction = soft_directions.get(soft_date, 'stable')
                df_out.loc[soft_mask & df_out['soft_rupture_direction'].isna(), 'soft_rupture_direction'] = soft_direction

    out_path = os.path.join(MODELS_DIR, 'BO3_with_ruptures.csv')
    df_out.to_csv(out_path, index=False)
    print(f"[OK] Dataset avec ruptures: {out_path}")
    return df_out


def print_summary(results):
    print("\n" + "="*70)
    print("  RESUME DES RUPTURES PAR SEGMENT / CLUSTER")
    print("="*70)

    total = 0
    for segment, clusters in results.items():
        print(f"\n{segment}")
        for cluster, data in clusters.items():
            if data.get('skipped', False):
                continue
            ruptures = data['rupture_dates']
            directions = data.get('rupture_direction_by_date', {})
            total   += len(ruptures)
            tag      = " [EMERGING]" if data.get('is_emerging', False) else ""
            th       = data.get('consensus_threshold', '?')
            n        = data.get('n_gouvernorats', '?')
            print(f"   C{cluster} - {data['cluster_name']}{tag}: "
                  f"consensus={len(ruptures)} (seuil {th}/{n}) -> {ruptures}")
            if directions:
                print(f"      direction: {directions}")
            print(f"      par gouvernorat: {data.get('rupture_count_by_date', {})}")

    print(f"\nTotal: {total} ruptures detectees")

    all_ruptures = [r for seg in results.values() for c in seg.values() for r in c['rupture_dates']]
    if all_ruptures:
        print("\nFrequence des ruptures:")
        for date, count in sorted(Counter(all_ruptures).items()):
            print(f"   {date}: {count} cluster(s)")

    emerging = [
        (seg, cl, data)
        for seg, clusters in results.items()
        for cl, data in clusters.items()
        if data.get('is_emerging', False)
    ]
    if emerging:
        print("\nZones emergentes prioritaires:")
        for seg, cl, data in emerging:
            print(f"   {seg} - C{cl} - {data['cluster_name']} ({len(data['rupture_dates'])} ruptures)")


if __name__ == '__main__':
    results, df = run_tcn_detection(threshold_factor=2.2)

    if results:
        save_results(results)
        df_with_ruptures = create_dataset_with_ruptures(results, df)
        print_summary(results)

    print("\n" + "="*70)
    print("OK Detection de ruptures par TCN terminee !")
    print("="*70)