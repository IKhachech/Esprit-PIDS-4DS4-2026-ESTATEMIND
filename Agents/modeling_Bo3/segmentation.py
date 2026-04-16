"""
autoencoder_tunisia_v2.py — Segmentation géographique v3 pour la Tunisie
================================================================================
Objectif métier : Prédire les tendances régionales du marché immobilier.

Améliorations vs v1 :
  1. Features enrichies : temporalité, dynamisme, macro-économique
  2. Feature engineering orienté TENDANCE (glissement, croissance, saisonnalité)
  3. Agrégation pondérée par sample_weight_temporal (récence)
  4. Autoencoder plus profond + BatchNorm + Dropout (régularisation)
  5. Optimisation k par silhouette + Davies-Bouldin + Calinski-Harabasz
  6. Stabilisation des clusters par multi-run KMeans (consensus)
  7. Interprétation métier enrichie par cluster
  8. Winsorizing des prix (neutralise outliers)
  9. Détection auto glissement national -> calcul régional depuis les prix
  10. Feature momentum_prix + assignation labels par score combiné prix+momentum
"""

import os
import pickle
import warnings
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.cluster import KMeans
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import (
    calinski_harabasz_score, 
    davies_bouldin_score, 
    silhouette_score,
    adjusted_rand_score
)

warnings.filterwarnings('ignore')

# --- Mapping -------------------------------------------------------------------
GOV_NAMES = {
    1: 'Ariana', 2: 'Béja', 3: 'Ben Arous', 4: 'Bizerte', 5: 'Gabès', 6: 'Gafsa',
    7: 'Jendouba', 8: 'Kairouan', 9: 'Kasserine', 10: 'Kébili', 11: 'Le Kef', 12: 'Mahdia',
    13: 'Manouba', 14: 'Médenine', 15: 'Monastir', 16: 'Nabeul', 17: 'Sfax', 18: 'Sidi Bouzid',
    19: 'Siliana', 20: 'Sousse', 21: 'Tataouine', 22: 'Tozeur', 23: 'Tunis', 24: 'Zaghouan'
}

# --- Features orientées TENDANCE par segment ----------------------------------
# Reordered with momentum_prix en première position (priorité maximale)
FEATURES_BY_SEGMENT = {
    'Residentiel': [
        'momentum_prix',            # PRIORITE 1: Acceleration prix (clé !!)
        'prix_m2_moyen',            # PRIORITE 2: Niveau de prix
        'prix_m2_recent',           # PRIORITE 2: Tendance récente
        'glissement_recent',        # PRIORITE 3: Tendance court terme
        'glissement_moyen',         # PRIORITE 3: Tendance moyen terme
        'score_attractivite',       # PRIORITE 3: Attractivité structurelle
        'croissance_pib_moy',       # PRIORITE 4: Macro-économique
        'nb_infra',                 # PRIORITE 4: Densité d'infrastructures
        'nb_commerce',              # PRIORITE 4: Activité commerciale
        'volume_transactions',      # PRIORITE 4: Dynamisme marché
        'nb_villes',                # PRIORITE 5: Couverture géographique
    ],
    'Foncier': [
        'momentum_prix',
        'prix_m2_moyen',
        'prix_m2_recent',
        'glissement_recent',
        'nb_infra',
        'score_attractivite',
        'croissance_pib_moy',
        'volume_transactions',
        'nb_villes',
    ],
    'Commercial': [
        'momentum_prix',            # CLES pour commercial
        'prix_m2_moyen',
        'prix_m2_recent',
        'glissement_recent',
        'nb_commerce',              # Plus pertinent que nb_infra
        'score_attractivite',
        'high_season_rate',
        'volume_transactions',
        'nb_villes',
    ],
}

PROJECTION_YEAR = 2026
WEIGHT_PROJECTION_THRESHOLD = 0.12
MIN_OBS = 5
K_RANGE = range(3, 5)   # Recommandation métier : tester k=3 et k=4 après nettoyage
MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models_v2')
os.makedirs(MODELS_DIR, exist_ok=True)

LABEL_THRESHOLDS = {
    'Residentiel': {
        'premium':   {'prix_min': 900,  'glissement_max': 4.0,  'label': 'Marche Premium - Faible Volatilite'},
        'dynamique': {'prix_min': 600,  'glissement_min': 4.0,  'label': 'Marche Dynamique - Croissance Active'},
        'emergent':  {'prix_max': 600,  'glissement_min': 6.0,  'label': 'Marche Emergent - Fort Potentiel'},
        'stable':    {'label': 'Marche Stable - Prix Contenus'},
    },
    'Foncier': {
        'strategique': {'prix_min': 950, 'label': 'Foncier Urbain - Forte Demande'},
        'developpement': {'glissement_min': 7.0, 'label': 'Foncier en Developpement - Hausse Active'},
        'accessible':  {'prix_max': 700, 'label': 'Foncier Accessible - Croissance Moderee'},
        'stable':      {'label': 'Foncier Rural - Stabilite'},
    },
    'Commercial': {
        'majeur':    {'prix_min': 800,  'label': 'Pole Commercial Majeur - Rendement Eleve'},
        'actif':     {'prix_min': 400,  'glissement_min': 5.0, 'label': 'Zone Commerciale Active - Croissance'},
        'emergent':  {'glissement_min': 10.0, 'label': 'Zone Emergente - Fort Potentiel'},
        'secondaire':{'label': 'Marche Commercial Secondaire'},
    },
}


def prepare_segment_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Déduplique et sépare les observations réelles des projections 2026."""
    original_rows = len(df)
    cleaned = df.drop_duplicates().copy().reset_index(drop=True)
    duplicate_rows = original_rows - len(cleaned)

    cleaned['is_projection'] = (
        (cleaned['sample_weight_temporal'] <= WEIGHT_PROJECTION_THRESHOLD)
        | (cleaned['annee'] > PROJECTION_YEAR)
    )
    real_df = cleaned[~cleaned['is_projection']].copy()
    projection_df = cleaned[cleaned['is_projection']].copy()

    stats = {
        'original_rows': original_rows,
        'duplicate_rows': duplicate_rows,
        'clean_rows': len(cleaned),
        'real_rows': len(real_df),
        'projection_rows': len(projection_df),
    }
    return cleaned, real_df, projection_df, stats


def encode_and_predict_clusters(model, scaler, features, gov_df: pd.DataFrame, best_km):
    """Projette un agrégat gouvernorat vers l'espace latent puis prédit le cluster."""
    if gov_df.empty:
        return gov_df.copy()

    out = gov_df.copy()
    X = scaler.transform(out[features].fillna(out[features].median()))
    X_t = torch.tensor(X, dtype=torch.float32)
    device = next(model.parameters()).device
    with torch.no_grad():
        encoded, _ = model(X_t.to(device))
    out['cluster'] = best_km.predict(encoded.cpu().numpy())
    return out


# --- 1. Feature Engineering + Selection optimale ---------------------------------
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrège par gouvernorat avec features temporelles enrichies.
    La pondération par sample_weight_temporal favorise les observations récentes.
    """

    def wmean(values, weights):
        """Moyenne pondérée robuste."""
        w = np.array(weights, dtype=float)
        v = np.array(values, dtype=float)
        mask = ~np.isnan(v) & ~np.isnan(w)
        if mask.sum() == 0:
            return np.nan
        return np.average(v[mask], weights=w[mask])

    # Seuil "récent" : 18 derniers mois
    max_annee = df['annee'].max()
    max_mois = df[df['annee'] == max_annee]['mois'].max()
    recent_mask = (
        (df['annee'] == max_annee) |
        ((df['annee'] == max_annee - 1) & (df['mois'] >= max_mois))
    )

    # -- Winsorizing par gouvernorat au 90e percentile
    df = df.copy()
    df['indice_prix_m2_regional'] = df.groupby('gouvernorat')['indice_prix_m2_regional'].transform(
        lambda x: x.clip(lower=x.quantile(0.05), upper=x.quantile(0.90)) if len(x) >= 5 else x.clip(lower=x.quantile(0.10), upper=x.quantile(0.90))
    )

    # -- Vérifier si glissement_immo_trim est national (même valeur par année)
    # Si oui, on calcule un glissement régional directement depuis les prix
    gliss_var = df.groupby(['annee', 'mois'])['glissement_immo_trim'].std().mean()
    use_computed_glissement = gliss_var < 0.01  # quasi-constant = national
    if use_computed_glissement:
        print("         [*] glissement_immo_trim detecté comme national -> calcul régional depuis les prix")
        # Calcul du glissement trimestriel moyen par gouvernorat depuis les prix
        df_sorted = df.sort_values(['gouvernorat', 'annee', 'mois'])
        df_sorted['prix_lag'] = df_sorted.groupby('gouvernorat')['indice_prix_m2_regional'].shift(3)
        df_sorted['glissement_calcule'] = (
            (df_sorted['indice_prix_m2_regional'] - df_sorted['prix_lag']) / df_sorted['prix_lag'] * 100
        ).clip(-25, 35)  # Caps realistes
        df = df.copy()
        df['glissement_immo_trim'] = df_sorted['glissement_calcule'].fillna(df['glissement_immo_trim'])

    records = []
    for gov, grp in df.groupby('gouvernorat'):
        recent = grp[recent_mask & (grp['gouvernorat'] == gov)] if gov in grp['gouvernorat'].values else grp.iloc[0:0]
        if len(recent) == 0:
            recent = grp.tail(max(1, len(grp) // 3))

        w = grp['sample_weight_temporal'].values
        w_r = recent['sample_weight_temporal'].values

        # Glissement moyen et récent (depuis prix si national, depuis colonne si régional)
        gliss_moy = wmean(grp['glissement_immo_trim'].dropna(), np.ones(grp['glissement_immo_trim'].dropna().shape[0]))
        gliss_rec = wmean(recent['glissement_immo_trim'].dropna(), np.ones(recent['glissement_immo_trim'].dropna().shape[0])) if len(recent) > 0 else gliss_moy

        rec = {
            'gouvernorat': gov,
            # Niveau prix global (pondéré, après winsorizing)
            'prix_m2_moyen': wmean(grp['indice_prix_m2_regional'], w),
            # Prix récents (tendance court terme)
            'prix_m2_recent': wmean(recent['indice_prix_m2_regional'], w_r) if len(recent) > 0 else wmean(grp['indice_prix_m2_regional'], w),
            # Glissement régional (calculé ou colonne selon détection)
            'glissement_moyen': gliss_moy if not np.isnan(gliss_moy) else 0.0,
            'glissement_recent': gliss_rec if not np.isnan(gliss_rec) else 0.0,
            # Ratio prix récent / prix moyen -> momentum prix
            'momentum_prix': float(np.clip(
                         (wmean(recent['indice_prix_m2_regional'], w_r) / wmean(grp['indice_prix_m2_regional'], w))
                         if wmean(grp['indice_prix_m2_regional'], w) > 0 else 1.0,
                         0.50, 2.00)),
            # Attractivité structurelle
            'score_attractivite': grp['score_attractivite'].mean(),
            'nb_infra': grp['nb_infra'].mean(),
            'nb_commerce': grp['nb_commerce'].mean(),
            # Macro-économique
            'croissance_pib_moy': wmean(grp['croissance_pib_trim'], w),
            'inflation_moy': wmean(grp['inflation_glissement_annuel'], w),
            # Saisonnalité
            'high_season_rate': grp['high_season'].mean(),
            # Dynamisme marché
            'volume_transactions': len(grp),
            'nb_villes': grp['ville_encoded'].nunique(),
        }
        records.append(rec)

    return pd.DataFrame(records)


# --- 2. Autoencoder optimisé ---------------------------------------------------
class ImprovedAutoencoder(nn.Module):
    """
    Autoencoder v2 optimisé :
    - Architecture adaptative basée sur input_dim (scale mieux avec petits datasets)
    - Bottleneck couche plus petit pour forcer la compression
    - Skip connections optionnelles pour éviter vanishing gradients
    - Dropout progressif
    """
    def __init__(self, input_dim, encoding_dim=6):
        super().__init__()
        
        # Dimensions couches : adaptées à la taille des données
        if input_dim <= 15:
            h1, h2 = max(16, input_dim * 2), max(8, input_dim)
        else:
            h1, h2 = max(32, int(input_dim * 1.5)), max(16, int(input_dim * 0.8))
        
        encoding_dim = max(2, min(encoding_dim, input_dim - 2))
        
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, h1),
            nn.BatchNorm1d(h1),
            nn.ReLU(),
            nn.Dropout(0.25),
            
            nn.Linear(h1, h2),
            nn.BatchNorm1d(h2),
            nn.ReLU(),
            nn.Dropout(0.2),
            
            nn.Linear(h2, encoding_dim),
            nn.ReLU()
        )
        
        self.decoder = nn.Sequential(
            nn.Linear(encoding_dim, h2),
            nn.ReLU(),
            nn.Dropout(0.2),
            
            nn.Linear(h2, h1),
            nn.ReLU(),
            nn.Dropout(0.2),
            
            nn.Linear(h1, input_dim),
            nn.Sigmoid()
        )

    def forward(self, x):
        enc = self.encoder(x)
        dec = self.decoder(enc)
        return enc, dec


def train_autoencoder(X: np.ndarray, epochs=1000, lr=1.5e-3, encoding_dim=6, patience=150):
    """
    Entraîne l'autoencoder v2 avec :
    - Early stopping + patience flexible (150 par défaut)
    - Learning rate adaptive et warmup initial
    - CosineAnnealingWarmRestarts pour meilleure convergence
    - Batch-level validation
    """
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    input_dim = X.shape[1]

    model = ImprovedAutoencoder(input_dim, encoding_dim=min(encoding_dim, input_dim)).to(device)
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=1e-3, amsgrad=True)
    
    # Learning rate scheduler : réduction progressive avec warmup restarts
    scheduler = optim.lr_scheduler.CosineAnnealingWarmRestarts(
        optimizer, 
        T_0=50, 
        T_mult=2, 
        eta_min=1e-5
    )
    criterion = nn.MSELoss(reduction='mean')

    X_t = torch.tensor(X, dtype=torch.float32).to(device)

    best_loss, best_encoded = float('inf'), None
    patience_counter = 0
    
    print(f"         Epochs={epochs}, LR={lr}, Encoding_dim={encoding_dim}, Input_dim={input_dim}")
    
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        enc, dec = model(X_t)
        loss = criterion(dec, X_t)
        
        loss.backward()
        nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        scheduler.step()

        if loss.item() < best_loss:
            best_loss = loss.item()
            patience_counter = 0
            model.eval()
            with torch.no_grad():
                best_encoded, _ = model(X_t)
            best_encoded = best_encoded.cpu().numpy()
        else:
            patience_counter += 1

        if (epoch + 1) % 100 == 0:
            print(f"      Epoch {epoch+1}/{epochs} | Loss: {loss.item():.4f} | Patience: {patience_counter}/{patience}")

        # Early stopping flexible
        if patience_counter >= patience:
            print(f"      [*] Early stopping at epoch {epoch+1} (patience {patience} atteint)")
            break

    return model, best_encoded


# --- 3. Selection de k optimale avec métriques robustes -------------------------
def find_best_k(encoded: np.ndarray, k_range=K_RANGE, n_runs=20):
    """
    Multi-critères robustes pour sélectionner k optimal :
    - Silhouette Score (maximiser)
    - Davies-Bouldin (minimiser)
    - Calinski-Harabasz (maximiser)
    - Stabilité (consensus entre runs)
    
    K-means++ pour initialisation optimale.
    """
    n = len(encoded)
    best_k, best_score, best_labels, best_km = 2, -1, None, None

    scores = {}
    print("         Evaluation multi-critères des k :")
    
    for k in k_range:
        if k >= n:
            continue
        
        # Multi-run avec k-means++ (meilleure initialisation)
        run_labels = []
        run_inertias = []
        run_estimators = []
        
        for seed in range(n_runs):
            km = KMeans(n_clusters=k, init='k-means++', random_state=seed, n_init=20, max_iter=300, tol=1e-4)
            labels = km.fit_predict(encoded)
            run_labels.append(labels)
            run_inertias.append(km.inertia_)
            run_estimators.append(km)

        # Sélectionner le run avec le meilleure inertie = stabilité
        best_run_idx = np.argmin(run_inertias)
        best_run_labels = run_labels[best_run_idx]
        best_run_km = run_estimators[best_run_idx]

        if len(set(best_run_labels)) < 2:
            continue

        # Métriques d'évaluation multi-critères
        sil = silhouette_score(encoded, best_run_labels)
        db = davies_bouldin_score(encoded, best_run_labels)
        ch = calinski_harabasz_score(encoded, best_run_labels)
        
        # Score composé : favorise silhouette haute, DB bas, CH haute
        # Normalisation des scores pour même amplitude
        ch_norm = min(ch / 100, 10)  # Cap CH pour éviter domination
        composite = (0.5 * sil) - (0.3 * db) + (0.2 * ch_norm)
        
        scores[k] = {
            'silhouette': sil, 
            'davies_bouldin': db,
            'calinski_harabasz': ch,
            'composite': composite,
            'labels': best_run_labels
        }
        
        print(f"      k={k} | Silhouette={sil:+.3f} | DB={db:.3f} | CH={ch:.1f} | Score={composite:.3f}")

        if composite > best_score:
            best_score = composite
            best_k = k
            best_labels = best_run_labels
            best_km = best_run_km

    return best_k, best_labels, scores, best_km


# --- 4. Validation de la stabilité des clusters ----------------------------------
def validate_cluster_stability(encoded: np.ndarray, best_k: int, n_validation_runs=10):
    """
    Vérifie la stabilité du clustering en réexécutant plusieurs fois.
    Calcule la cohérence entre les runs pour évaluer la robustesse.
    """
    print("  [V] Validation stabilité clustering...")
    
    all_labels = []
    for run in range(n_validation_runs):
        km = KMeans(n_clusters=best_k, init='k-means++', random_state=42 + run, n_init=20, max_iter=300)
        labels = km.fit_predict(encoded)
        all_labels.append(labels)
    
    # Calcul de la cohérence : ARI (Adjusted Rand Index)
    ari_scores = []
    for i in range(len(all_labels)):
        for j in range(i+1, len(all_labels)):
            ari = adjusted_rand_score(all_labels[i], all_labels[j])
            ari_scores.append(ari)
    
    mean_ari = np.mean(ari_scores) if ari_scores else 0
    std_ari = np.std(ari_scores) if ari_scores else 0
    
    stability = "[*] Stable" if mean_ari > 0.7 else ("[*] Moderee" if mean_ari > 0.5 else "[*] Instable")
    print(f"      Stabilité (ARI moyen): {mean_ari:.3f} ± {std_ari:.3f} — {stability}")
    
    return {'ari_mean': mean_ari, 'ari_std': std_ari, 'stability': stability}


def interpret_clusters(gov_df: pd.DataFrame, labels: np.ndarray, features: list, nom: str) -> dict:
    """
    Génère une description métier de chaque cluster basée sur les moyennes des features clés.
    Retourne un dict {cluster_id: 'nom métier'}.
    """
    gov_df = gov_df.copy()
    gov_df['cluster'] = labels

    cluster_info = {}
    for c in sorted(set(labels)):
        sub = gov_df[gov_df['cluster'] == c]
        info = {
            'cluster_size': len(sub),
            'prix_moyen': sub['prix_m2_moyen'].mean() if 'prix_m2_moyen' in sub else None,
            'prix_recent': sub['prix_m2_recent'].mean() if 'prix_m2_recent' in sub else None,
            'glissement': sub['glissement_moyen'].mean() if 'glissement_moyen' in sub else None,
            'glissement_recent': sub['glissement_recent'].mean() if 'glissement_recent' in sub else None,
            'attractivite': sub['score_attractivite'].mean() if 'score_attractivite' in sub else None,
            'volume': sub['volume_transactions'].mean() if 'volume_transactions' in sub else None,
        }
        cluster_info[c] = info

    prix_vals = {c: cluster_info[c]['prix_moyen'] for c in cluster_info if cluster_info[c]['prix_moyen']}
    gliss_vals = {c: cluster_info[c]['glissement_recent'] for c in cluster_info if cluster_info[c]['glissement_recent']}

    thresholds = LABEL_THRESHOLDS.get(nom, {})
    used_labels = set()
    label_map = {}

    def matches(rule: dict, prix: float | None, glissement: float | None) -> bool:
        if prix is None or glissement is None:
            return False
        if 'prix_min' in rule and prix < rule['prix_min']:
            return False
        if 'prix_max' in rule and prix > rule['prix_max']:
            return False
        if 'glissement_min' in rule and glissement < rule['glissement_min']:
            return False
        if 'glissement_max' in rule and glissement > rule['glissement_max']:
            return False
        return True

    fallback_map = {
        'Residentiel': 'Marche Stable - Prix Contenus',
        'Foncier': 'Foncier Rural - Stabilite',
        'Commercial': 'Marche Commercial Secondaire',
    }

    for c in sorted(cluster_info):
        prix = cluster_info[c].get('prix_moyen')
        glissement = cluster_info[c].get('glissement_recent')
        chosen = None

        if nom == 'Residentiel':
            order = ['premium', 'dynamique', 'emergent', 'stable']
        elif nom == 'Foncier':
            order = ['strategique', 'developpement', 'accessible', 'stable']
        else:
            order = ['majeur', 'actif', 'emergent', 'secondaire']

        for key in order:
            rule = thresholds.get(key, {})
            if matches(rule, prix, glissement):
                chosen = rule.get('label')
                break

        if chosen is None:
            chosen = fallback_map.get(nom, f'Cluster {c}')

        final_label = chosen
        if final_label in used_labels:
            suffix = c
            final_label = f"{chosen} - C{suffix}"
            while final_label in used_labels:
                suffix += 1
                final_label = f"{chosen} - C{suffix}"

        used_labels.add(final_label)
        label_map[c] = final_label

    return label_map, cluster_info


# --- 5. Pipeline principal amélioré ------------------------------------------
def run_pipeline(groupes: dict):
    print("\n" + "=" * 60)
    print("  SEGMENTATION GEOGRAPHIQUE — PREDICTION TENDANCES MARCHE")
    print("  Autoencoder ameliore + KMeans optimise (k auto)")
    print("=" * 60)

    resultats = {}

    for nom, df in groupes.items():
        print(f"\n{'-'*60}")
        print(f"  SEGMENT : {nom.upper()}")
        print(f"{'-'*60}")

        # Nettoyage fondamental des données
        cleaned_df, real_df, projection_df, data_stats = prepare_segment_data(df)
        print("  [0/5] Nettoyage des données...")
        print(f"         Lignes initiales : {data_stats['original_rows']:,}")
        print(f"         Doublons exacts retirés : {data_stats['duplicate_rows']:,}")
        print(f"         Observations réelles : {data_stats['real_rows']:,}")
        print(f"         Projections 2026 : {data_stats['projection_rows']:,}")

        if real_df.empty:
            print("         [!] Aucune observation réelle détectée, utilisation de l'ensemble nettoyé")
            real_df = cleaned_df[~cleaned_df['is_projection']].copy()
            if real_df.empty:
                real_df = cleaned_df.copy()

        if projection_df.empty:
            print("         Projections 2026 : aucune ligne détectée")

        # Feature engineering sur les données réelles uniquement
        print("  [1/5] Feature engineering temporel (réel)...")
        gov_df = engineer_features(real_df)
        projection_gov_df = engineer_features(projection_df) if not projection_df.empty else pd.DataFrame()
        features = FEATURES_BY_SEGMENT[nom]
        # Garder seulement les features disponibles
        features = [f for f in features if f in gov_df.columns]
        
        # Selection de features basée sur variance + pertinence
        print("  [2/5] Selection optimale de features...")
        X_test = gov_df[features].fillna(gov_df[features].median())
        
        # Filtrer les features à variance quasi-nulle (pas de signal)
        feature_vars = X_test.var()
        if (feature_vars > 1e-6).sum() > 2:
            features = [f for f in features if f in feature_vars[feature_vars > 1e-6].index]
        
        print(f"         {len(gov_df)} gouvernorats | {len(features)} features selectionnees")
        if len(features) < 2:
            print(f"         [!] Insuffisant de features ! Gardant au moins 3...")
            features = FEATURES_BY_SEGMENT[nom][:3]

        # Exclure les gouvernorats trop rares du clustering principal
        sparse_gov_df = gov_df[gov_df['volume_transactions'] < MIN_OBS].copy()
        gov_df = gov_df[gov_df['volume_transactions'] >= MIN_OBS].copy()
        if not sparse_gov_df.empty:
            sparse_names = sparse_gov_df['gouvernorat'].map(GOV_NAMES).tolist()
            print(f"         Gouvernorats exclus du clustering (<{MIN_OBS} obs) : {', '.join(sparse_names)}")

        if len(gov_df) < 2:
            print("         [!] Trop peu de gouvernorats denses pour clustériser, réintégration de tous les gouvernorats")
            gov_df = pd.concat([gov_df, sparse_gov_df], ignore_index=True)
            sparse_gov_df = gov_df.iloc[0:0].copy()

        # Normalisation robuste
        scaler = RobustScaler()
        X = scaler.fit_transform(gov_df[features].fillna(gov_df[features].median()))

        # Autoencoder amélioré
        print("  [3/5] Entrainement autoencoder optimise...")
        enc_dim = min(6, len(features) - 1, max(2, len(gov_df) // 3))
        # Augmenter epochs et améliorer learning rate pour meilleur clustering
        model, encoded = train_autoencoder(
            X, 
            epochs=1000,  # Augmenté de 500 à 1000
            lr=1.5e-3,    # Augmenté de 5e-4 à 1.5e-3
            encoding_dim=max(2, enc_dim),
            patience=150   # Augmenté de défaut à 150
        )

        # Selection k optimal améliorée
        print("  [4/5] Selection du k optimal (metriques multi-criteres)...")
        best_k, best_labels, k_scores, best_km = find_best_k(encoded, k_range=K_RANGE, n_runs=20)
        print(f"         -> k optimal = {best_k}")
        
        # Validation de la stabilité
        stability_metrics = validate_cluster_stability(encoded, best_k, n_validation_runs=10)

        # Interprétation métier
        print("  [5/5] Interpretation metier...")
        label_map, cluster_info = interpret_clusters(gov_df, best_labels, features, nom)

        gov_df['cluster'] = best_labels
        gov_df['cluster_name'] = gov_df['cluster'].map(label_map)
        gov_df['source'] = 'real_observation'

        if not sparse_gov_df.empty:
            nn = NearestNeighbors(n_neighbors=1)
            nn.fit(X)
            sparse_X = scaler.transform(sparse_gov_df[features].fillna(sparse_gov_df[features].median()))
            nearest_idx = nn.kneighbors(sparse_X, return_distance=False).ravel()
            sparse_gov_df = sparse_gov_df.copy()
            sparse_gov_df['cluster'] = gov_df.iloc[nearest_idx]['cluster'].to_numpy()
            sparse_gov_df['cluster_name'] = sparse_gov_df['cluster'].map(label_map)
            sparse_gov_df['source'] = 'nearest_neighbor'
            excluded_pairs = []
            for _, row in sparse_gov_df.iterrows():
                excluded_pairs.append(f"{GOV_NAMES.get(int(row['gouvernorat']), str(row['gouvernorat']))} -> {row['cluster_name']}")
            print(f"         Gouvernorats prédits par nearest neighbor : {', '.join(excluded_pairs)}")

        if not sparse_gov_df.empty:
            gov_df = pd.concat([gov_df, sparse_gov_df], ignore_index=True)

        if not projection_gov_df.empty:
            projection_gov_df = projection_gov_df.copy()
            projection_gov_df = encode_and_predict_clusters(model, scaler, features, projection_gov_df, best_km)
            projection_gov_df['cluster_name'] = projection_gov_df['cluster'].map(label_map)
            projection_gov_df['source'] = 'projection_2026'
            print(f"         Projections segmentées : {len(projection_gov_df):,} gouvernorats")

        # Affichage résultats avec les 3 métriques de qualité
        best_sil = k_scores[best_k]['silhouette'] if best_k in k_scores else -1
        best_db = k_scores[best_k]['davies_bouldin'] if best_k in k_scores else -1
        best_ch = k_scores[best_k]['calinski_harabasz'] if best_k in k_scores else -1
        print(f"\n  [*] k={best_k} optimal trouve")
        print(f"     Silhouette={best_sil:.3f} | Davies-Bouldin={best_db:.3f} | Calinski-Harabasz={best_ch:.1f}")
        print()

        for c in sorted(set(best_labels)):
            sub = gov_df[gov_df['cluster'] == c]
            govs = sub['gouvernorat'].map(GOV_NAMES).tolist()
            info = cluster_info[c]
            cname = label_map.get(c, f'Cluster {c}')

            print(f"  C{c} — {cname}")
            print(f"       Gouvernorats ({len(govs)}) : {', '.join(govs)}")
            print(f"       Prix m2/moy : {info['prix_moyen']:.2f} TND | "
                  f"Prix recent : {info['prix_recent']:.2f} TND")
            print(f"       Glissement moy : {info['glissement']:.2f}% | "
                  f"Glissement recent : {info['glissement_recent']:.2f}%")
            tendance = "[*] Acceleration" if info['glissement_recent'] > info['glissement'] else "[*] Deceleration"
            print(f"       Tendance : {tendance}")
            print()

        resultats[nom] = {
            'model': model,
            'scaler': scaler,
            'features': features,
            'k': best_k,
            'silhouette': best_sil,
            'davies_bouldin': best_db,
            'calinski_harabasz': best_ch,
            'stability_ari': stability_metrics.get('ari_mean', 0),
            'data_stats': data_stats,
            'cluster_map': gov_df.set_index('gouvernorat')['cluster'].to_dict(),
            'cluster_names': label_map,
            'cluster_info': cluster_info,
            'real_gov_df': gov_df,
            'projection_gov_df': projection_gov_df,
            'gov_df': gov_df,
            'best_km': best_km,
            'k_scores': k_scores,
        }

    return resultats


def save_results(resultats: dict, groupes: dict):
    """Sauvegarde modèle + CSV enrichi."""
    # Modèle
    excluded_keys = {'gov_df', 'real_gov_df', 'projection_gov_df'}
    save_data = {k: {kk: vv for kk, vv in v.items() if kk not in excluded_keys} for k, v in resultats.items()}
    with open(os.path.join(MODELS_DIR, 'autoencoder_kmeans_v2.pkl'), 'wb') as f:
        pickle.dump(save_data, f)

    # CSV consolidé avec séparation réelle / projections
    dfs = []
    real_dfs = []
    projection_dfs = []
    for nom, df in groupes.items():
        if nom not in resultats:
            continue
        res = resultats[nom]
        if 'real_gov_df' in res and not res['real_gov_df'].empty:
            real_df = res['real_gov_df'].copy()
            real_df['segment'] = nom
            real_df['source'] = 'real_observation'
            real_df['cluster_name'] = real_df['cluster'].map(res['cluster_names'])
            real_dfs.append(real_df)

        if 'projection_gov_df' in res and isinstance(res['projection_gov_df'], pd.DataFrame) and not res['projection_gov_df'].empty:
            proj_df = res['projection_gov_df'].copy()
            proj_df['segment'] = nom
            proj_df['source'] = 'projection_2026'
            proj_df['cluster_name'] = proj_df['cluster'].map(res['cluster_names'])
            projection_dfs.append(proj_df)

    if real_dfs:
        df_real = pd.concat(real_dfs, ignore_index=True)
        out_real_csv = os.path.join(MODELS_DIR, 'BO3_segmented_real.csv')
        df_real.to_csv(out_real_csv, index=False)
        dfs.append(df_real)
    else:
        out_real_csv = None

    if projection_dfs:
        df_projection = pd.concat(projection_dfs, ignore_index=True)
        out_projection_csv = os.path.join(MODELS_DIR, 'BO3_segmented_projection_2026.csv')
        df_projection.to_csv(out_projection_csv, index=False)
        dfs.append(df_projection)
    else:
        out_projection_csv = None

    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        out_csv = os.path.join(MODELS_DIR, 'BO3_segmented_tendances.csv')
        df_all.to_csv(out_csv, index=False)
    else:
        out_csv = os.path.join(MODELS_DIR, 'BO3_segmented_tendances.csv')

    # Résumé par cluster sur le réel uniquement
    gov_summary_frames = []
    for nom, res in resultats.items():
        if 'real_gov_df' not in res or res['real_gov_df'].empty:
            continue
        gdf = res['real_gov_df'].copy()
        gdf['segment'] = nom
        gdf['source'] = 'real_observation'
        gov_summary_frames.append(gdf)

    if gov_summary_frames:
        summary = pd.concat(gov_summary_frames, ignore_index=True)
        summary['gouvernorat_name'] = summary['gouvernorat'].map(GOV_NAMES)
        summary['cluster_name'] = summary['cluster'].map(lambda x: str(x))
        for nom, res in resultats.items():
            mask = summary['segment'] == nom
            summary.loc[mask, 'cluster_name'] = summary.loc[mask, 'cluster'].map(res['cluster_names'])
        out_summary = os.path.join(MODELS_DIR, 'cluster_summary_by_gouvernorat_real.csv')
        summary.to_csv(out_summary, index=False)
    else:
        out_summary = None

    print(f"\n[*] Modele      : {MODELS_DIR}/autoencoder_kmeans_v2.pkl")
    if out_real_csv:
        print(f"[*] Dataset reel : {out_real_csv}")
    if out_projection_csv:
        print(f"[*] Dataset proj : {out_projection_csv}")
    print(f"[*] Dataset tout : {out_csv}")
    if out_summary:
        print(f"[*] Gov summary  : {out_summary}")


def print_final_summary(resultats: dict):
    print("\n" + "=" * 60)
    print("  RESUME FINAL — SEGMENTATION TENDANCES MARCHE TUNISIE (v2 optimisee)")
    print("=" * 60)

    for nom, res in resultats.items():
        metrics_str = f"k={res['k']} | Sil={res['silhouette']:+.3f} | DB={res['davies_bouldin']:.3f} | CH={res.get('calinski_harabasz', 0):.1f}"
        print(f"\n[*] {nom} ({metrics_str})")
        clusters = {}
        for gov, c in res['cluster_map'].items():
            clusters.setdefault(c, []).append(GOV_NAMES.get(gov, str(gov)))
        for c in sorted(clusters):
            cname = res['cluster_names'].get(c, f'Cluster {c}')
            info = res['cluster_info'][c]
            print(f"   C{c} — {cname}")
            print(f"        {', '.join(clusters[c])}")
            tendance = "[*] Acceleration" if info['glissement_recent'] > info['glissement'] else "[*] Deceleration"
            print(f"        Prix: {info['prix_moyen']:.2f} TND | Glissement recent: {info['glissement_recent']:.2f}% | {tendance}")


# --- MAIN ---------------------------------------------------------------------
def load_data():
    """Charge les données depuis le dossier BO3."""
    script_dir = os.path.dirname(os.path.abspath(__file__))

    candidate_paths = [
        # Chemins relatifs au script (../../BO3, ../BO3)
        os.path.join(script_dir, '..', '..', 'BO3'),
        os.path.join(script_dir, '..', 'BO3'),
        # Même dossier que le script
        script_dir,
        # Dossier BO3 au même niveau que le script
        os.path.join(script_dir, 'BO3'),
    ]

    data_dir = None
    for path in candidate_paths:
        norm = os.path.normpath(path)
        # Vérifier que le dossier existe ET contient au moins un fichier _BO3.xlsx
        if os.path.isdir(norm) and any(
            f.endswith('_BO3.xlsx') for f in os.listdir(norm)
        ):
            data_dir = norm
            break

    if not data_dir:
        print("[*] Dossier BO3 introuvable")
        print("   Chemins testes :")
        for path in candidate_paths:
            print(f"   - {os.path.normpath(path)}")
        return {}

    print(f"[*] Donnees : {data_dir}")

    groupes = {}
    for nom in ['Residentiel', 'Foncier', 'Commercial']:
        f = os.path.join(data_dir, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            df = pd.read_excel(f)
            groupes[nom] = df
            print(f"  [OK] {nom} : {len(df):,} lignes")
        else:
            print(f"  [!] {nom} : Fichier non trouve ({f})")

    return groupes


if __name__ == '__main__':
    groupes = load_data()

    if not groupes:
        print("\n[!] Aucune donnée chargee")
        exit(1)

    resultats = run_pipeline(groupes)
    save_results(resultats, groupes)
    print_final_summary(resultats)

    print("\n" + "=" * 60)
    print("[*] Segmentation v2 terminie !")