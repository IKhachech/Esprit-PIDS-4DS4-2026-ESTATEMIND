"""
vision.py — Extraction de features visuelles avec ResNet50 + PCA
BO2 — EstateMind : Tunisian Real Estate Valuation

Objectif :
    Remplacer les 8 features heuristiques existantes par de vraies features CNN
    extraites des images réelles des annonces immobilières.

Pipeline :
    image_url → téléchargement en RAM → ResNet50 (2048-dim) → PCA (50-dim) → .npy

Utilisation :
    python vision.py

Outputs :
    image_features_resnet.npy   — shape (N_total, 50) — features PCA de toutes les annonces
    image_features_resnet_meta.json — metadata (index, dataset, url, statut)
    pca_model.pkl               — modèle PCA sauvegardé pour l'inférence future

Auteur : BO2 Team — EstateMind
"""

import os
import json
import time
import warnings
import numpy as np
import pandas as pd
import pickle
import requests
from io import BytesIO
from PIL import Image

warnings.filterwarnings('ignore')

# ================================================================
# CONFIG
# ================================================================

DATASETS = {
    'residentiel': 'residentiel_BO2.xlsx',
    'foncier':     'foncier_BO2.xlsx',
    'commercial':  'commercial_BO2.xlsx',
    'divers':      'divers_BO2.xlsx',
}

PCA_COMPONENTS  = 50      # Dimensions finales après PCA
BATCH_SIZE      = 64      # Images par batch ResNet50
N_WORKERS       = 16      # Threads parallèles pour téléchargement
IMG_SIZE        = (224, 224)
TIMEOUT         = 6       # secondes par requête HTTP
MAX_RETRIES     = 1
SAVE_EVERY      = 500     # Sauvegarde progressive toutes les N images
CHECKPOINT_NPY  = 'vision_checkpoint.npy'
CHECKPOINT_META = 'vision_checkpoint_meta.json'
ZERO_VECTOR     = np.zeros(PCA_COMPONENTS)  # fallback si image non disponible

OUTPUT_NPY      = 'image_features_resnet.npy'
OUTPUT_META     = 'image_features_resnet_meta.json'
OUTPUT_PCA      = 'pca_model.pkl'

# ================================================================
# CHARGEMENT MODELE ResNet50
# ================================================================

def load_resnet():
    """Charge ResNet50 pré-entraîné sur ImageNet, retire la couche de classification."""
    try:
        import torch
        import torch.nn as nn
        from torchvision import models, transforms

        model = models.resnet50(weights='IMAGENET1K_V1')
        # Retirer la dernière couche (fc) → output 2048-dim
        model = nn.Sequential(*list(model.children())[:-1])
        model.eval()

        # GPU si disponible, sinon CPU
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model = model.to(device)

        transform = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(IMG_SIZE),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225]
            )
        ])

        print(f"  ✔ ResNet50 chargé | device={device}")
        return model, transform, device, 'torch'

    except ImportError:
        print("  ⚠ PyTorch non disponible → fallback TensorFlow/Keras")

    try:
        from tensorflow.keras.applications import ResNet50
        from tensorflow.keras.applications.resnet50 import preprocess_input
        from tensorflow.keras.models import Model

        base = ResNet50(weights='imagenet', include_top=False, pooling='avg')
        print("  ✔ ResNet50 chargé (TensorFlow/Keras)")
        return base, preprocess_input, None, 'keras'

    except ImportError:
        raise RuntimeError(
            "Ni PyTorch ni TensorFlow disponible.\n"
            "Installe PyTorch : pip install torch torchvision\n"
            "ou TensorFlow   : pip install tensorflow"
        )


# ================================================================
# TÉLÉCHARGEMENT & PREPROCESSING IMAGE
# ================================================================

def download_image(url: str) -> Image.Image | None:
    """Télécharge une image depuis une URL → PIL Image RGB. Retourne None si échec."""
    if not url or not str(url).startswith('http'):
        return None
    for attempt in range(MAX_RETRIES):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            r = requests.get(url, timeout=TIMEOUT, headers=headers, stream=True)
            if r.status_code == 200:
                img = Image.open(BytesIO(r.content)).convert('RGB')
                return img
        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(0.5)
    return None


def preprocess_pil(img: Image.Image, transform, backend: str):
    """Transforme une PIL Image en tensor prêt pour le modèle."""
    if backend == 'torch':
        return transform(img)
    else:
        import numpy as np
        img_resized = img.resize(IMG_SIZE)
        arr = np.array(img_resized, dtype=np.float32)
        arr = np.expand_dims(arr, axis=0)
        return transform(arr)


# ================================================================
# EXTRACTION FEATURES PAR BATCH
# ================================================================


# ================================================================
# EXTRACTION COMPLETE — multithreading + reprise automatique
# ================================================================

def download_image_fast(args) -> tuple:
    """Worker pour ThreadPoolExecutor — retourne (index, PIL Image ou None)."""
    idx, url = args
    if not url or not str(url).startswith('http'):
        return idx, None
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        r = requests.get(url, timeout=TIMEOUT, headers=headers)
        if r.status_code == 200:
            img = Image.open(BytesIO(r.content)).convert('RGB')
            return idx, img
    except Exception:
        pass
    return idx, None


def extract_all_features(df_all: pd.DataFrame, model, transform, device, backend: str) -> tuple:
    """
    Extrait les features ResNet50 (2048-dim) pour toutes les annonces.
    - Téléchargement parallèle (N_WORKERS threads)
    - Sauvegarde progressive tous les SAVE_EVERY images
    - Reprise automatique si checkpoint existant
    """
    import torch
    from concurrent.futures import ThreadPoolExecutor, as_completed

    urls     = df_all['image_url'].fillna('').tolist()
    datasets = df_all['_dataset'].tolist()
    indices  = df_all.index.tolist()
    n        = len(urls)

    # ── Reprise depuis checkpoint si existant ───────────────────
    start_from = 0
    if os.path.exists(CHECKPOINT_NPY) and os.path.exists(CHECKPOINT_META):
        features_2048 = np.load(CHECKPOINT_NPY)
        with open(CHECKPOINT_META, 'r') as f:
            metadata = json.load(f)
        start_from = len(metadata)
        success_count = sum(1 for m in metadata if m['success'])
        print(f"  ♻ Reprise depuis checkpoint : {start_from}/{n} déjà traités")
    else:
        features_2048 = np.zeros((n, 2048))
        metadata      = []
        success_count = 0

    if start_from >= n:
        print("  ✔ Déjà complété !")
        return features_2048, metadata

    remaining = list(range(start_from, n))
    print(f"\n  Extraction ResNet50 sur {len(remaining):,} annonces restantes...")
    print(f"  Threads : {N_WORKERS} | Batch ResNet : {BATCH_SIZE} | Timeout : {TIMEOUT}s")
    print(f"  Estimation : ~{len(remaining) / N_WORKERS / 6:.0f} min\n")

    # Traitement par batch
    for batch_start in range(0, len(remaining), BATCH_SIZE):
        batch_indices = remaining[batch_start:batch_start + BATCH_SIZE]
        batch_urls    = [(i, urls[i]) for i in batch_indices]

        # Téléchargement parallèle
        downloaded = {}
        with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
            futures = {executor.submit(download_image_fast, item): item for item in batch_urls}
            for future in as_completed(futures):
                idx, img = future.result()
                downloaded[idx] = img

        # Préparer tensors pour ResNet50
        tensors     = []
        valid_local = []
        order       = []

        for i in batch_indices:
            img = downloaded.get(i)
            if img is not None:
                try:
                    tensors.append(transform(img))
                    valid_local.append(True)
                except Exception:
                    valid_local.append(False)
            else:
                valid_local.append(False)
            order.append(i)

        # Inférence ResNet50 sur les images valides
        valid_tensors = [t for t, v in zip(tensors, valid_local) if v]
        valid_order   = [i for i, v in zip(order, valid_local) if v]

        if valid_tensors:
            batch_tensor = torch.stack(valid_tensors).to(device)
            with torch.no_grad():
                batch_feats = model(batch_tensor).squeeze(-1).squeeze(-1).cpu().numpy()
            for i, feat in zip(valid_order, batch_feats):
                features_2048[i] = feat

        # Enregistrer metadata
        for i, valid in zip(order, valid_local):
            metadata.append({
                'global_index':    i,
                'original_index':  int(indices[i]),
                'dataset':         datasets[i],
                'url':             urls[i],
                'success':         valid,
            })
            if valid:
                success_count += 1

        processed = start_from + batch_start + len(batch_indices)
        pct = processed / n * 100
        print(f"  [{processed:>6}/{n}] {pct:>5.1f}% | succès={success_count}", end='\r')

        # Sauvegarde progressive
        if len(metadata) % SAVE_EVERY < BATCH_SIZE:
            np.save(CHECKPOINT_NPY, features_2048)
            with open(CHECKPOINT_META, 'w') as f:
                json.dump(metadata, f)

    # Sauvegarde finale checkpoint
    np.save(CHECKPOINT_NPY, features_2048)
    with open(CHECKPOINT_META, 'w') as f:
        json.dump(metadata, f)

    print(f"\n  ✔ Extraction terminée : {success_count}/{n} images ({success_count/n*100:.1f}%)")
    return features_2048, metadata


# ================================================================
# PCA
# ================================================================

def apply_pca(features_2048: np.ndarray, n_components: int = PCA_COMPONENTS) -> tuple:
    """
    Applique PCA sur les features 2048-dim.
    Ignore les lignes nulles (images non téléchargées) pour fit le PCA.
    Retourne (features_pca, pca_model).
    """
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    print(f"\n  Calcul PCA : {features_2048.shape[1]}-dim → {n_components}-dim")

    # Identifier les lignes non nulles (vraies features)
    non_zero_mask = np.any(features_2048 != 0, axis=1)
    n_valid = non_zero_mask.sum()
    print(f"  Lignes valides pour PCA : {n_valid:,}/{len(features_2048):,}")

    # Normalisation avant PCA
    scaler = StandardScaler()
    features_valid = scaler.fit_transform(features_2048[non_zero_mask])

    # Fit PCA sur les vraies features
    n_comp = min(n_components, n_valid, features_2048.shape[1])
    pca = PCA(n_components=n_comp, random_state=42)
    pca.fit(features_valid)

    variance_explained = pca.explained_variance_ratio_.sum() * 100
    print(f"  Variance expliquée par {n_comp} composantes : {variance_explained:.1f}%")

    # Transformer toutes les lignes
    features_all_scaled = scaler.transform(features_2048)
    features_pca = pca.transform(features_all_scaled)

    # Remettre à zéro les lignes sans image (fallback)
    features_pca[~non_zero_mask] = 0.0

    # Sauvegarder le modèle PCA + scaler pour l'inférence future
    pca_bundle = {'pca': pca, 'scaler': scaler, 'n_components': n_comp}
    with open(OUTPUT_PCA, 'wb') as f:
        pickle.dump(pca_bundle, f)
    print(f"  ✔ PCA model sauvegardé : {OUTPUT_PCA}")

    return features_pca, pca_bundle


# ================================================================
# ENTRY POINT
# ================================================================

def run_vision_extraction():
    print("\n" + "=" * 65)
    print("   VISION.PY — EXTRACTION FEATURES RESNET50 + PCA")
    print("   EstateMind BO2 — Tunisian Real Estate")
    print("=" * 65)

    # ── 1. Charger tous les datasets ────────────────────────────
    print("\n[1] Chargement des datasets...")
    dfs = []
    for name, filename in DATASETS.items():
        if not os.path.exists(filename):
            print(f"  ⚠ {filename} introuvable — ignoré")
            continue
        df = pd.read_excel(filename)
        df['_dataset'] = name
        dfs.append(df)
        n_url = df['image_url'].notna().sum()
        print(f"  ✔ {filename:<30} : {len(df):>6} lignes | {n_url:>5} avec URL")

    if not dfs:
        raise FileNotFoundError("Aucun dataset trouvé. Lance ce script depuis le dossier BO2.")

    df_all = pd.concat(dfs, ignore_index=False)
    print(f"\n  Total : {len(df_all):,} annonces | {df_all['image_url'].notna().sum():,} avec URL")

    # ── 2. Charger ResNet50 ──────────────────────────────────────
    print("\n[2] Chargement ResNet50...")
    model, transform, device, backend = load_resnet()

    # ── 3. Extraction features 2048-dim ─────────────────────────
    print("\n[3] Extraction features CNN...")
    features_2048, metadata = extract_all_features(df_all, model, transform, device, backend)

    # ── 4. PCA 2048 → 50 ────────────────────────────────────────
    print("\n[4] Réduction dimensionnelle PCA...")
    features_pca, pca_bundle = apply_pca(features_2048, PCA_COMPONENTS)

    # ── 5. Sauvegarde ────────────────────────────────────────────
    print("\n[5] Sauvegarde des résultats...")

    np.save(OUTPUT_NPY, features_pca)
    print(f"  ✔ {OUTPUT_NPY} : shape={features_pca.shape}")

    with open(OUTPUT_META, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"  ✔ {OUTPUT_META} : {len(metadata)} entrées")

    # ── 6. Statistiques finales ──────────────────────────────────
    print("\n" + "=" * 65)
    print("   RÉSUMÉ")
    print("=" * 65)
    success = sum(1 for m in metadata if m['success'])
    by_ds   = {}
    for m in metadata:
        ds = m['dataset']
        if ds not in by_ds:
            by_ds[ds] = {'total': 0, 'success': 0}
        by_ds[ds]['total']   += 1
        by_ds[ds]['success'] += int(m['success'])

    for ds, counts in by_ds.items():
        pct = counts['success'] / counts['total'] * 100 if counts['total'] > 0 else 0
        print(f"  {ds:<15} : {counts['success']:>5}/{counts['total']:>5} ({pct:.0f}%)")

    print(f"\n  Total succès  : {success}/{len(metadata)} ({success/len(metadata)*100:.1f}%)")
    print(f"  Features shape: {features_pca.shape}")
    print(f"  PCA variance  : {pca_bundle['pca'].explained_variance_ratio_.sum()*100:.1f}%")
    print("\n  Fichiers générés :")
    print(f"    → {OUTPUT_NPY}  (à utiliser à la place de image_embeddings.npy)")
    print(f"    → {OUTPUT_META} (metadata pour debugging)")
    print(f"    → {OUTPUT_PCA}  (pour inférence future)")
    print("\n  ✔ Extraction terminée avec succès !")
    print("=" * 65)

    return features_pca, metadata


# ================================================================
# UTILITAIRE : charger les features pour l'entraînement
# ================================================================

def load_image_features(npy_path: str = OUTPUT_NPY) -> np.ndarray:
    """
    Charge les features image pour les utiliser dans le modèle XGBoost.

    Usage dans modeling.py :
        from vision import load_image_features
        img_features = load_image_features()  # shape (28573, 50)
        X = np.concatenate([X_tabular, img_features], axis=1)
    """
    if not os.path.exists(npy_path):
        raise FileNotFoundError(
            f"{npy_path} introuvable. Lance d'abord : python vision.py"
        )
    features = np.load(npy_path)
    print(f"  ✔ Features image chargées : {features.shape}")
    return features


if __name__ == '__main__':
    run_vision_extraction()