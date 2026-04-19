"""
vision_new.py — Extraction features CLIP (multi-images par annonce)
EstateMind BO2 — Nouvelle division (2 fichiers)

Pour chaque annonce :
    - Charge TOUTES les images (colonne images, separateur |)
    - Passe chaque image dans CLIP ViT-B/32
    - Moyenne les vecteurs CLIP de toutes les images
    - Reduit a 50 dims via PCA

Pourquoi CLIP ?
    ResNet50 voit : formes, textures, couleurs
    CLIP voit     : "cuisine moderne", "vue sur mer", "appartement lumineux"
    CLIP est entraine sur 400M paires image+texte → comprend le sens semantique

Pourquoi plusieurs images ?
    1 image = facade seulement
    4 images = facade + salon + cuisine + chambre → bien plus representatif

Outputs :
    image_features_residentiel.npy
    image_features_terrain.npy
    pca_models.pkl

Usage :
    pip install openai-clip
    python vision_new.py
"""

import os, json, warnings, pickle
import numpy as np
import pandas as pd
import requests
from io import BytesIO
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
warnings.filterwarnings('ignore')

# ================================================================
# CONFIG
# ================================================================

SEGMENTS = {
    'residentiel_vente':    'dataset_residentiel_vente.csv',
    'residentiel_location': 'dataset_residentiel_location.csv',
    'terrain':              'dataset_terrain.csv',
}

PCA_COMPONENTS = 50
N_WORKERS      = 16
BATCH_SIZE     = 32   # CLIP est plus lourd
TIMEOUT        = 6
MAX_IMAGES     = 4    # max images par annonce
OUTPUT_PCA     = 'pca_models.pkl'

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')


# ================================================================
# CHARGEMENT CLIP
# ================================================================

def load_clip():
    try:
        import clip
        import torch
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model, preprocess = clip.load('ViT-B/32', device=device)
        model.eval()
        log(f'CLIP ViT-B/32 charge | device={device} | features=512 dims')
        return model, preprocess, device, 512, 'clip'
    except ImportError:
        log('CLIP non installe → fallback ResNet50')
        log('  Pour installer : pip install openai-clip')
        import torch, torch.nn as nn
        from torchvision import models, transforms
        m = models.resnet50(weights='IMAGENET1K_V1')
        m = nn.Sequential(*list(m.children())[:-1])
        m.eval()
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        m = m.to(device)
        t = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std =[0.229, 0.224, 0.225])
        ])
        log(f'ResNet50 charge (fallback) | device={device} | features=2048 dims')
        return m, t, device, 2048, 'resnet50'


# ================================================================
# PARSING DES URLs MULTIPLES
# ================================================================

def parse_urls(images_str, image_url_fallback):
    """
    Parse la colonne images (separateur |) et retourne liste d URLs.
    Fallback sur image_url si images est vide.
    """
    urls = []
    raw  = str(images_str).strip()

    if raw and raw not in ['nan', 'None', '']:
        parts = [u.strip() for u in raw.split('|') if u.strip()]
        urls  = [u for u in parts if u.startswith('http')]

    # Fallback sur image_url si pas d URLs valides
    if not urls:
        fallback = str(image_url_fallback).strip()
        if fallback.startswith('http'):
            urls = [fallback]

    # Limiter a MAX_IMAGES
    return urls[:MAX_IMAGES]


# ================================================================
# TELECHARGEMENT IMAGE
# ================================================================

def download_image(args):
    idx, url = args
    if not url or not str(url).startswith('http'):
        return idx, None
    try:
        r = requests.get(str(url), timeout=TIMEOUT,
                         headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            img = Image.open(BytesIO(r.content)).convert('RGB')
            return idx, img
    except Exception:
        pass
    return idx, None


# ================================================================
# EXTRACTION CLIP PAR LOT
# ================================================================

def encode_batch(images_list, model, preprocess, device, n_feats, model_type):
    """Encode une liste d images PIL → matrice numpy (N, n_feats)."""
    import torch
    tensors = []
    valid   = []
    for i, img in enumerate(images_list):
        if img is None:
            continue
        try:
            tensors.append(preprocess(img))
            valid.append(i)
        except Exception:
            pass

    if not tensors:
        return np.zeros((len(images_list), n_feats)), []

    batch = torch.stack(tensors).to(device)
    with torch.no_grad():
        if model_type == 'clip':
            feats = model.encode_image(batch).float().cpu().numpy()
        else:
            feats = model(batch).squeeze(-1).squeeze(-1).cpu().numpy()

    result = np.zeros((len(images_list), n_feats))
    for i, feat in zip(valid, feats):
        result[i] = feat
    return result, valid


# ================================================================
# EXTRACTION FEATURES — MULTI-IMAGES PAR ANNONCE
# ================================================================

def extract_features(df, model, preprocess, device, n_feats, model_type, seg_name):
    """
    Pour chaque annonce :
        1. Parse toutes les URLs (colonne images | separateur)
        2. Telecharge toutes les images en parallele
        3. Encode via CLIP
        4. Moyenne les vecteurs → 1 vecteur par annonce
    """
    n             = len(df)
    features_out  = np.zeros((n, n_feats))
    success_mask  = np.zeros(n, dtype=bool)
    success_count = 0

    # Checkpoint
    ckpt_npy  = f'ckpt_{seg_name}.npy'
    ckpt_meta = f'ckpt_{seg_name}_meta.json'
    start_from = 0

    if os.path.exists(ckpt_npy) and os.path.exists(ckpt_meta):
        features_out = np.load(ckpt_npy)
        with open(ckpt_meta) as f:
            meta = json.load(f)
        start_from    = meta['processed']
        success_count = meta['success']
        success_mask[:start_from] = meta['mask'][:start_from]
        log(f'Reprise checkpoint : {start_from}/{n} deja traites')

    # Preparer liste URLs par annonce
    has_images_col = 'images' in df.columns
    all_url_lists  = []
    for i in range(n):
        row = df.iloc[i]
        img_str  = str(row['images'])   if has_images_col else ''
        url_fall = str(row['image_url']) if 'image_url' in df.columns else ''
        all_url_lists.append(parse_urls(img_str, url_fall))

    n_single = sum(1 for ul in all_url_lists if len(ul) == 1)
    n_multi  = sum(1 for ul in all_url_lists if len(ul) > 1)
    log(f'Annonces : 1 image={n_single} | 2-4 images={n_multi}')

    remaining = list(range(start_from, n))
    log(f'{len(remaining)} annonces a traiter...')

    # Traitement par batch d annonces
    for batch_start in range(0, len(remaining), BATCH_SIZE):
        batch_indices = remaining[batch_start:batch_start + BATCH_SIZE]

        # Collecter toutes les (idx_annonce, url_idx, url) a telecharger
        to_download = []
        for ann_i in batch_indices:
            for url_j, url in enumerate(all_url_lists[ann_i]):
                to_download.append((ann_i, url_j, url))

        # Telechargement parallele
        downloaded = {}
        with ThreadPoolExecutor(max_workers=N_WORKERS) as ex:
            futures = {
                ex.submit(download_image, (f'{ai}_{ui}', url)): (ai, ui)
                for ai, ui, url in to_download
            }
            for future in as_completed(futures):
                key, img = future.result()
                ann_i, url_j = futures[future]
                if ann_i not in downloaded:
                    downloaded[ann_i] = {}
                downloaded[ann_i][url_j] = img

        # Encoder et moyenner par annonce
        for ann_i in batch_indices:
            imgs_dict = downloaded.get(ann_i, {})
            if not imgs_dict:
                continue

            imgs_list = [imgs_dict.get(j) for j in range(len(all_url_lists[ann_i]))]
            feats_mat, valid = encode_batch(
                imgs_list, model, preprocess, device, n_feats, model_type
            )

            if valid:
                # Moyenne des features de toutes les images valides
                mean_feat              = feats_mat[valid].mean(axis=0)
                features_out[ann_i]    = mean_feat
                success_mask[ann_i]    = True
                success_count         += 1

        processed = start_from + batch_start + len(batch_indices)
        pct       = processed / n * 100
        print(f'  [{processed:>6}/{n}] {pct:>5.1f}% | succes={success_count}', end='\r')

        # Checkpoint toutes les 300 annonces
        if batch_start % 300 < BATCH_SIZE:
            np.save(ckpt_npy, features_out)
            with open(ckpt_meta, 'w') as f:
                json.dump({
                    'processed': processed,
                    'success':   success_count,
                    'mask':      success_mask.tolist(),
                }, f)

    # Sauvegarde finale checkpoint
    np.save(ckpt_npy, features_out)
    with open(ckpt_meta, 'w') as f:
        json.dump({
            'processed': n,
            'success':   success_count,
            'mask':      success_mask.tolist(),
        }, f)

    print(f'\n  {success_count}/{n} annonces extraites ({success_count/n*100:.1f}%)')
    return features_out, success_mask


# ================================================================
# PCA
# ================================================================

def apply_pca(features, success_mask, n_components=PCA_COMPONENTS):
    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    n_valid = success_mask.sum()
    n_dim   = features.shape[1]
    log(f'PCA : {n_dim}-dim → {n_components}-dim | {n_valid} lignes valides')

    scaler      = StandardScaler()
    feats_valid = scaler.fit_transform(features[success_mask])

    n_comp = min(n_components, n_valid - 1, n_dim)
    pca    = PCA(n_components=n_comp, random_state=42)
    pca.fit(feats_valid)

    var = pca.explained_variance_ratio_.sum() * 100
    log(f'Variance expliquee : {var:.1f}%')

    all_scaled   = scaler.transform(features)
    features_pca = pca.transform(all_scaled)
    features_pca[~success_mask] = 0.0

    return features_pca, {'pca': pca, 'scaler': scaler, 'n_components': n_comp}


def cleanup(seg_name):
    for f in [f'ckpt_{seg_name}.npy', f'ckpt_{seg_name}_meta.json']:
        if os.path.exists(f): os.remove(f)


# ================================================================
# MAIN
# ================================================================

def run():
    section('VISION_NEW.PY — CLIP MULTI-IMAGES')
    log('EstateMind BO2 — CLIP ViT-B/32 + moyenne toutes images')

    model, preprocess, device, n_feats, model_type = load_clip()
    pca_models = {}

    for seg_name, csv_file in SEGMENTS.items():
        section(f'SEGMENT : {seg_name.upper()}')

        if not os.path.exists(csv_file):
            log(f'⚠ {csv_file} introuvable — ignore')
            continue

        df = pd.read_csv(csv_file)
        log(f'{len(df)} lignes')

        # Stats images
        if 'images' in df.columns:
            n_multi = sum(1 for v in df['images'].fillna('')
                         if '|' in str(v))
            log(f'Colonne images : {n_multi} annonces multi-images')
        else:
            log('Colonne images absente → utilisation image_url uniquement')

        # Extraction CLIP multi-images
        features_raw, success_mask = extract_features(
            df, model, preprocess, device, n_feats, model_type, seg_name
        )

        # PCA
        features_pca, pca_bundle = apply_pca(features_raw, success_mask)

        # Sauvegarder
        out_npy = f'image_features_{seg_name}.npy'
        np.save(out_npy, features_pca)
        log(f'Sauvegarde : {out_npy} | shape={features_pca.shape}')

        pca_models[seg_name] = pca_bundle
        cleanup(seg_name)

    with open(OUTPUT_PCA, 'wb') as f:
        pickle.dump(pca_models, f)
    log(f'PCA models sauvegardes : {OUTPUT_PCA}')

    section('RAPPORT FINAL')
    total = 0
    for seg_name in SEGMENTS:
        npy = f'image_features_{seg_name}.npy'
        if os.path.exists(npy):
            arr       = np.load(npy)
            n_nonzero = arr.any(axis=1).sum()
            log(f'{seg_name:<15} : shape={arr.shape} | '
                f'images={n_nonzero} ({n_nonzero/arr.shape[0]*100:.0f}%)')
            total += arr.shape[0]
    log(f'\nTotal lignes traitees : {total:,}')
    log(f'Modele utilise       : {model_type}')
    log('Prochaine etape      : python train_model.py')


if __name__ == '__main__':
    run()