"""
preannotate.py — Pre-annotation automatique avec CLIP
EstateMind BO2 — Active Learning Pipeline

Etape 1 du fine-tuning EfficientNet :
    CLIP propose des labels pour chaque image
    → tu valides/corriges les cas douteux
    → ces labels servent a fine-tuner EfficientNet-B0

Labels produits :
    standing    : 1=modeste | 2=standard | 3=luxe
    etat        : 1=ancien  | 2=correct  | 3=renove
    lumineux    : 0=sombre  | 1=lumineux
    cuisine     : 0=simple  | 1=moderne
    exterieur   : 0=aucun   | 1=jardin/piscine/terrasse visible

Output :
    annotations_clip.csv  → labels automatiques + scores de confiance
    a_valider.csv         → images avec confiance faible (<0.60) a corriger

Usage :
    pip install openai-clip
    python preannotate.py
"""

import os, warnings
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
    'residentiel': 'dataset_residentiel.csv',  # vente + location ensemble
    # terrain exclu — standing/etat n'ont pas de sens pour un terrain
}

TIMEOUT        = 6
N_WORKERS      = 8
# Seuils de confiance par nombre de labels
# 2 labels → max theorique 0.50 → seuil 0.62
# 3 labels → max theorique 0.33 → seuil 0.45
CONF_MIN_2 = 0.62
CONF_MIN_3 = 0.45

N_LABELS = {
    'standing': 3,
    'etat':     2,
}
BATCH_SAVE     = 100    # sauvegarder toutes les N images

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')

# ================================================================
# LABELS CLIP PAR CATEGORIE
# ================================================================

# Pour chaque categorie, CLIP choisit le label le plus proche
LABELS = {
    # Standing : visible sur toutes les photos (salon, chambre, cuisine...)
    'standing': [
        "luxury high-end real estate modern elegant premium quality",
        "standard average normal quality real estate",
        "modest old simple low quality real estate",
    ],
    # Etat renovation : visible sur toutes les photos
    'etat': [
        "renovated new modern interior clean fresh walls floors",
        "old worn not renovated deteriorated dirty walls floors",
    ],
}

# Labels pour terrain — ce qu'on peut voir sur une photo
LABELS_TERRAIN = {
    'standing': [
        "premium valuable land urban zone infrastructure visible",
        "standard average land semi-urban",
        "low value rural agricultural land remote",
    ],
    'etat': [
        "flat clean accessible land ready to build",
        "rough rocky difficult terrain hard to access",
    ],
}

# Mapping label index → valeur finale
LABEL_VALUES = {
    'standing': {0: 3, 1: 2, 2: 1},   # 0=luxe→3, 1=standard→2, 2=modeste→1
    'etat':     {0: 1, 1: 0},          # 0=renove→1, 1=ancien→0
}

# ================================================================
# CHARGEMENT CLIP
# ================================================================

def load_clip():
    try:
        import clip
        import torch
        import os
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        # Specifier un dossier de cache valide pour eviter OSError
        cache_dir = os.path.join(os.path.expanduser('~'), 'clip_cache')
        os.makedirs(cache_dir, exist_ok=True)
        model, preprocess = clip.load('ViT-B/32', device=device,
                                       download_root=cache_dir)
        model.eval()
        log(f'CLIP ViT-B/32 | device={device}')
        return model, preprocess, device, True
    except ImportError:
        log('ERREUR : pip install openai-clip')
        return None, None, None, False
    except Exception as e:
        log(f'Erreur CLIP : {e}')
        log('Essayez de supprimer le cache : %USERPROFILE%\.cache\clip')
        return None, None, None, False

# ================================================================
# TELECHARGEMENT
# ================================================================

def download_image(args):
    idx, url = args
    if not url or not str(url).startswith('http'):
        return idx, None
    try:
        r = requests.get(str(url), timeout=TIMEOUT,
                         headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return idx, Image.open(BytesIO(r.content)).convert('RGB')
    except Exception:
        pass
    return idx, None

def parse_urls(images_str, fallback):
    urls = []
    raw  = str(images_str).strip()
    if raw and raw not in ['nan', 'None', '']:
        parts = [u.strip() for u in raw.split('|') if u.strip()]
        urls  = [u for u in parts if u.startswith('http')]
    if not urls:
        fb = str(fallback).strip()
        if fb.startswith('http'):
            urls = [fb]
    return urls[:4]

# ================================================================
# ANNOTATION CLIP
# ================================================================

def annotate_image(img, model, preprocess, device, labels_dict):
    """
    Annote une image avec CLIP.
    Retourne {categorie: (label_idx, label_valeur, confiance)}
    """
    import clip, torch
    results = {}
    try:
        img_tensor = preprocess(img).unsqueeze(0).to(device)
        with torch.no_grad():
            img_feat = model.encode_image(img_tensor)
            img_feat = img_feat / img_feat.norm(dim=-1, keepdim=True)

            for cat, labels in labels_dict.items():
                tokens   = clip.tokenize(labels).to(device)
                txt_feat = model.encode_text(tokens)
                txt_feat = txt_feat / txt_feat.norm(dim=-1, keepdim=True)
                sim      = (img_feat @ txt_feat.T).squeeze(0)
                probs    = sim.softmax(dim=0).cpu().numpy()
                best_idx = int(np.argmax(probs))
                conf     = float(probs[best_idx])
                val      = LABEL_VALUES[cat][best_idx]
                results[cat] = (best_idx, val, conf)
    except Exception:
        for cat in labels_dict:
            results[cat] = (0, 1, 0.0)
    return results

def annotate_annonce(urls, model, preprocess, device, labels_dict):
    """
    Annote toutes les images d une annonce et retourne
    le label majoritaire + confiance moyenne.
    """
    all_results = []

    downloaded = {}
    with ThreadPoolExecutor(max_workers=min(N_WORKERS, len(urls))) as ex:
        futures = {ex.submit(download_image, (i, url)): i
                   for i, url in enumerate(urls)}
        for future in as_completed(futures):
            idx, img = future.result()
            downloaded[idx] = img

    for i in range(len(urls)):
        img = downloaded.get(i)
        if img is not None:
            res = annotate_image(img, model, preprocess, device, labels_dict)
            all_results.append(res)

    if not all_results:
        return {cat: {'label': 1, 'confiance': 0.0} for cat in labels_dict}

    # Moyenner les resultats sur toutes les images
    final = {}
    for cat in labels_dict:
        vals  = [r[cat][1]   for r in all_results]
        confs = [r[cat][2]   for r in all_results]
        # Label majoritaire
        from collections import Counter
        label = Counter(vals).most_common(1)[0][0]
        conf  = float(np.mean(confs))
        final[cat] = {'label': label, 'confiance': round(conf, 3)}
    return final

# ================================================================
# TRAITEMENT PAR SEGMENT
# ================================================================

def process_segment(seg_name, csv_file, model, preprocess, device):
    section(f'PRE-ANNOTATION : {seg_name.upper()}')

    df = pd.read_csv(csv_file)
    log(f'Dataset complet : {len(df)} annonces')

    # Active Learning — commencer avec 500 images
    # Changer N_SAMPLE=None pour traiter tout le dataset
    N_SAMPLE = 500
    if N_SAMPLE and len(df) > N_SAMPLE:
        df = df.sample(n=N_SAMPLE, random_state=42).reset_index(drop=True)
        log(f'Echantillon Active Learning : {N_SAMPLE} annonces')
        log(f'(changer N_SAMPLE=None pour traiter tout le dataset)')

    labels_dict = LABELS_TERRAIN if seg_name == 'terrain' else LABELS
    cats        = list(labels_dict.keys())

    out_file = f'annotations_clip_{seg_name}.csv'
    start_from = 0
    results = []

    if os.path.exists(out_file):
        existing   = pd.read_csv(out_file)
        start_from = len(existing)
        results    = existing.to_dict('records')
        log(f'Reprise : {start_from}/{len(df)} deja annotes')

    has_images = 'images' in df.columns

    for i in range(start_from, len(df)):
        row      = df.iloc[i]
        img_str  = str(row['images'])    if has_images else ''
        url_fall = str(row['image_url']) if 'image_url' in df.columns else ''
        urls     = parse_urls(img_str, url_fall)

        # Utiliser seulement la premiere image pour aller plus vite
        urls_fast = urls[:1] if urls else []
        if urls_fast:
            ann = annotate_annonce(urls_fast, model, preprocess, device, labels_dict)
        else:
            ann = {cat: {'label': 0, 'confiance': 0.0} for cat in cats}

        # Construire la ligne
        row_out = {'idx': i, 'image_url': url_fall}
        for cat in cats:
            row_out[cat]               = ann[cat]['label']
            row_out[f'{cat}_confiance'] = ann[cat]['confiance']

        # Marquer si confiance faible → a valider manuellement
        # Seuil adapte au nombre de labels par categorie
        faible = False
        for cat in cats:
            n_lab = N_LABELS.get(cat, 2)
            seuil = CONF_MIN_3 if n_lab == 3 else CONF_MIN_2
            if ann[cat]['confiance'] < seuil:
                faible = True
                break
        row_out['a_valider'] = int(faible)

        results.append(row_out)

        # Sauvegarder periodiquement
        if (i + 1) % BATCH_SAVE == 0 or i == len(df) - 1:
            pd.DataFrame(results).to_csv(out_file, index=False)
            pct = (i + 1) / len(df) * 100
            n_valider = sum(1 for r in results if r.get('a_valider', 0))
            print(f'  [{i+1:>6}/{len(df)}] {pct:>5.1f}% | '
                  f'a valider={n_valider}', end='\r')

    print()
    df_ann = pd.DataFrame(results)
    df_ann.to_csv(out_file, index=False)

    # Extraire les cas a valider
    df_valider = df_ann[df_ann['a_valider'] == 1]
    valider_file = f'a_valider_{seg_name}.csv'
    df_valider.to_csv(valider_file, index=False)

    log(f'')
    log(f'  annotations_clip_{seg_name}.csv : {len(df_ann)} lignes')
    log(f'  a_valider_{seg_name}.csv        : {len(df_valider)} images a corriger')
    log(f'  Confiance moyenne :')
    for cat in cats:
        col = f'{cat}_confiance'
        if col in df_ann.columns:
            log(f'    {cat:<15} : {df_ann[col].mean():.3f}')

    return df_ann

# ================================================================
# MAIN
# ================================================================

def run():
    section('PREANNOTATE.PY — CLIP PRE-ANNOTATION')
    log('Etape 1 du pipeline Active Learning / Fine-tuning EfficientNet')
    log('CLIP propose des labels → tu valides les cas douteux')

    model, preprocess, device, ok = load_clip()
    if not ok:
        return

    for seg_name, csv_file in SEGMENTS.items():
        if not os.path.exists(csv_file):
            log(f'⚠ {csv_file} introuvable — ignore')
            continue
        process_segment(seg_name, csv_file, model, preprocess, device)

    section('TERMINE')
    log('Fichiers produits :')
    for seg_name in SEGMENTS:
        ann_f = f'annotations_clip_{seg_name}.csv'
        val_f = f'a_valider_{seg_name}.csv'
        if os.path.exists(ann_f):
            df = pd.read_csv(ann_f)
            n_val = df['a_valider'].sum() if 'a_valider' in df.columns else 0
            log(f'  {ann_f} : {len(df)} images | {n_val} a valider')
    log('')
    log('Prochaine etape :')
    log('  1. Ouvrir a_valider_*.csv et corriger les labels')
    log('  2. Lancer : python finetune_efficientnet.py')


if __name__ == '__main__':
    run()