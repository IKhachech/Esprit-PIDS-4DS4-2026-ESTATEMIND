"""
score_dataset.py — Application EfficientNet sur tout le dataset
EstateMind BO2 — Active Learning Pipeline

Etape 3 : Appliquer les modeles fine-tunes sur les 13,406 annonces
          pour produire etat_score et standing_score comme features

Input  :
    dataset_residentiel.csv
    efficientnet_standing.pth
    efficientnet_etat.pth

Output :
    scores_efficientnet.csv  → etat_score + standing_score par annonce
    dataset_residentiel_scored.csv  → dataset enrichi avec ces scores

Usage :
    python score_dataset.py
"""

import os, warnings
import numpy as np
import pandas as pd
import requests
from io import BytesIO
from PIL import Image
warnings.filterwarnings('ignore')

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from torchvision import models, transforms

# ================================================================
# CONFIG
# ================================================================

DATASET_FILE  = 'dataset_residentiel.csv'
MODEL_STAND   = 'efficientnet_standing.pth'
MODEL_ETAT    = 'efficientnet_etat.pth'
OUT_SCORES    = 'scores_efficientnet.csv'
OUT_DATASET   = 'dataset_residentiel_scored.csv'
CACHE_DIR     = 'img_cache_score'
BATCH_SIZE    = 32
IMG_SIZE      = 224
TIMEOUT       = 6
DEVICE        = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')

# ================================================================
# TRANSFORM
# ================================================================

TRANSFORM = transforms.Compose([
    transforms.Resize((IMG_SIZE, IMG_SIZE)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                         std =[0.229, 0.224, 0.225])
])

# ================================================================
# DATASET
# ================================================================

def download_image(url, cache_path, timeout=TIMEOUT):
    """Telecharge et met en cache une image."""
    if os.path.exists(cache_path):
        try:
            return Image.open(cache_path).convert('RGB')
        except Exception:
            pass
    try:
        r = requests.get(str(url), timeout=timeout,
                         headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            img = Image.open(BytesIO(r.content)).convert('RGB')
            img.save(cache_path)
            return img
    except Exception:
        pass
    return None


class ScoreDataset(Dataset):
    def __init__(self, df, cache_dir=CACHE_DIR):
        self.df        = df.reset_index(drop=True)
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        row        = self.df.iloc[idx]
        url        = str(row.get('image_url', ''))
        cache_path = os.path.join(self.cache_dir, f'{idx}.jpg')

        img = download_image(url, cache_path)
        if img is None:
            img = Image.new('RGB', (IMG_SIZE, IMG_SIZE), color=(128, 128, 128))

        return TRANSFORM(img), idx


# ================================================================
# CHARGEMENT MODELES
# ================================================================

def load_efficientnet(model_path, n_classes):
    """Charge un modele EfficientNet fine-tune."""
    model = models.efficientnet_b0(weights=None)
    in_features = model.classifier[1].in_features
    model.classifier = nn.Sequential(
        nn.Dropout(p=0.3),
        nn.Linear(in_features, 128),
        nn.ReLU(),
        nn.Dropout(p=0.2),
        nn.Linear(128, n_classes)
    )
    model.load_state_dict(
        torch.load(model_path, map_location=DEVICE)
    )
    model.eval()
    model.to(DEVICE)
    log(f'  Modele charge : {model_path} ({n_classes} classes)')
    return model


# ================================================================
# SCORING
# ================================================================

def score_all(model, dataset, n_classes, task_name):
    """
    Applique le modele sur tout le dataset.
    Retourne les predictions et probabilites.
    """
    loader = DataLoader(dataset, batch_size=BATCH_SIZE,
                        shuffle=False, num_workers=0)

    all_preds = np.zeros(len(dataset), dtype=int)
    all_probs = np.zeros((len(dataset), n_classes))

    with torch.no_grad():
        for imgs, indices in loader:
            imgs    = imgs.to(DEVICE)
            outputs = model(imgs)
            probs   = torch.softmax(outputs, dim=1).cpu().numpy()
            preds   = probs.argmax(axis=1)

            for i, idx in enumerate(indices.numpy()):
                all_preds[idx] = preds[i]
                all_probs[idx] = probs[i]

            n_done = min(indices.max().item() + 1, len(dataset))
            pct    = n_done / len(dataset) * 100
            print(f'  [{n_done:>6}/{len(dataset)}] {pct:>5.1f}%', end='\r')

    print()
    log(f'Distribution {task_name} : {dict(zip(*np.unique(all_preds, return_counts=True)))}')
    return all_preds, all_probs


# ================================================================
# MAIN
# ================================================================

def run():
    section('SCORE_DATASET.PY — APPLICATION EFFICIENTNET')
    log('Application des modeles fine-tunes sur tout le dataset')
    log(f'Device : {DEVICE}')

    # Charger dataset
    if not os.path.exists(DATASET_FILE):
        log(f'ERREUR : {DATASET_FILE} introuvable')
        return

    df = pd.read_csv(DATASET_FILE)
    log(f'Dataset : {len(df):,} annonces')

    # Reprendre si interrompu
    start_from = 0
    if os.path.exists(OUT_SCORES):
        existing   = pd.read_csv(OUT_SCORES)
        start_from = len(existing)
        if start_from >= len(df):
            log(f'Deja complete : {start_from} scores existants')
            df_scores = existing
        else:
            log(f'Reprise : {start_from}/{len(df)} deja traites')
            df = df.iloc[start_from:].copy()
    else:
        existing = None

    dataset = ScoreDataset(df)

    # ── Etat (0=ancien / 1=renove) ──────────────────────────────
    section('SCORING ETAT (0=ancien / 1=renove)')
    if not os.path.exists(MODEL_ETAT):
        log(f'ERREUR : {MODEL_ETAT} introuvable')
        log('Lancez d abord : python finetune_efficientnet.py')
        return

    model_etat    = load_efficientnet(MODEL_ETAT, n_classes=2)
    preds_etat, probs_etat = score_all(model_etat, dataset, 2, 'etat')
    log(f'  0=ancien : {(preds_etat==0).sum()} | 1=renove : {(preds_etat==1).sum()}')

    # ── Standing (0=modeste / 1=standard / 2=luxe) ───────────────
    section('SCORING STANDING (0=modeste / 1=standard / 2=luxe)')
    if not os.path.exists(MODEL_STAND):
        log(f'ERREUR : {MODEL_STAND} introuvable')
        return

    model_stand    = load_efficientnet(MODEL_STAND, n_classes=3)
    preds_stand, probs_stand = score_all(model_stand, dataset, 3, 'standing')
    # Reconvertir 0/1/2 → 1/2/3
    preds_stand_orig = preds_stand + 1
    log(f'  1=modeste : {(preds_stand_orig==1).sum()} | '
        f'2=standard : {(preds_stand_orig==2).sum()} | '
        f'3=luxe : {(preds_stand_orig==3).sum()}')

    # ── Sauvegarder les scores ───────────────────────────────────
    section('SAUVEGARDE')

    df_new = df.copy().reset_index(drop=True)
    df_new['etat_score']        = preds_etat
    df_new['etat_prob_renove']  = probs_etat[:, 1].round(4)
    df_new['standing_score']    = preds_stand_orig
    df_new['standing_prob_luxe']= probs_stand[:, 2].round(4)

    # Combiner avec les scores existants si reprise
    if existing is not None and start_from > 0:
        df_scores = pd.concat([existing, df_new[['etat_score','etat_prob_renove',
                                                   'standing_score','standing_prob_luxe']]],
                               ignore_index=True)
    else:
        df_scores = df_new[['etat_score','etat_prob_renove',
                             'standing_score','standing_prob_luxe']]

    df_scores.to_csv(OUT_SCORES, index=False)
    log(f'  {OUT_SCORES} : {len(df_scores):,} lignes')

    # Enrichir le dataset complet
    df_full = pd.read_csv(DATASET_FILE)
    df_full['etat_score']         = df_scores['etat_score'].values[:len(df_full)]
    df_full['etat_prob_renove']   = df_scores['etat_prob_renove'].values[:len(df_full)]
    df_full['standing_score']     = df_scores['standing_score'].values[:len(df_full)]
    df_full['standing_prob_luxe'] = df_scores['standing_prob_luxe'].values[:len(df_full)]

    df_full.to_csv(OUT_DATASET, index=False)
    log(f'  {OUT_DATASET} : {len(df_full):,} lignes | {len(df_full.columns)} colonnes')

    section('TERMINE')
    log('Nouvelles features ajoutees :')
    log('  etat_score        : 0=ancien | 1=renove')
    log('  etat_prob_renove  : probabilite d etre renove (0-1)')
    log('  standing_score    : 1=modeste | 2=standard | 3=luxe')
    log('  standing_prob_luxe: probabilite d etre luxueux (0-1)')
    log('')
    log('Prochaine etape : python train_model.py')
    log('  → utiliser dataset_residentiel_scored.csv')
    log('  → etat_score et standing_score comme features supplementaires')


if __name__ == '__main__':
    run()