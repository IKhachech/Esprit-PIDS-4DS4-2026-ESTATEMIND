"""
finetune_efficientnet.py — Fine-tuning EfficientNet-B0
EstateMind BO2 — Active Learning Pipeline

Etape 2 : Fine-tuner EfficientNet-B0 sur les annotations validees

Input  : annotations_clip_residentiel.csv (467 images annotees)
Output : efficientnet_standing.pth  — modele standing (1/2/3)
         efficientnet_etat.pth      — modele etat (0/1)

Usage :
    pip install torchvision
    python finetune_efficientnet.py
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
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score

# ================================================================
# CONFIG
# ================================================================

ANNOTATIONS_FILE = 'annotations_clip_residentiel.csv'
BATCH_SIZE       = 16
EPOCHS           = 15
LR               = 1e-4
IMG_SIZE         = 224
TIMEOUT          = 6
DEVICE           = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def section(t): print('\n' + '='*60 + f'\n   {t}\n' + '='*60)
def log(m):     print(f'  {m}')

# ================================================================
# DATASET
# ================================================================

# Transformations pour EfficientNet
TRANSFORM_TRAIN = transforms.Compose([
    transforms.Resize((IMG_SIZE + 32, IMG_SIZE + 32)),
    transforms.RandomCrop(IMG_SIZE),
    transforms.RandomHorizontalFlip(),
    transforms.ColorJitter(brightness=0.2, contrast=0.2),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                         std =[0.229, 0.224, 0.225])
])

TRANSFORM_VAL = transforms.Compose([
    transforms.Resize((IMG_SIZE, IMG_SIZE)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                         std =[0.229, 0.224, 0.225])
])


def download_image(url, timeout=TIMEOUT):
    try:
        r = requests.get(str(url), timeout=timeout,
                         headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            return Image.open(BytesIO(r.content)).convert('RGB')
    except Exception:
        pass
    return None


class ImmobilierDataset(Dataset):
    def __init__(self, df, label_col, transform, cache_dir='img_cache'):
        self.df        = df.reset_index(drop=True)
        self.label_col = label_col
        self.transform = transform
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def __len__(self):
        return len(self.df)

    def __getitem__(self, idx):
        row   = self.df.iloc[idx]
        url   = str(row['image_url'])
        label = int(row[self.label_col])

        # Cache local pour eviter de re-telecharger
        cache_file = os.path.join(self.cache_dir, f'{idx}.jpg')
        if os.path.exists(cache_file):
            try:
                img = Image.open(cache_file).convert('RGB')
            except Exception:
                img = None
        else:
            img = download_image(url)
            if img is not None:
                img.save(cache_file)

        if img is None:
            img = Image.new('RGB', (IMG_SIZE, IMG_SIZE), color=(128, 128, 128))

        img_tensor = self.transform(img)
        return img_tensor, torch.tensor(label, dtype=torch.long)


# ================================================================
# MODELE EFFICIENTNET-B0
# ================================================================

def build_model(n_classes):
    """EfficientNet-B0 pre-entraine, derniere couche remplacee."""
    model = models.efficientnet_b0(weights='IMAGENET1K_V1')

    # Geler les premieres couches (features generiques)
    for param in model.parameters():
        param.requires_grad = False

    # Remplacer la derniere couche par notre classifieur
    in_features = model.classifier[1].in_features
    model.classifier = nn.Sequential(
        nn.Dropout(p=0.3),
        nn.Linear(in_features, 128),
        nn.ReLU(),
        nn.Dropout(p=0.2),
        nn.Linear(128, n_classes)
    )

    # Degeler les 3 derniers blocs + classifieur pour mieux apprendre
    for param in model.classifier.parameters():
        param.requires_grad = True
    for param in model.features[-3:].parameters():
        param.requires_grad = True

    return model.to(DEVICE)


# ================================================================
# ENTRAINEMENT
# ================================================================

def compute_class_weights(labels, n_classes):
    """Calcule les poids inversement proportionnels a la frequence."""
    counts = np.bincount(labels, minlength=n_classes).astype(float)
    counts = np.where(counts == 0, 1, counts)  # eviter division par 0
    weights = 1.0 / counts
    weights = weights / weights.sum() * n_classes  # normaliser
    log(f'  Class weights : {[round(w,2) for w in weights]}')
    return torch.tensor(weights, dtype=torch.float).to(DEVICE)


def train_model(model, train_loader, val_loader, task_name, epochs=EPOCHS,
                class_weights=None):
    criterion = nn.CrossEntropyLoss(weight=class_weights)
    optimizer = torch.optim.Adam(
        filter(lambda p: p.requires_grad, model.parameters()),
        lr=LR
    )
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=3, gamma=0.5)

    best_val_acc = 0
    best_model   = None

    for epoch in range(epochs):
        # ── Train ──────────────────────────────────────────────
        model.train()
        train_loss, train_correct, train_total = 0, 0, 0

        for imgs, labels in train_loader:
            imgs, labels = imgs.to(DEVICE), labels.to(DEVICE)
            optimizer.zero_grad()
            outputs = model(imgs)
            loss    = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            train_loss    += loss.item()
            preds          = outputs.argmax(dim=1)
            train_correct += (preds == labels).sum().item()
            train_total   += len(labels)

        # ── Validation ─────────────────────────────────────────
        model.eval()
        val_correct, val_total = 0, 0
        all_preds, all_labels  = [], []

        with torch.no_grad():
            for imgs, labels in val_loader:
                imgs, labels = imgs.to(DEVICE), labels.to(DEVICE)
                outputs      = model(imgs)
                preds        = outputs.argmax(dim=1)
                val_correct += (preds == labels).sum().item()
                val_total   += len(labels)
                all_preds.extend(preds.cpu().numpy())
                all_labels.extend(labels.cpu().numpy())

        train_acc = train_correct / train_total
        val_acc   = val_correct   / val_total

        log(f'Epoch {epoch+1:>2}/{epochs} | '
            f'Loss={train_loss/len(train_loader):.3f} | '
            f'Train={train_acc:.3f} | Val={val_acc:.3f}')

        if val_acc > best_val_acc:
            best_val_acc = val_acc
            best_model   = {k: v.clone() for k, v in model.state_dict().items()}

        scheduler.step()

    # Charger le meilleur modele
    model.load_state_dict(best_model)
    log(f'\nMeilleure validation accuracy : {best_val_acc:.4f}')
    return model, all_preds, all_labels


# ================================================================
# INFERENCE — scorer tout le dataset
# ================================================================

def score_dataset(model, df_full, label_col, n_classes, out_col):
    """
    Applique le modele fine-tune sur tout le dataset residentiel
    pour produire un score par annonce.
    """
    model.eval()
    scores = []

    dataset = ImmobilierDataset(df_full, label_col, TRANSFORM_VAL,
                                cache_dir='img_cache_full')

    loader = DataLoader(dataset, batch_size=32, shuffle=False, num_workers=0)

    with torch.no_grad():
        for imgs, _ in loader:
            imgs    = imgs.to(DEVICE)
            outputs = model(imgs)
            probs   = torch.softmax(outputs, dim=1).cpu().numpy()
            # Score = classe predite (0, 1, 2...)
            preds   = probs.argmax(axis=1)
            scores.extend(preds.tolist())

    df_full[out_col] = scores
    log(f'  {out_col} : {pd.Series(scores).value_counts().to_dict()}')
    return df_full


# ================================================================
# MAIN
# ================================================================

def run():
    section('FINETUNE_EFFICIENTNET.PY')
    log('Fine-tuning EfficientNet-B0 sur annotations validees')
    log(f'Device : {DEVICE}')

    # Charger annotations
    if not os.path.exists(ANNOTATIONS_FILE):
        log(f'ERREUR : {ANNOTATIONS_FILE} introuvable')
        log('Lancez d abord : python preannotate.py')
        return

    df = pd.read_csv(ANNOTATIONS_FILE, sep=';')
    if 'standing' not in df.columns:
        df = pd.read_csv(ANNOTATIONS_FILE)

    log(f'Annotations : {len(df)} images')
    log(f'Standing : {df["standing"].value_counts().to_dict()}')
    log(f'Etat     : {df["etat"].value_counts().to_dict()}')

    # ── TACHE 1 : Standing ──────────────────────────────────────
    section('TACHE 1 : STANDING (1=modeste / 2=standard / 3=luxe)')

    # Convertir labels 1/2/3 → 0/1/2 pour PyTorch
    df['standing_idx'] = df['standing'] - 1

    df_train, df_val = train_test_split(df, test_size=0.2,
                                        random_state=42,
                                        stratify=df['standing_idx'])
    log(f'Train={len(df_train)} | Val={len(df_val)}')

    train_ds = ImmobilierDataset(df_train, 'standing_idx', TRANSFORM_TRAIN)
    val_ds   = ImmobilierDataset(df_val,   'standing_idx', TRANSFORM_VAL)
    train_ld = DataLoader(train_ds, batch_size=BATCH_SIZE, shuffle=True,  num_workers=0)
    val_ld   = DataLoader(val_ds,   batch_size=BATCH_SIZE, shuffle=False, num_workers=0)

    model_standing = build_model(n_classes=3)
    # Class weights pour equilibrer modeste/standard/luxe
    standing_labels = df_train['standing_idx'].values
    w_standing = compute_class_weights(standing_labels, n_classes=3)
    log(f'  Poids standing : modeste={w_standing[0]:.2f} | standard={w_standing[1]:.2f} | luxe={w_standing[2]:.2f}')

    model_standing, preds, labels = train_model(
        model_standing, train_ld, val_ld, 'standing',
        class_weights=w_standing
    )

    # Rapport classification
    log('\nRapport Standing :')
    print(classification_report(labels, preds,
                                target_names=['modeste','standard','luxe'],
                                zero_division=0))

    torch.save(model_standing.state_dict(), 'efficientnet_standing.pth')
    log('Sauvegarde : efficientnet_standing.pth ✔')

    # ── TACHE 2 : Etat ──────────────────────────────────────────
    section('TACHE 2 : ETAT (0=ancien / 1=renove)')

    df_train2, df_val2 = train_test_split(df, test_size=0.2,
                                           random_state=42,
                                           stratify=df['etat'])
    log(f'Train={len(df_train2)} | Val={len(df_val2)}')

    train_ds2 = ImmobilierDataset(df_train2, 'etat', TRANSFORM_TRAIN)
    val_ds2   = ImmobilierDataset(df_val2,   'etat', TRANSFORM_VAL)
    train_ld2 = DataLoader(train_ds2, batch_size=BATCH_SIZE, shuffle=True,  num_workers=0)
    val_ld2   = DataLoader(val_ds2,   batch_size=BATCH_SIZE, shuffle=False, num_workers=0)

    model_etat = build_model(n_classes=2)
    # Class weights pour equilibrer ancien/renove
    etat_labels = df_train2['etat'].values
    w_etat = compute_class_weights(etat_labels, n_classes=2)
    log(f'  Poids etat : ancien={w_etat[0]:.2f} | renove={w_etat[1]:.2f}')

    model_etat, preds2, labels2 = train_model(
        model_etat, train_ld2, val_ld2, 'etat',
        class_weights=w_etat
    )

    log('\nRapport Etat :')
    print(classification_report(labels2, preds2,
                                target_names=['ancien','renove'],
                                zero_division=0))

    torch.save(model_etat.state_dict(), 'efficientnet_etat.pth')
    log('Sauvegarde : efficientnet_etat.pth ✔')

    section('TERMINE')
    log('Modeles sauvegardes :')
    log('  efficientnet_standing.pth')
    log('  efficientnet_etat.pth')
    log('')
    log('Prochaine etape : python score_dataset.py')
    log('  → applique les modeles sur tout le dataset (13,406 images)')
    log('  → produit standing_score et etat_score comme features')


if __name__ == '__main__':
    run()