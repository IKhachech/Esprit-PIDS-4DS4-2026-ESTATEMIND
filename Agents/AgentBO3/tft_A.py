"""
tft_A.py — TFT (Temporal Fusion Transformer) — Modèle causal
Usage : python tft_A.py
"""
import os, pickle, warnings
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import MinMaxScaler
warnings.filterwarnings('ignore')

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODELS_DIR, exist_ok=True)

# ── Hyperparamètres ────────────────────────────────────────────
SEQ_LEN      = 12
PRED_LEN     =  3
EPOCHS       = 80
BATCH_SIZE   = 16
HIDDEN       = 64
DROPOUT      = 0.3
LR           = 3e-4
WEIGHT_DECAY = 1e-3
PATIENCE     = 15
GRAD_CLIP    = 1.0


# ── Architecture TFT ───────────────────────────────────────────
class TFT(nn.Module):
    """
    Temporal Fusion Transformer simplifié :
      1. Variable Selection : chaque feature a un poids appris
         → le modèle apprend QUELLE variable est importante (inflation ? PIB ? saison ?)
      2. LSTM Encoder : capture le contexte temporel
      3. Multi-Head Attention : capture les relations entre pas de temps
      4. Gate + Norm : connexion résiduelle + normalisation
      5. Feed-Forward : 2 couches denses pour la prédiction finale
    """
    def __init__(self, n_feats, hidden, n_heads, pred_len, dropout):
        super().__init__()
        # Sélection de variables
        self.var_sel = nn.Sequential(nn.Linear(n_feats, n_feats), nn.Sigmoid())
        # Projection + encodeur
        self.proj    = nn.Linear(n_feats, hidden)
        self.lstm    = nn.LSTM(hidden, hidden, num_layers=2,
                                batch_first=True, dropout=dropout)
        # Attention causale
        self.attn    = nn.MultiheadAttention(hidden, n_heads,
                                              dropout=dropout, batch_first=True)
        self.gate    = nn.Linear(hidden, hidden)
        self.norm1   = nn.LayerNorm(hidden)
        self.norm2   = nn.LayerNorm(hidden)
        # Feed-Forward 2 couches
        self.ff      = nn.Sequential(
            nn.Linear(hidden, hidden * 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden * 2, hidden),
        )
        self.drop    = nn.Dropout(dropout)
        self.fc      = nn.Linear(hidden, pred_len)

    def forward(self, x):
        # Pondérer les features selon leur importance
        x    = x * self.var_sel(x)
        h    = self.proj(x)
        h, _ = self.lstm(h)
        # Self-attention + gating
        a, _ = self.attn(h, h, h)
        g    = torch.sigmoid(self.gate(h))
        h    = self.norm1(h + g * a)
        # Feed-Forward résiduel
        h    = self.norm2(h + self.ff(h))
        return self.fc(self.drop(h[:, -1, :]))


# ── Helpers ────────────────────────────────────────────────────

def preparer_serie(df, min_annonces=3):
    cols = ['indice_prix_m2_regional', 'inflation_glissement_annuel',
            'croissance_pib_trim', 'glissement_immo_trim',
            'high_season', 'type_transaction',
            'score_attractivite', 'nb_infra', 'nb_commerce']
    cols_ok = [c for c in cols if c in df.columns]

    grp = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg = grp[cols_ok].mean()
    cnt = grp.size().rename('n')
    agg = agg.join(cnt).reset_index()
    agg = agg[agg['n'] >= min_annonces].drop(columns='n')
    agg = agg.sort_values(['gouvernorat', 'annee', 'mois']).reset_index(drop=True)

    # TARGET lissée
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                            .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    # Features lag
    agg['prix_lag1']  = agg.groupby('gouvernorat')['prix_lisse'].shift(1)
    agg['prix_lag2']  = agg.groupby('gouvernorat')['prix_lisse'].shift(2)
    agg['prix_diff1'] = agg['prix_lisse'] - agg['prix_lag1']

    return agg.dropna(subset=['prix_lag1', 'prix_lag2']).reset_index(drop=True)


def creer_sequences(data, seq_len, pred_len):
    X, y = [], []
    for i in range(len(data) - seq_len - pred_len + 1):
        X.append(data[i : i + seq_len])
        y.append(data[i + seq_len : i + seq_len + pred_len, -1])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


def metriques(y_true, y_pred):
    rmse = float(np.sqrt(np.mean((y_true - y_pred)**2)))
    mae  = float(np.mean(np.abs(y_true - y_pred)))
    mape = float(np.mean(np.abs((y_true - y_pred) /
                  np.clip(np.abs(y_true), 1, None)))) * 100
    ss_r = np.sum((y_true - y_pred)**2)
    ss_t = np.sum((y_true - y_true.mean())**2)
    r2   = float(1 - ss_r / (ss_t + 1e-10))
    return {'rmse': round(rmse,2), 'mae': round(mae,2),
            'mape': round(mape,2), 'r2': round(r2,4)}


# ── Modèle principal ───────────────────────────────────────────

def run_tft(groupes):
    print("\n" + "="*55)
    print("  TFT — Temporal Fusion Transformer causal")
    print("="*55)

    FEATURES  = ['inflation_glissement_annuel', 'croissance_pib_trim',
                 'glissement_immo_trim', 'high_season', 'type_transaction',
                 'score_attractivite',
                 'prix_lag1', 'prix_lag2', 'prix_diff1',
                 'prix_lisse']   # TARGET en dernière position

    resultats = {}
    all_r2    = []
    device    = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"  Device : {device}")

    for groupe, df in groupes.items():
        if len(df) < 200:
            print(f"\n  {groupe} : {len(df)} lignes → skip")
            resultats[groupe] = {'status': 'skip'}
            continue

        seq_len = 6 if 'Commercial' in groupe or 'Divers' in groupe else SEQ_LEN
        print(f"\n  {groupe} : {len(df):,} lignes | SEQ_LEN={seq_len}")

        serie = preparer_serie(df)
        feats = [f for f in FEATURES if f in serie.columns]
        data  = serie[feats].values.astype(np.float32)

        if len(data) < seq_len + PRED_LEN + 10:
            print(f"  {groupe} : trop court → skip")
            continue

        # Filtre p5-p95 + log transform TARGET
        tgt = data[:, -1]
        p5, p95 = np.percentile(tgt, 5), np.percentile(tgt, 95)
        data    = data[(tgt >= p5) & (tgt <= p95)]
        data[:, -1] = np.log1p(data[:, -1])

        scaler = MinMaxScaler()
        data_n = scaler.fit_transform(data)

        X, y = creer_sequences(data_n, seq_len, PRED_LEN)
        if len(X) < 10:
            print(f"  {groupe} : séquences insuffisantes → skip")
            continue

        n_tr = int(len(X) * 0.70)
        n_vl = int(len(X) * 0.10)
        Xtr  = torch.FloatTensor(X[:n_tr]).to(device)
        ytr  = torch.FloatTensor(y[:n_tr]).to(device)
        Xvl  = torch.FloatTensor(X[n_tr:n_tr+n_vl]).to(device)
        yvl  = torch.FloatTensor(y[n_tr:n_tr+n_vl]).to(device)
        Xte  = torch.FloatTensor(X[n_tr+n_vl:]).to(device)
        yte  = torch.FloatTensor(y[n_tr+n_vl:]).to(device)

        loader = DataLoader(TensorDataset(Xtr, ytr),
                            batch_size=BATCH_SIZE, shuffle=True)

        n_heads = 4 if HIDDEN % 4 == 0 and len(df) >= 1000 else 2
        model   = TFT(len(feats), HIDDEN, n_heads, PRED_LEN, DROPOUT).to(device)
        optim   = torch.optim.AdamW(model.parameters(),
                                     lr=LR, weight_decay=WEIGHT_DECAY)
        # CosineAnnealing avec redémarrages
        sched   = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
            optim, T_0=20, T_mult=2)
        crit    = nn.HuberLoss(delta=0.5)

        best_val, pat, best_st = float('inf'), 0, None

        for epoch in range(EPOCHS):
            model.train()
            for xb, yb in loader:
                optim.zero_grad()
                loss = crit(model(xb), yb)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP)
                optim.step()
            sched.step()

            model.eval()
            with torch.no_grad():
                vl = crit(model(Xvl), yvl).item() if len(Xvl) > 0 else loss.item()

            if vl < best_val:
                best_val = vl
                pat      = 0
                best_st  = {k: v.clone() for k, v in model.state_dict().items()}
            else:
                pat += 1

            if (epoch + 1) % 20 == 0:
                print(f"    Epoch {epoch+1:>3}/{EPOCHS} | "
                      f"val={vl:.4f} | patience={pat}/{PATIENCE} | "
                      f"lr={optim.param_groups[0]['lr']:.6f}")
            if pat >= PATIENCE:
                print(f"    Early stopping à epoch {epoch+1}")
                break

        if best_st:
            model.load_state_dict(best_st)

        model.eval()
        with torch.no_grad():
            preds = model(Xte).cpu().numpy()
            trues = yte.cpu().numpy()

        idx    = feats.index('prix_lisse')
        sc_t   = MinMaxScaler().fit(data[:, idx].reshape(-1, 1))
        p_orig = np.expm1(sc_t.inverse_transform(preds.reshape(-1,1)).flatten())
        t_orig = np.expm1(sc_t.inverse_transform(trues.reshape(-1,1)).flatten())

        m = metriques(t_orig, p_orig)
        all_r2.append(m['r2'])

        print(f"  {groupe} → R²={m['r2']:.4f} | RMSE={m['rmse']:.2f} | "
              f"MAE={m['mae']:.2f} | MAPE={m['mape']:.1f}%")

        pt = os.path.join(MODELS_DIR, f'tft_{groupe}.pt')
        torch.save({'state_dict': model.state_dict(), 'scaler': scaler,
                    'feats': feats, 'n_heads': n_heads,
                    'seq_len': seq_len, 'p5': p5, 'p95': p95}, pt)
        print(f"    Modèle sauvegardé : {pt}")

        resultats[groupe] = {'status': 'trained', **m, 'model_path': pt}

    score  = float(np.mean(all_r2)) if all_r2 else 0.0
    chemin = os.path.join(MODELS_DIR, 'tft.pkl')
    with open(chemin, 'wb') as f:
        pickle.dump(resultats, f)

    print(f"\n  → R² moyen : {score:.4f}")
    print(f"  → Sauvegardé : {chemin}")
    return {'score': score, 'metric': 'r2', 'pkl': chemin}


if __name__ == '__main__':
    dossier = os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', '..', 'BO3'))
    print(f"Dossier : {dossier}")

    groupes = {}
    for nom in ['Residentiel', 'Foncier', 'Commercial', 'Divers']:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f"  ✔ {nom} : {len(groupes[nom]):,} lignes")
        else:
            print(f"  ✗ {nom} manquant")

    if 'Commercial' in groupes and 'Divers' in groupes:
        groupes['Commercial_Divers'] = pd.concat(
            [groupes.pop('Commercial'), groupes.pop('Divers')],
            ignore_index=True)
        print(f"  Commercial+Divers fusionnés : "
              f"{len(groupes['Commercial_Divers']):,} lignes")

    if groupes:
        run_tft(groupes)
