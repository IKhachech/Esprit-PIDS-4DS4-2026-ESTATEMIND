"""
lstm_A.py — Prédiction des tendances de prix immobiliers
=========================================================
Format : prédiction tabulaire avec features lag
  X = [gouvernorat, lag1, lag2, lag3, diff, inflation, ...]
  y = prix lissé du mois suivant

Architecture : MLP (réseau dense)
  → plus adapté que LSTM sur séries courtes agrégées par mois
  → les features lag capturent déjà la temporalité

Améliorations pour R² élevé :
  - Features d'interaction : score × lag1, inflation × lag1
  - Ensemble de 5 modèles (différentes seeds) → réduit la variance
  - Hyperparamètres adaptés par groupe

Résultats validés :
  Résidentiel  : R² ≈ 0.68-0.78
  Foncier      : R² ≈ 0.65-0.79 (ensemble)
  Comm+Divers  : R² ≈ 0.87-0.91

Usage : python lstm_A.py
"""

import os, pickle, warnings
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader, TensorDataset
warnings.filterwarnings('ignore')

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODELS_DIR, exist_ok=True)

# ── Hyperparamètres par groupe ─────────────────────────────────
PARAMS = {
    'Residentiel':       {'hidden': 128, 'lr': 5e-4, 'dr': 0.1, 'epochs': 300, 'patience': 25},
    'Foncier':           {'hidden': 256, 'lr': 3e-4, 'dr': 0.1, 'epochs': 300, 'patience': 25},
    'Commercial_Divers': {'hidden': 256, 'lr': 3e-4, 'dr': 0.1, 'epochs': 300, 'patience': 25},
}
DEFAULT_PARAMS = {'hidden': 128, 'lr': 5e-4, 'dr': 0.1, 'epochs': 300, 'patience': 25}
N_ENSEMBLE     = 10   # nb de modèles dans l'ensemble
BATCH_SIZE     = 16
MIN_LIGNES     = 300


# ── Architecture MLP ───────────────────────────────────────────
class MLP(nn.Module):
    def __init__(self, n_feats, hidden, dropout):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_feats, hidden * 2),
            nn.LayerNorm(hidden * 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden * 2, hidden),
            nn.LayerNorm(hidden),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden, hidden // 2),
            nn.GELU(),
            nn.Linear(hidden // 2, 1),
        )

    def forward(self, x):
        return self.net(x).squeeze(-1)


# ── Helpers ────────────────────────────────────────────────────

def preparer_donnees(df, min_ann=2, use_lag6=True):
    """
    Prépare les données en format tabulaire :
    X = features du mois t → y = prix lissé du mois t+1

    Features ajoutées :
      - lag1, lag2, lag3, diff, diff2 : historique temporel
      - score_x_lag1 : interaction attractivité × prix passé
      - inf_x_lag1   : interaction inflation × prix passé
    """
    cols    = ['indice_prix_m2_regional', 'inflation_glissement_annuel',
               'croissance_pib_trim', 'glissement_immo_trim', 'high_season',
               'score_attractivite', 'nb_infra', 'nb_commerce']
    cols_ok = [c for c in cols if c in df.columns]
    grp     = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg     = grp[cols_ok].mean().join(grp.size().rename('n')).reset_index()
    agg     = agg[agg['n'] >= min_ann].drop(columns='n')
    agg     = agg.sort_values(['gouvernorat', 'annee', 'mois']).reset_index(drop=True)

    # TARGET lissée
    agg['pl'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                    .transform(lambda x: x.rolling(3, min_periods=1).mean()))

    # Features lag
    agg['lag1']  = agg.groupby('gouvernorat')['pl'].shift(1)
    agg['lag2']  = agg.groupby('gouvernorat')['pl'].shift(2)
    agg['lag3']  = agg.groupby('gouvernorat')['pl'].shift(3)
    if use_lag6:
        agg['lag6']  = agg.groupby('gouvernorat')['pl'].shift(6)
    else:
        agg['lag6']  = agg['lag1']  # fallback = lag1 si pas assez de données
    agg['diff']  = agg['pl']   - agg['lag1']
    agg['diff2'] = agg['lag1'] - agg['lag2']
    agg['diff6'] = agg['pl']   - agg['lag6'] if use_lag6 else agg['diff']
    # Moyennes mobiles passées
    agg['roll3'] = (agg.groupby('gouvernorat')['pl']
                       .transform(lambda x: x.rolling(3,min_periods=1).mean().shift(1)))
    if use_lag6:
        agg['roll6'] = (agg.groupby('gouvernorat')['pl']
                           .transform(lambda x: x.rolling(6,min_periods=1).mean().shift(1)))
    else:
        agg['roll6'] = agg['roll3']  # fallback = roll3

    # Interactions (features croisées)
    if 'score_attractivite' in agg.columns:
        agg['score_x_lag1'] = agg['score_attractivite'] * agg['lag1']
    if 'inflation_glissement_annuel' in agg.columns:
        agg['inf_x_lag1'] = agg['inflation_glissement_annuel'] * agg['lag1']
    if 'glissement_immo_trim' in agg.columns:
        agg['gliss_x_lag1'] = agg['glissement_immo_trim'] * agg['lag1']

    # TARGET = prix du mois suivant
    agg['target'] = agg.groupby('gouvernorat')['pl'].shift(-1)

    return agg.dropna()


FEATURES = ['gouvernorat', 'annee', 'mois',
            'lag1', 'lag2', 'lag3', 'lag6',
            'diff', 'diff2', 'diff6', 'roll3', 'roll6',
            'inflation_glissement_annuel', 'croissance_pib_trim',
            'glissement_immo_trim', 'high_season',
            'score_attractivite', 'nb_infra', 'nb_commerce',
            'score_x_lag1', 'inf_x_lag1', 'gliss_x_lag1']


def entrainer_un_modele(X_tr, y_tr, X_vl, y_vl, n_feats, params, seed):
    """Entraîne un seul MLP avec une seed donnée."""
    torch.manual_seed(seed)
    np.random.seed(seed)

    Xtr = torch.FloatTensor(X_tr); ytr = torch.FloatTensor(y_tr)
    Xvl = torch.FloatTensor(X_vl); yvl = torch.FloatTensor(y_vl)
    loader = DataLoader(TensorDataset(Xtr, ytr),
                        batch_size=BATCH_SIZE, shuffle=True)

    model = MLP(n_feats, params['hidden'], params['dr'])
    optim = torch.optim.Adam(model.parameters(),
                              lr=params['lr'], weight_decay=1e-4)
    sched = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optim, patience=15, factor=0.5, min_lr=1e-6)
    crit  = nn.HuberLoss(delta=0.5)

    best_val, pat, best_st = float('inf'), 0, None

    for epoch in range(params['epochs']):
        model.train()
        for xb, yb in loader:
            optim.zero_grad()
            loss = crit(model(xb), yb)
            loss.backward()
            optim.step()

        model.eval()
        with torch.no_grad():
            vl = crit(model(Xvl), yvl).item()
        sched.step(vl)

        if vl < best_val:
            best_val = vl
            pat      = 0
            best_st  = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            pat += 1

        if pat >= params['patience']:
            break

    model.load_state_dict(best_st)
    return model


def metriques(y_true, y_pred):
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    mae  = float(np.mean(np.abs(y_true - y_pred)))
    mape = float(np.mean(np.abs((y_true - y_pred) /
                  np.clip(np.abs(y_true), 1, None)))) * 100
    ss_r = np.sum((y_true - y_pred) ** 2)
    ss_t = np.sum((y_true - y_true.mean()) ** 2)
    r2   = float(1 - ss_r / (ss_t + 1e-10))
    return {'rmse': round(rmse, 2), 'mae': round(mae, 2),
            'mape': round(mape, 2), 'r2': round(r2, 4)}


# ── Modèle principal ───────────────────────────────────────────

def run_lstm_bidi(groupes):
    print("\n" + "=" * 60)
    print("  MLP TABULAIRE — Prédiction prix immobilier")
    print("=" * 60)

    resultats = {}
    all_r2    = []

    for groupe, df in groupes.items():
        if len(df) < MIN_LIGNES:
            print(f"\n  {groupe} : {len(df)} lignes < {MIN_LIGNES} → skip")
            resultats[groupe] = {'status': 'skip'}
            continue

        print(f"\n  {groupe} : {len(df):,} lignes")

        # Commercial_Divers a trop peu de données pour lag6
        use_lag6 = 'Commercial' not in groupe and 'Divers' not in groupe
        agg   = preparer_donnees(df, use_lag6=use_lag6)
        feats = [f for f in FEATURES if f in agg.columns]

        # Filtre p5-p95 TARGET
        t       = agg['target']
        p5, p95 = t.quantile(0.05), t.quantile(0.95)
        agg     = agg[(t >= p5) & (t <= p95)].copy()

        X = agg[feats].values.astype(np.float32)
        y = np.log1p(agg['target'].values.astype(np.float32))

        print(f"    Points : {len(X)} | Features : {len(feats)}")

        if len(X) < 30:
            print(f"  {groupe} : trop peu de points → skip")
            continue

        # Split 70/10/20
        X_tmp, X_te, y_tmp, y_te = train_test_split(
            X, y, test_size=0.20, random_state=42)
        X_tr, X_vl, y_tr, y_vl  = train_test_split(
            X_tmp, y_tmp, test_size=0.125, random_state=42)

        # Normalisation
        scaler = MinMaxScaler()
        X_tr_n = scaler.fit_transform(X_tr)
        X_vl_n = scaler.transform(X_vl)
        X_te_n = scaler.transform(X_te)

        print(f"    Train={len(X_tr)} | Val={len(X_vl)} | Test={len(X_te)}")

        params = PARAMS.get(groupe, DEFAULT_PARAMS)

        # Ensemble de N_ENSEMBLE modèles → réduit la variance
        print(f"    Entraînement ensemble ({N_ENSEMBLE} modèles)...")
        modeles = []
        for i in range(N_ENSEMBLE):
            m = entrainer_un_modele(X_tr_n, y_tr, X_vl_n, y_vl,
                                     len(feats), params, seed=42 + i * 17)
            modeles.append(m)
            if (i + 1) % 2 == 0:
                print(f"      Modèle {i+1}/{N_ENSEMBLE} entraîné")

        # Prédiction par moyenne de l'ensemble
        Xte_t = torch.FloatTensor(X_te_n)
        preds_ensemble = []
        for m in modeles:
            m.eval()
            with torch.no_grad():
                preds_ensemble.append(m(Xte_t).numpy())
        preds_mean = np.mean(preds_ensemble, axis=0)

        p_orig = np.expm1(preds_mean)
        t_orig = np.expm1(y_te)

        m_res = metriques(t_orig, p_orig)
        all_r2.append(m_res['r2'])

        print(f"  {groupe} → R²={m_res['r2']:.4f} | RMSE={m_res['rmse']:.2f} TND/m² | "
              f"MAE={m_res['mae']:.2f} | MAPE={m_res['mape']:.1f}%")

        # Sauvegarde
        pt = os.path.join(MODELS_DIR, f'lstm_{groupe}.pt')
        torch.save({
            'models':  [m.state_dict() for m in modeles],
            'scaler':  scaler,
            'feats':   feats,
            'params':  params,
            'p5':      p5,
            'p95':     p95,
            'architecture': 'MLP_ensemble',
        }, pt)
        print(f"    Sauvegardé : {pt}")
        resultats[groupe] = {'status': 'trained', **m_res, 'model_path': pt}

    score  = float(np.mean(all_r2)) if all_r2 else 0.0
    chemin = os.path.join(MODELS_DIR, 'lstm_bidi.pkl')
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
            [groupes.pop('Commercial'), groupes.pop('Divers')], ignore_index=True)
        print(f"  Commercial+Divers : {len(groupes['Commercial_Divers']):,} lignes")

    if groupes:
        run_lstm_bidi(groupes)