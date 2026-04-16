"""
run_pipeline_final.py — Orchestrateur final · Pipeline BO3 complet
===================================================================

Exécute le pipeline dans l'ordre exact :
  1. KMeans   → cluster_map.pkl
  2. PELT     → pelt_A_rapport.csv
  3. LSTM     → modèles .keras (baseline)
  4. TFT      → tft_predictions.csv  (modèle principal)
  5. Score    → market_potential_scores.csv

Peut être lancé entièrement (mode full) ou par étapes (mode step).

Usage :
  python run_pipeline_final.py              # pipeline complet
  python run_pipeline_final.py --from tft   # depuis l'étape TFT (skip 1-3)
  python run_pipeline_final.py --step score # score engine seul
  python run_pipeline_final.py --summary    # résumé des artefacts existants
"""

import os
import sys
import argparse
import pickle
import warnings
import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR  = os.path.join(BASE_DIR, 'models')
DOSSIER_BO3 = os.path.normpath(os.path.join(BASE_DIR, '..', '..', 'BO3'))

os.makedirs(MODELS_DIR, exist_ok=True)

GROUPES_ACTIFS = ['Residentiel', 'Foncier', 'Commercial']

# ─── Couleurs console ─────────────────────────────────────────────────────────
GREEN  = '\033[92m'
YELLOW = '\033[93m'
RED    = '\033[91m'
CYAN   = '\033[96m'
RESET  = '\033[0m'
BOLD   = '\033[1m'

def ok(msg):    print(f"  {GREEN}✔{RESET} {msg}")
def warn(msg):  print(f"  {YELLOW}⚠{RESET}  {msg}")
def err(msg):   print(f"  {RED}✗{RESET} {msg}")
def info(msg):  print(f"  {CYAN}→{RESET} {msg}")
def header(msg):print(f"\n{BOLD}{'═'*68}\n  {msg}\n{'═'*68}{RESET}")


# ================================================================
# CHARGEMENT DONNÉES BO3
# ================================================================

def charger_donnees_bo3(dossier: str) -> dict:
    """Charge les Excel BO3 produits par pipeline_BO3.py."""
    groupes = {}
    for nom in GROUPES_ACTIFS:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            ok(f"{nom:<14} : {len(groupes[nom]):,} lignes")
        else:
            warn(f"{nom} introuvable : {f}")
    if not groupes:
        err("Aucun fichier BO3 trouvé. Exécutez d'abord pipeline_BO3.py")
        sys.exit(1)
    return groupes


# ================================================================
# ÉTAPE 1 — KMEANS
# ================================================================

def run_step_kmeans(groupes: dict) -> dict:
    header("ÉTAPE 1 — KMEANS : Segmentation géographique")
    try:
        from kmeans_A import run_kmeans
        result = run_kmeans(groupes)
        ok(f"KMeans terminé | Silhouette moyen : {result['score']:.4f}")
        ok(f"cluster_map.pkl → {result['cluster_map']}")
        with open(result['cluster_map'], 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        err(f"KMeans échoué : {e}")
        raise


# ================================================================
# ÉTAPE 2 — PELT
# ================================================================

def run_step_pelt(groupes: dict, cluster_map: dict) -> pd.DataFrame:
    header("ÉTAPE 2 — PELT : Détection de ruptures")
    try:
        from pelt_A import run_pelt
        result = run_pelt(groupes, cluster_map)
        ok(f"PELT terminé | {result['n_ruptures']} ruptures validées")
        ok(f"pelt_A_rapport.csv → {result['csv']}")
        return pd.read_csv(result['csv'])
    except Exception as e:
        err(f"PELT échoué : {e}")
        raise


# ================================================================
# ÉTAPE 3 — LSTM (baseline)
# ================================================================

def run_step_lstm(groupes: dict, cluster_map: dict,
                  pelt_df: pd.DataFrame) -> dict:
    header("ÉTAPE 3 — LSTM : Forecasting baseline")
    try:
        from lstm_A import run_lstm
        result = run_lstm(groupes, cluster_map, pelt_df)
        ok(f"LSTM terminé | {len(result)} groupes entraînés")
        return result
    except Exception as e:
        err(f"LSTM échoué : {e}")
        warn("Continuation sans LSTM baseline...")
        return {}


# ================================================================
# ÉTAPE 4 — TFT (modèle principal)
# ================================================================

def run_step_tft(groupes: dict, cluster_map: dict,
                 pelt_df: pd.DataFrame) -> pd.DataFrame:
    header("ÉTAPE 4 — TFT : Temporal Fusion Transformer")
    try:
        from tft_dataset_builder import build_tft_dataset, get_tft_column_groups
        from tft_model import TFTConfig, TFTModel, train_tft, predict_tft

        # ── Construire le dataset TFT ──────────────────────────────
        info("Construction du dataset TFT...")
        tft_df = build_tft_dataset(groupes, cluster_map, pelt_df)
        col_groups = get_tft_column_groups(tft_df)
        ok(f"Dataset TFT : {len(tft_df):,} lignes | "
           f"{tft_df['gouvernorat'].nunique()} gouvernorats")
        ok(f"rupture_flag : {int(tft_df['rupture_flag'].sum())} mois "
           f"({tft_df['rupture_flag'].mean()*100:.1f}%)")

        # Sauvegarder dataset TFT
        tft_pkl = os.path.join(MODELS_DIR, 'tft_dataset.pkl')
        with open(tft_pkl, 'wb') as f:
            pickle.dump({'df': tft_df, 'col_groups': col_groups}, f)
        ok(f"tft_dataset.pkl → {tft_pkl}")

        # ── Split temporel train / validation ──────────────────────
        train_cutoff = tft_df["time_idx"].quantile(0.8)
        train_df = tft_df[tft_df["time_idx"] <= train_cutoff].copy()
        val_df   = tft_df[tft_df["time_idx"] >  train_cutoff].copy()

        print(f"\n  {CYAN}→{RESET} Split temporel (80/20) :")
        print(f"      Train : {len(train_df):,} lignes "
              f"(time_idx ≤ {train_cutoff:.0f})")
        print(f"      Val   : {len(val_df):,} lignes "
              f"(time_idx > {train_cutoff:.0f})")

        if len(val_df) == 0:
            err("Validation set vide — vérifiez que time_idx couvre "
                "suffisamment de périodes (minimum 20% des time_idx)")
            raise ValueError("val_df est vide après le split temporel")

        # Vérifier que val couvre au moins encoder_length + decoder_length
        min_val_periods = 12 + 12  # encoder + decoder
        val_periods = val_df["time_idx"].nunique()
        if val_periods < min_val_periods:
            warn(f"Val set court ({val_periods} périodes uniques) — "
                 f"idéalement ≥ {min_val_periods}")

        # ── Entraîner TFT ──────────────────────────────────────────
        info("Entraînement TFT...")
        config = TFTConfig(
            encoder_length  = 12,
            decoder_length  = 12,
            d_model         = 64,
            num_heads       = 4,
            num_lstm_layers = 2,
            dropout         = 0.15,
            max_epochs      = 100,
            patience        = 15,
            batch_size      = 32,
        )

        model, history = train_tft(train_df, val_df, config, MODELS_DIR)
        ok(f"TFT entraîné | best_epoch={history['best_epoch']} | "
           f"val_loss={min(history['val_loss']):.4f}")

        if 'coverage_80' in history and history['coverage_80']:
            last_cov = history['coverage_80'][-1]
            status = GREEN if 0.60 <= last_cov <= 0.90 else YELLOW
            print(f"  {status}Coverage P10-P90 : {last_cov*100:.0f}% "
                  f"(idéale 60-90%){RESET}")

        # ── Prédictions ────────────────────────────────────────────
        info("Génération des prédictions TFT...")
        meta_path = os.path.join(MODELS_DIR, 'tft_meta.pkl')
        with open(meta_path, 'rb') as f:
            meta = pickle.load(f)

        preds = predict_tft(model, tft_df, config,
                             meta['scalers'], meta['encoders'])

        pred_csv = os.path.join(MODELS_DIR, 'tft_predictions.csv')
        preds.to_csv(pred_csv, index=False)
        ok(f"tft_predictions.csv → {pred_csv}")

        # Résumé prédictions
        print("\n  Prédictions TFT (médiane par horizon) :")
        print(f"  {'Groupe':<14} {'Horizon':<8} "
              f"{'P50 (TND/m²)':>13} {'Incertitude':>12}")
        print(f"  {'─'*50}")
        for (grp, hor), sub in preds.groupby(['groupe', 'horizon']):
            print(f"  {grp:<14} {hor:<8} "
                  f"{sub['q50_tnd'].mean():>13,.0f} "
                  f"±{sub['incertitude'].mean():>10,.0f}")

        # Sauvegarder aussi le dataset TFT enrichi
        tft_enrichi_pkl = os.path.join(MODELS_DIR, 'tft_dataset_enrichi.pkl')
        with open(tft_enrichi_pkl, 'wb') as f:
            pickle.dump(tft_df, f)

        return preds

    except Exception as e:
        import traceback
        err(f"TFT échoué : {e}")
        traceback.print_exc()
        warn("Continuation sans prédictions TFT...")
        return pd.DataFrame()


# ================================================================
# ÉTAPE 5 — SCORE ENGINE
# ================================================================

def run_step_score(groupes       : dict,
                   cluster_map   : dict,
                   pelt_df       : pd.DataFrame,
                   tft_preds     : pd.DataFrame,
                   lstm_results  : dict) -> pd.DataFrame:
    header("ÉTAPE 5 — SCORE ENGINE : Market Potential Score")
    try:
        from score_engine import ScoreEngine

        # Charger le dataset TFT si disponible
        tft_enrichi_pkl = os.path.join(MODELS_DIR, 'tft_dataset_enrichi.pkl')
        tft_dataset_pkl = os.path.join(MODELS_DIR, 'tft_dataset.pkl')

        if os.path.exists(tft_enrichi_pkl):
            with open(tft_enrichi_pkl, 'rb') as f:
                tft_df = pickle.load(f)
        elif os.path.exists(tft_dataset_pkl):
            with open(tft_dataset_pkl, 'rb') as f:
                tft_df = pickle.load(f)['df']
        else:
            # Fallback : construire dataset minimal depuis les données BO3
            warn("Dataset TFT non trouvé — construction depuis BO3...")
            from tft_dataset_builder import build_tft_dataset
            tft_df = build_tft_dataset(groupes, cluster_map, pelt_df)

        engine = ScoreEngine(models_dir=MODELS_DIR)
        scores = engine.compute(
            tft_df       = tft_df,
            cluster_map  = cluster_map,
            pelt_df      = pelt_df,
            tft_preds    = tft_preds,
            lstm_results = lstm_results,
        )

        engine.print_report(scores, top_n=5)
        score_path = engine.save(scores, MODELS_DIR)
        ok(f"market_potential_scores.csv → {score_path}")

        # ── Résumé quadrants ──────────────────────────────────────
        if 'quadrant' in scores.columns:
            q_counts = scores['quadrant'].value_counts()
            print("\n  Distribution quadrants :")
            for q, n in q_counts.items():
                print(f"    {q:<28} : {n} entrées")

        # ── Top 3 opportunités premium ────────────────────────────
        premium = scores[scores.get('quadrant', pd.Series()) == '🏆 Opportunité premium'] \
                  if 'quadrant' in scores.columns else pd.DataFrame()
        if not premium.empty:
            print(f"\n  {GREEN}TOP OPPORTUNITÉS PREMIUM :{RESET}")
            top = (premium.groupby(['gouvernorat', 'nom'])
                          ['invest_score'].mean()
                          .sort_values(ascending=False)
                          .head(3))
            for (gov, nom), sc in top.items():
                print(f"    🏆 {nom:<16} invest={sc:.3f}")

        return scores

    except Exception as e:
        import traceback
        err(f"Score Engine échoué : {e}")
        traceback.print_exc()
        return pd.DataFrame()


# ================================================================
# RÉSUMÉ DES ARTEFACTS
# ================================================================

def print_summary():
    """Affiche l'état des artefacts existants dans models/."""
    header("RÉSUMÉ DES ARTEFACTS PIPELINE BO3")

    artefacts = [
        ('cluster_map.pkl',              'KMeans — segmentation',         'étape 1'),
        ('kmeans_A.pkl',                 'KMeans — métriques',             'étape 1'),
        ('pelt_A.pkl',                   'PELT — ruptures (pkl)',           'étape 2'),
        ('pelt_A_rapport.csv',           'PELT — rapport ruptures',        'étape 2'),
        ('lstm_A_scalers.pkl',           'LSTM — scalers',                 'étape 3'),
        ('tft_dataset.pkl',              'TFT — dataset structuré',        'étape 4'),
        ('tft_model.pt',                 'TFT — poids modèle',             'étape 4'),
        ('tft_meta.pkl',                 'TFT — config + scalers',         'étape 4'),
        ('tft_predictions.csv',          'TFT — prédictions',              'étape 4'),
        ('market_potential_scores.csv',  'Score — potentiel final',        'étape 5'),
    ]

    all_ok = True
    for fname, desc, step in artefacts:
        path = os.path.join(MODELS_DIR, fname)
        if os.path.exists(path):
            size = os.path.getsize(path) / 1024
            ok(f"[{step}] {fname:<38} {desc:<30} ({size:.0f} KB)")
        else:
            warn(f"[{step}] {fname:<38} {desc:<30} MANQUANT")
            all_ok = False

    if all_ok:
        print(f"\n  {GREEN}{BOLD}Pipeline complet ✔ — tous les artefacts présents{RESET}")
    else:
        print(f"\n  {YELLOW}Pipeline incomplet — exécutez run_pipeline_final.py{RESET}")

    # Charge et affiche le classement si disponible
    score_csv = os.path.join(MODELS_DIR, 'market_potential_scores.csv')
    if os.path.exists(score_csv):
        scores = pd.read_csv(score_csv)
        print(f"\n  Classement global (invest_score moyen) :")
        if 'invest_score' in scores.columns:
            top = (scores.groupby(['gouvernorat', 'nom'])
                         ['invest_score'].mean()
                         .sort_values(ascending=False)
                         .head(5))
            for i, ((gov, nom), sc) in enumerate(top.items(), 1):
                bar = '█' * int(sc * 20)
                print(f"    {i}. {nom:<16} {sc:.3f}  {bar}")


# ================================================================
# PIPELINE COMPLET
# ================================================================

def run_full_pipeline(start_from: str = 'kmeans') -> None:
    """
    Exécute le pipeline depuis start_from.

    start_from : 'kmeans' | 'pelt' | 'lstm' | 'tft' | 'score'
    """
    STEPS = ['kmeans', 'pelt', 'lstm', 'tft', 'score']
    start_idx = STEPS.index(start_from) if start_from in STEPS else 0

    header("PIPELINE BO3 COMPLET — KMeans → PELT → LSTM → TFT → Score")
    print(f"  Démarrage depuis : {start_from.upper()}")

    # ── Chargement données ────────────────────────────────────────
    info(f"Chargement données BO3 depuis : {DOSSIER_BO3}")
    groupes = charger_donnees_bo3(DOSSIER_BO3)

    # ── Chargement artefacts existants (pour skip) ────────────────
    cluster_map  = {}
    pelt_df      = pd.DataFrame()
    lstm_results = {}
    tft_preds    = pd.DataFrame()

    if start_idx > 0:
        cm_path = os.path.join(MODELS_DIR, 'cluster_map.pkl')
        if os.path.exists(cm_path):
            with open(cm_path, 'rb') as f:
                cluster_map = pickle.load(f)
            ok(f"cluster_map.pkl chargé ({sum(len(v) for v in cluster_map.values())} govs)")
        else:
            err("cluster_map.pkl introuvable → relancez depuis kmeans")
            sys.exit(1)

    if start_idx > 1:
        pelt_csv = os.path.join(MODELS_DIR, 'pelt_A_rapport.csv')
        if os.path.exists(pelt_csv):
            pelt_df = pd.read_csv(pelt_csv)
            ok(f"pelt_A_rapport.csv chargé ({len(pelt_df)} ruptures)")
        else:
            warn("pelt_A_rapport.csv introuvable → PELT sera ignoré")

    if start_idx > 3:
        tft_pred_csv = os.path.join(MODELS_DIR, 'tft_predictions.csv')
        if os.path.exists(tft_pred_csv):
            tft_preds = pd.read_csv(tft_pred_csv)
            ok(f"tft_predictions.csv chargé ({len(tft_preds)} prédictions)")
        else:
            warn("tft_predictions.csv introuvable → score sans TFT")

    # ── Exécution des étapes ──────────────────────────────────────
    if start_idx <= 0:
        cluster_map = run_step_kmeans(groupes)

    if start_idx <= 1:
        pelt_df = run_step_pelt(groupes, cluster_map)

    if start_idx <= 2:
        lstm_results = run_step_lstm(groupes, cluster_map, pelt_df)

    if start_idx <= 3:
        tft_preds = run_step_tft(groupes, cluster_map, pelt_df)

    if start_idx <= 4:
        scores = run_step_score(groupes, cluster_map, pelt_df,
                                tft_preds, lstm_results)

    # ── Résumé final ──────────────────────────────────────────────
    print_summary()

    header("PIPELINE TERMINÉ")
    ok("Tous les artefacts ont été générés dans models/")
    info("Prochaines étapes :")
    print("    python run_pipeline_final.py --summary     # vérifier les artefacts")
    print("    python run_pipeline_final.py --step score  # re-scorer seul")


# ================================================================
# POINT D'ENTRÉE
# ================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Pipeline BO3 — KMeans → PELT → LSTM → TFT → Score')
    parser.add_argument('--from',   dest='start_from', default='kmeans',
                        choices=['kmeans','pelt','lstm','tft','score'],
                        help='Démarrer depuis cette étape (défaut: kmeans)')
    parser.add_argument('--step',   dest='step', default=None,
                        choices=['kmeans','pelt','lstm','tft','score'],
                        help='Exécuter une seule étape')
    parser.add_argument('--summary', action='store_true',
                        help='Afficher le résumé des artefacts existants')
    args = parser.parse_args()

    if args.summary:
        print_summary()
        sys.exit(0)

    if args.step:
        # Mode étape unique
        groupes    = charger_donnees_bo3(DOSSIER_BO3)
        cluster_map = {}
        pelt_df     = pd.DataFrame()
        tft_preds   = pd.DataFrame()

        cm_path = os.path.join(MODELS_DIR, 'cluster_map.pkl')
        if os.path.exists(cm_path):
            with open(cm_path, 'rb') as f:
                cluster_map = pickle.load(f)

        pelt_csv = os.path.join(MODELS_DIR, 'pelt_A_rapport.csv')
        if os.path.exists(pelt_csv):
            pelt_df = pd.read_csv(pelt_csv)

        tft_pred_csv = os.path.join(MODELS_DIR, 'tft_predictions.csv')
        if os.path.exists(tft_pred_csv):
            tft_preds = pd.read_csv(tft_pred_csv)

        if   args.step == 'kmeans': run_step_kmeans(groupes)
        elif args.step == 'pelt':   run_step_pelt(groupes, cluster_map)
        elif args.step == 'lstm':   run_step_lstm(groupes, cluster_map, pelt_df)
        elif args.step == 'tft':    run_step_tft(groupes, cluster_map, pelt_df)
        elif args.step == 'score':  run_step_score(groupes, cluster_map, pelt_df,
                                                    tft_preds, {})
    else:
        run_full_pipeline(start_from=args.start_from)
