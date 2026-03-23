"""
agent_collecte.py — Agent de Collecte Multi-Sources Intelligent
================================================================

Objectif : Pipeline automatique de collecte, nettoyage et fusion
           de données immobilières tunisiennes multi-sources.

Fonctionnalités :
  1. Scheduler  — lance le pipeline automatiquement (nuit, ou manuellement)
  2. Dedup global — détecte les nouvelles annonces vs déjà vues (SQLite)
  3. Pipeline    — appelle ton pipeline.py existant sur les nouvelles données
  4. Monitoring  — rapport de run + alertes si anomalie
  5. Export delta — fichier Excel des nouvelles annonces seulement

Usage :
  # Lancement manuel immédiat
  python agent_collecte.py --run

  # Lancement planifié chaque nuit à 02h00
  python agent_collecte.py --schedule

  # Voir le rapport des derniers runs
  python agent_collecte.py --report

  # Voir uniquement les nouvelles annonces depuis le dernier run
  python agent_collecte.py --delta

Structure fichiers générés :
  data/annonces_raw.db          SQLite — historique de toutes les annonces vues
  data/run_log.json             Journal des runs (date, nb annonces, erreurs)
  data/nouvelles_annonces.xlsx  Delta — annonces nouvelles depuis dernier run
  data/agent_config.json        Configuration de l'agent
"""

import os
import sys
import json
import time
import sqlite3
import hashlib
import logging
import argparse
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / 'data'
DB_PATH     = DATA_DIR / 'annonces_raw.db'
LOG_PATH    = DATA_DIR / 'run_log.json'
DELTA_PATH  = DATA_DIR / 'nouvelles_annonces.xlsx'
CONFIG_PATH = DATA_DIR / 'agent_config.json'

DATA_DIR.mkdir(exist_ok=True)

DEFAULT_CONFIG = {
    'schedule_hour':    2,       # heure de lancement automatique (02h00)
    'schedule_minute':  0,
    'min_new_annonces': 10,      # alerte si moins de X nouvelles annonces
    'max_error_pct':    20.0,    # alerte si plus de X% d'erreurs dans le run
    'sources': [
        {'name': 'Facebook Marketplace', 'path': '../marketplace/data/facebook_marketplace_2185_annonces_20260216_222412.csv', 'type': 'csv', 'sep': None},
        {'name': 'Mubawab',              'path': '../mubawab/mubawab_annonces.csv',                                            'type': 'csv', 'sep': None},
        {'name': 'Mubawab Partial',      'path': '../mubawab2/mubawab_partial_120.xlsx',                                       'type': 'xlsx','sep': None},
        {'name': 'Tayara',               'path': '../tayara/tayara_complete.csv',                                              'type': 'csv', 'sep': None},
        {'name': 'Tunisie Annonces',     'path': '../tunisie_annance_scraper/ta_properties.csv',                               'type': 'csv', 'sep': None},
        {'name': 'Century21',            'path': '../scrapping/century21_data2.csv',                                           'type': 'csv', 'sep': ';'},
        {'name': 'HomeInTunisia',        'path': '../scrapping/homeintunisia_data2.csv',                                       'type': 'csv', 'sep': ';'},
        {'name': 'BnB',                  'path': '../bnb/bnb_properties.csv',                                                  'type': 'csv', 'sep': None},
    ]
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DATA_DIR / 'agent.log', encoding='utf-8'),
    ]
)
log = logging.getLogger('agent')


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding='utf-8') as f:
            cfg = json.load(f)
        # Merge with defaults (add missing keys)
        for k, v in DEFAULT_CONFIG.items():
            if k not in cfg:
                cfg[k] = v
        return cfg
    save_config(DEFAULT_CONFIG)
    return DEFAULT_CONFIG.copy()


def save_config(cfg: dict) -> None:
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# BASE DE DONNÉES SQLITE — HISTORIQUE GLOBAL
# ─────────────────────────────────────────────────────────────────────────────

def init_db() -> sqlite3.Connection:
    """
    Crée/ouvre la base SQLite.
    Table annonces : une ligne par annonce unique vue.
    Clé de déduplication : hash SHA256 de (url + description + prix).
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS annonces (
            hash        TEXT PRIMARY KEY,
            source      TEXT,
            url         TEXT,
            prix        REAL,
            ville       TEXT,
            gouvernorat TEXT,
            type_bien   TEXT,
            surface_m2  REAL,
            date_vu     TEXT,
            run_id      TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS runs (
            run_id          TEXT PRIMARY KEY,
            date_debut      TEXT,
            date_fin        TEXT,
            nb_brut         INTEGER,
            nb_nouveaux     INTEGER,
            nb_doublons     INTEGER,
            nb_erreurs      INTEGER,
            sources_ok      TEXT,
            sources_erreur  TEXT,
            statut          TEXT
        )
    ''')
    conn.commit()
    return conn


def compute_hash(row: pd.Series) -> str:
    """Calcule un identifiant unique pour une annonce."""
    url   = str(row.get('url',         '') or '')
    desc  = str(row.get('description', '') or '')[:200]
    prix  = str(row.get('prix',        '') or '')
    ville = str(row.get('ville',       '') or '')
    raw   = f"{url}|{desc}|{prix}|{ville}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]


def get_known_hashes(conn: sqlite3.Connection) -> set:
    """Retourne l'ensemble des hashes déjà vus."""
    cur = conn.execute('SELECT hash FROM annonces')
    return {row[0] for row in cur.fetchall()}


def _safe_float(val):
    """Convertit en float de façon robuste — gère '475 000 TND', None, NaN."""
    if val is None: return None
    try:
        if pd.isna(val): return None
    except: pass
    try:
        return float(val)
    except (ValueError, TypeError):
        import re as _re
        s = _re.sub(r'[^\d.,]', '', str(val)).replace(',', '.')
        parts = s.split('.')
        if len(parts) > 2:
            s = ''.join(parts[:-1]) + '.' + parts[-1]
        try: return float(s) if s else None
        except ValueError: return None


def insert_new_annonces(conn: sqlite3.Connection, df: pd.DataFrame,
                        known_hashes: set, run_id: str) -> tuple[pd.DataFrame, int]:
    """
    Filtre les nouvelles annonces (pas dans known_hashes).
    Insère les nouvelles dans la DB.
    Retourne (df_nouvelles, nb_doublons).
    """
    df = df.copy()
    df['_hash'] = df.apply(compute_hash, axis=1)

    nouvelles = df[~df['_hash'].isin(known_hashes)].copy()
    doublons  = len(df) - len(nouvelles)

    now = datetime.now().isoformat()
    rows = []
    for _, r in nouvelles.iterrows():
        rows.append((
            r['_hash'],
            str(r.get('source',      '') or ''),
            str(r.get('url',         '') or ''),
            _safe_float(r.get('prix')),
            str(r.get('ville',       '') or ''),
            str(r.get('gouvernorat', '') or ''),
            str(r.get('type_bien',   '') or ''),
            _safe_float(r.get('surface_m2')),
            now,
            run_id
        ))

    if rows:
        conn.executemany('''
            INSERT OR IGNORE INTO annonces
            (hash, source, url, prix, ville, gouvernorat, type_bien, surface_m2, date_vu, run_id)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        ''', rows)
        conn.commit()

    nouvelles = nouvelles.drop(columns=['_hash'], errors='ignore')
    return nouvelles, doublons


# ─────────────────────────────────────────────────────────────────────────────
# CHARGEMENT DES SOURCES
# ─────────────────────────────────────────────────────────────────────────────

def load_sources(cfg: dict) -> tuple[pd.DataFrame, list, list]:
    """
    Charge toutes les sources configurées.
    Retourne (df_fusionne, sources_ok, sources_erreur).
    """
    dfs         = []
    sources_ok  = []
    sources_err = []

    for src in cfg['sources']:
        name = src['name']
        path = Path(src['path'])
        if not path.exists():
            log.warning(f"  Source absente : {name} ({path})")
            sources_err.append(name)
            continue
        try:
            if src['type'] == 'xlsx':
                df = pd.read_excel(path)
            else:
                sep = src.get('sep') or ','
                df  = pd.read_csv(path, sep=sep, on_bad_lines='skip')
            df['source'] = name
            dfs.append(df)
            sources_ok.append(name)
            log.info(f"  OK  {name:<25} : {len(df):>6} lignes")
        except Exception as e:
            log.error(f"  ERR {name}: {e}")
            sources_err.append(name)

    if not dfs:
        return pd.DataFrame(), sources_ok, sources_err

    df_all = pd.concat(dfs, ignore_index=True)
    log.info(f"  Total brut fusionné : {len(df_all):,} lignes")
    return df_all, sources_ok, sources_err


# ─────────────────────────────────────────────────────────────────────────────
# RUN LOG
# ─────────────────────────────────────────────────────────────────────────────

def load_run_log() -> list:
    if LOG_PATH.exists():
        with open(LOG_PATH, encoding='utf-8') as f:
            return json.load(f)
    return []


def save_run_log(runs: list) -> None:
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        json.dump(runs[-50:], f, ensure_ascii=False, indent=2)  # garde les 50 derniers


def log_run(run_id: str, conn: sqlite3.Connection,
            date_debut: str, date_fin: str,
            nb_brut: int, nb_nouveaux: int, nb_doublons: int,
            nb_erreurs: int, sources_ok: list, sources_err: list,
            statut: str) -> None:
    """Enregistre le résumé d'un run dans SQLite et JSON."""
    conn.execute('''
        INSERT OR REPLACE INTO runs
        (run_id, date_debut, date_fin, nb_brut, nb_nouveaux, nb_doublons,
         nb_erreurs, sources_ok, sources_erreur, statut)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    ''', (run_id, date_debut, date_fin, nb_brut, nb_nouveaux, nb_doublons,
          nb_erreurs, json.dumps(sources_ok), json.dumps(sources_err), statut))
    conn.commit()

    runs = load_run_log()
    runs.append({
        'run_id':      run_id,
        'date_debut':  date_debut,
        'date_fin':    date_fin,
        'nb_brut':     nb_brut,
        'nb_nouveaux': nb_nouveaux,
        'nb_doublons': nb_doublons,
        'nb_erreurs':  nb_erreurs,
        'sources_ok':  sources_ok,
        'sources_err': sources_err,
        'statut':      statut,
    })
    save_run_log(runs)


# ─────────────────────────────────────────────────────────────────────────────
# MONITORING & ALERTES
# ─────────────────────────────────────────────────────────────────────────────

def check_anomalies(cfg: dict, nb_nouveaux: int, nb_erreurs: int,
                    nb_brut: int, sources_err: list) -> list:
    """
    Vérifie les anomalies après un run.
    Retourne une liste de messages d'alerte.
    """
    alertes = []

    if nb_nouveaux < cfg['min_new_annonces']:
        alertes.append(
            f"ALERTE : seulement {nb_nouveaux} nouvelles annonces "
            f"(seuil min = {cfg['min_new_annonces']}). "
            f"Vérifier si les scrapers sont toujours actifs."
        )

    if nb_brut > 0:
        err_pct = nb_erreurs / nb_brut * 100
        if err_pct > cfg['max_error_pct']:
            alertes.append(
                f"ALERTE : {err_pct:.1f}% d'erreurs de parsing "
                f"(seuil = {cfg['max_error_pct']}%)."
            )

    if sources_err:
        alertes.append(
            f"ALERTE : {len(sources_err)} source(s) inaccessible(s) : "
            f"{', '.join(sources_err)}"
        )

    return alertes


def print_monitoring_report() -> None:
    """Affiche le rapport des derniers runs."""
    runs = load_run_log()
    if not runs:
        print("\n  Aucun run enregistré.")
        return

    print("\n" + "="*65)
    print("   RAPPORT DES DERNIERS RUNS")
    print("="*65)
    for r in reversed(runs[-10:]):
        statut_sym = '✔' if r['statut'] == 'OK' else '✗'
        print(f"\n  {statut_sym} Run {r['run_id']}")
        print(f"    Début    : {r['date_debut']}")
        print(f"    Fin      : {r['date_fin']}")
        print(f"    Brut     : {r['nb_brut']:,}")
        print(f"    Nouveaux : {r['nb_nouveaux']:,}  |  Doublons : {r['nb_doublons']:,}")
        print(f"    Sources OK   : {', '.join(r['sources_ok'])}")
        if r['sources_err']:
            print(f"    Sources ERR  : {', '.join(r['sources_err'])}")

    # Stats globales depuis la DB
    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        total = conn.execute('SELECT COUNT(*) FROM annonces').fetchone()[0]
        first = conn.execute('SELECT MIN(date_vu) FROM annonces').fetchone()[0]
        conn.close()
        print(f"\n  Total annonces en base : {total:,}")
        print(f"  Première collecte      : {first or 'N/A'}")
    print("="*65)


# ─────────────────────────────────────────────────────────────────────────────
# RUN PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_once(cfg: dict) -> dict:
    """
    Exécute un cycle complet de collecte :
    1. Charge toutes les sources
    2. Déduplique globalement (nouvelles vs déjà vues)
    3. Lance pipeline.py sur les nouvelles annonces uniquement
    4. Exporte le delta
    5. Contrôle les anomalies
    Retourne un dict résumé du run.
    """
    run_id    = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_deb  = datetime.now().isoformat()

    print("\n" + "="*65)
    print(f"   AGENT COLLECTE — Run {run_id}")
    print("="*65)

    conn = init_db()
    known_hashes = get_known_hashes(conn)
    log.info(f"  Annonces déjà en base : {len(known_hashes):,}")

    # ── 1. Chargement sources ────────────────────────────────────
    print("\n─── Chargement des sources ───────────────────────────────")
    df_brut, sources_ok, sources_err = load_sources(cfg)
    nb_brut = len(df_brut)

    if df_brut.empty:
        log.error("  Aucune donnée chargée — run interrompu.")
        log_run(run_id, conn, date_deb, datetime.now().isoformat(),
                0, 0, 0, 0, sources_ok, sources_err, 'ERREUR')
        conn.close()
        return {'statut': 'ERREUR', 'nb_nouveaux': 0}

    # ── 2. Déduplication globale ─────────────────────────────────
    print("\n─── Déduplication globale ────────────────────────────────")
    df_nouvelles, nb_doublons = insert_new_annonces(
        conn, df_brut, known_hashes, run_id
    )
    nb_nouveaux = len(df_nouvelles)
    log.info(f"  Nouvelles annonces : {nb_nouveaux:,}")
    log.info(f"  Doublons filtrés   : {nb_doublons:,}")

    # ── 3. Pipeline nettoyage — appelle pipeline.py directement ──
    nb_erreurs = 0
    df_clean   = pd.DataFrame()

    if nb_nouveaux > 0:
        print("\n─── Pipeline de nettoyage ────────────────────────────────")
        try:
            import importlib, sys as _sys

            # Ajouter le dossier courant au path
            _sys.path.insert(0, str(BASE_DIR))

            # Recharger les modules proprement
            for mod in ['cleaning', 'modeling', 'mappings', 'external_data', 'pipeline']:
                if mod in _sys.modules:
                    importlib.reload(_sys.modules[mod])

            # Importer pipeline et ses dépendances
            import pipeline as _pipeline_mod

            # Récupérer les chemins BCT/INS/Signaux depuis pipeline.py
            bct_path     = _pipeline_mod.BCT_PATH
            ins_path     = _pipeline_mod.INS_PATH
            signaux_path = _pipeline_mod.SIGNAUX_PATH
            sat_dir      = _pipeline_mod.SATELLITE_DIR

            import external_data as ext
            import cleaning      as clng
            import modeling      as mdlg

            # Charger les données externes avec les bons chemins
            external       = ext.load_all(bct_path=bct_path, ins_path=ins_path, sig_path=signaux_path)
            market_context = external['market_context']
            nego_rates     = external['nego_rates']
            gov_features   = external['gov_features']
            bct_info       = external['bct']

            # ── ETL complet sur les nouvelles annonces ────────────
            # On passe par le pipeline complet avec les sources originales
            # pour que le nettoyage (clean_price, clean_surface, etc.) soit fait
            sources_config = [(s['name'], s['path'], s['type'], s.get('sep'))
                              for s in cfg['sources']]

            df_raw, n0 = clng.load_sources(sources_config)
            if df_raw.empty:
                raise ValueError("Aucune donnée chargée depuis les sources")

            df_raw, _ = clng.deduplicate(df_raw)
            df_raw    = clng.coerce_types(df_raw)
            df_raw    = clng.standardize(df_raw)
            df_raw    = clng.segment(df_raw)

            groupes = clng.clean_groups(df_raw)
            groupes = clng.handle_missing(groupes)

            df_raw, groupes = clng.encode_categorical(df_raw, groupes)
            df_raw, groupes, rank_map, rank_map_full = clng.encode_ville(df_raw, groupes, min_freq=30)

            gov_coords = clng.build_gouvernorat_coords(sat_dir)
            df_raw     = clng.geocode(df_raw, gov_coords)
            df_raw, groupes = clng.reverse_geocode_correction(df_raw, groupes)

            # Rebuild df from clean groups
            import pandas as _pd
            df_clean = _pd.concat(groupes.values(), ignore_index=False)
            _valid_idx = [i for i in df_clean.index if i in df_raw.index]
            for _col in df_raw.columns:
                if _col not in df_clean.columns and _valid_idx:
                    df_clean.loc[_valid_idx, _col] = df_raw.loc[_valid_idx, _col].values
            df_clean = df_clean.copy()

            # Features ML
            df_clean = mdlg.compute_market_features(df_clean, sat_dir, gov_features)
            df_clean = mdlg.compute_temporal_features(df_clean, market_context)
            df_clean, img_cols = mdlg.run_vision_features(df_clean, gov_features)
            df_clean, _ = mdlg.run_text_embedding(df_clean)
            df_clean = mdlg.run_multimodal_fusion(df_clean)
            df_clean = mdlg.compute_target(df_clean, nego_rates)
            df_clean = mdlg.compute_sample_weights(df_clean)

            # Filtrer uniquement les NOUVELLES annonces (celles du delta)
            # On recompute les hashes sur le df_clean pour croiser avec known_hashes
            df_clean['_hash'] = df_clean.apply(compute_hash, axis=1)
            df_nouvelles_clean = df_clean[~df_clean['_hash'].isin(known_hashes)].copy()
            df_nouvelles_clean = df_nouvelles_clean.drop(columns=['_hash'], errors='ignore')

            log.info(f"  Pipeline complet : {len(df_clean):,} ann. traitées")
            log.info(f"  Nouvelles nettoyées : {len(df_nouvelles_clean):,}")
            df_clean = df_nouvelles_clean

        except Exception as e:
            log.error(f"  Erreur pipeline : {e}")
            log.debug(traceback.format_exc())
            nb_erreurs = nb_nouveaux
            df_clean   = pd.DataFrame()

    # ── 4. Export delta ──────────────────────────────────────────
    if not df_clean.empty:
        print("\n─── Export delta ─────────────────────────────────────────")
        try:
            TARGET_COLS = [c for c in mdlg.TARGET_COLS if c in df_clean.columns]
            df_export   = df_clean[TARGET_COLS]
            df_export.to_excel(DELTA_PATH, index=False)
            log.info(f"  Delta exporté : {DELTA_PATH.name} ({len(df_export)} ann.)")
        except Exception as e:
            log.error(f"  Erreur export delta : {e}")

        # Mise à jour des fichiers Excel ML complets (optionnel — peut être lourd)
        # Si tu veux mettre à jour les 4 fichiers _BO2_final.xlsx à chaque run,
        # décommenter les lignes suivantes :
        # try:
        #     groupes_clean = mdlg.resegment_and_export(df_clean, {}, [])
        # except Exception as e:
        #     log.error(f"  Erreur export complet : {e}")

    # ── 5. Monitoring ────────────────────────────────────────────
    date_fin = datetime.now().isoformat()
    statut   = 'OK' if nb_erreurs == 0 else 'PARTIEL'

    log_run(run_id, conn, date_deb, date_fin,
            nb_brut, nb_nouveaux, nb_doublons,
            nb_erreurs, sources_ok, sources_err, statut)

    alertes = check_anomalies(cfg, nb_nouveaux, nb_erreurs, nb_brut, sources_err)

    # ── Résumé ───────────────────────────────────────────────────
    print("\n" + "="*65)
    print("   RÉSUMÉ DU RUN")
    print("="*65)
    print(f"  Run ID         : {run_id}")
    print(f"  Durée          : {date_deb[:19]} → {date_fin[:19]}")
    print(f"  Annonces brutes: {nb_brut:,}")
    print(f"  Nouvelles      : {nb_nouveaux:,}")
    print(f"  Doublons       : {nb_doublons:,}")
    print(f"  Nettoyées      : {len(df_clean):,}")
    print(f"  Sources OK     : {len(sources_ok)}")
    print(f"  Sources ERR    : {len(sources_err)}")
    print(f"  Statut         : {statut}")

    if alertes:
        print("\n  ALERTES :")
        for a in alertes:
            print(f"    ⚠  {a}")
    else:
        print("\n  ✔ Aucune anomalie détectée")

    conn.close()
    return {
        'statut':      statut,
        'run_id':      run_id,
        'nb_brut':     nb_brut,
        'nb_nouveaux': nb_nouveaux,
        'nb_doublons': nb_doublons,
        'alertes':     alertes,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────────────────────────────────────

def run_scheduled(cfg: dict) -> None:
    """
    Boucle infinie : lance run_once() chaque jour à l'heure configurée.
    Lance aussi un premier run immédiatement au démarrage.
    """
    heure   = cfg['schedule_hour']
    minute  = cfg['schedule_minute']
    log.info(f"  Scheduler démarré — run chaque jour à {heure:02d}:{minute:02d}")
    log.info(f"  Premier run immédiat...")

    run_once(cfg)  # run immédiat au démarrage

    while True:
        now     = datetime.now()
        target  = now.replace(hour=heure, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait_s = (target - now).total_seconds()
        log.info(f"  Prochain run dans {wait_s/3600:.1f}h ({target.strftime('%Y-%m-%d %H:%M')})")
        time.sleep(wait_s)
        try:
            run_once(cfg)
        except Exception as e:
            log.error(f"  Erreur run schedulé : {e}")
            log.debug(traceback.format_exc())


# ─────────────────────────────────────────────────────────────────────────────
# COMMANDE DELTA
# ─────────────────────────────────────────────────────────────────────────────

def show_delta() -> None:
    """Affiche les nouvelles annonces depuis le dernier run."""
    if not DELTA_PATH.exists():
        print("\n  Aucun delta disponible — lance d'abord : python agent_collecte.py --run")
        return
    df = pd.read_excel(DELTA_PATH)
    print(f"\n  Nouvelles annonces (dernier run) : {len(df):,} annonces")
    print(f"  Fichier : {DELTA_PATH}")
    if len(df) > 0 and 'gouvernorat' in df.columns:
        import mappings as maps
        gov_counts = df['gouvernorat'].value_counts().head(5)
        print("\n  Top gouvernorats :")
        for code, cnt in gov_counts.items():
            name = maps.GOUVERNORAT_DEC.get(int(code), '?')
            print(f"    {name:<15} : {cnt}")


# ─────────────────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Agent de Collecte Multi-Sources Immobilier Tunisie'
    )
    parser.add_argument('--run',      action='store_true', help='Lance un run immédiat')
    parser.add_argument('--schedule', action='store_true', help='Lance le scheduler (tourne en continu)')
    parser.add_argument('--report',   action='store_true', help='Affiche le rapport des derniers runs')
    parser.add_argument('--delta',    action='store_true', help='Affiche les nouvelles annonces')
    parser.add_argument('--config',   action='store_true', help='Affiche la configuration')
    args = parser.parse_args()

    cfg = load_config()

    if args.report:
        print_monitoring_report()

    elif args.delta:
        show_delta()

    elif args.config:
        print("\n  Configuration actuelle :")
        print(json.dumps(cfg, ensure_ascii=False, indent=2))

    elif args.run:
        run_once(cfg)

    elif args.schedule:
        run_scheduled(cfg)

    else:
        parser.print_help()
        print("\n  Exemple : python agent_collecte.py --run")


if __name__ == '__main__':
    main()
