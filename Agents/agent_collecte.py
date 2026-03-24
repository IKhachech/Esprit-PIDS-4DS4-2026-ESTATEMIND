"""
agent_collecte.py v2.0 - Agent Collecte Multi-Sources Intelligent
Chaque pipeline tourne dans un subprocess Python isole.
"""

import os
import sys
import time
import json
import sqlite3
import argparse
import subprocess
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

os.environ.setdefault("PYTHONIOENCODING", "utf-8")

AGENT_VERSION = "2.0.0"
_AGENT_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH       = os.path.join(_AGENT_DIR, "pipeline_runs.db")
LOG_DIR       = os.path.join(_AGENT_DIR, "logs")
MAX_RETRIES   = 3
RETRY_DELAY   = 5

PIPELINE_REGISTRY = {
    "BO2": {"module": "pipeline",     "fn": "run_pipeline", "description": "Valuation", "depends_on": [], "timeout": 3600},
    "BO3": {"module": "pipeline_BO3", "fn": "run_pipeline", "description": "Tendances", "depends_on": [], "timeout": 3600},
    "BO4": {"module": "pipeline_BO4", "fn": "run_pipeline", "description": "Juridique", "depends_on": [], "timeout": 1800},
    "BO5": {"module": "pipeline_BO5", "fn": "run_pipeline", "description": "Rentabilite", "depends_on": ["BO2", "BO3"], "timeout": 2400},
}

_PIPELINE_FILES = {
    "BO2": ("pipeline.py",     "BO2"),
    "BO3": ("pipeline_BO3.py", "BO3"),
    "BO4": ("pipeline_BO4.py", "BO4"),
    "BO5": ("pipeline_BO5.py", "BO5"),
}


def _find_pipeline_path(bo):
    filename, subdir = _PIPELINE_FILES[bo]
    parent_dir = os.path.dirname(_AGENT_DIR)
    candidates = [
        os.path.join(_AGENT_DIR, subdir),
        os.path.join(_AGENT_DIR, subdir.lower()),
        _AGENT_DIR,
        os.path.join(parent_dir, subdir),
        os.path.join(parent_dir, subdir.lower()),
        parent_dir,
    ]
    for folder in candidates:
        if os.path.isfile(os.path.join(folder, filename)):
            return folder
    for root, dirs, files in os.walk(parent_dir):
        if root.replace(parent_dir, "").count(os.sep) > 4:
            dirs.clear()
            continue
        if filename in files:
            return root
    return None


def _resolve_paths():
    resolved = {}
    print("\n  Pipelines detectes :")
    print("  " + "-" * 60)
    for bo, (fname, _) in _PIPELINE_FILES.items():
        path = _find_pipeline_path(bo)
        resolved[bo] = path
        if path:
            try:
                rel = os.path.relpath(path, _AGENT_DIR)
            except ValueError:
                rel = path
            print(f"  [OK] {bo:<4} {fname:<22} -> .{os.sep}{rel}")
        else:
            print(f"  [!!] {bo:<4} {fname:<22} -> INTROUVABLE")
    print("  " + "-" * 60)
    return resolved


PIPELINE_PATHS = _resolve_paths()


class Color:
    GREEN = "\033[92m"; YELLOW = "\033[93m"; RED = "\033[91m"
    CYAN  = "\033[96m"; BOLD   = "\033[1m";  BLUE = "\033[94m"; RESET = "\033[0m"


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    c  = {"INFO": Color.CYAN, "OK": Color.GREEN, "WARN": Color.YELLOW,
          "ERROR": Color.RED, "SECTION": Color.BOLD + Color.BLUE}.get(level, "")
    print(f"{c}[{ts}] [{level}] {msg}{Color.RESET}", flush=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    try:
        with open(os.path.join(LOG_DIR, f"agent_{datetime.now().strftime('%Y%m%d')}.log"), "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {msg.encode('ascii','replace').decode()}\n")
    except Exception:
        pass


def section(title):
    log(f"\n{'='*65}\n  {title}\n{'='*65}", "SECTION")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS runs (run_id TEXT PRIMARY KEY, started_at TEXT, finished_at TEXT, mode TEXT, status TEXT, pipelines TEXT, total_lignes INTEGER, duree_sec REAL, error_count INTEGER)")
    conn.execute("CREATE TABLE IF NOT EXISTS pipeline_results (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, pipeline TEXT, status TEXT, started_at TEXT, finished_at TEXT, duree_sec REAL, nb_lignes INTEGER, nb_retries INTEGER, error_msg TEXT)")
    conn.commit(); conn.close()


def log_pipeline_db(run_id, pipeline, status, started_at, finished_at, duree, nb_lignes, nb_retries, error_msg=""):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT INTO pipeline_results (run_id,pipeline,status,started_at,finished_at,duree_sec,nb_lignes,nb_retries,error_msg) VALUES (?,?,?,?,?,?,?,?,?)",
                     (run_id, pipeline, status, started_at, finished_at, duree, nb_lignes, nb_retries, error_msg[:500]))
        conn.commit(); conn.close()
    except Exception:
        pass


def log_run_db(run_id, started_at, finished_at, mode, status, pipelines, total_lignes, duree_sec, error_count):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("INSERT OR REPLACE INTO runs (run_id,started_at,finished_at,mode,status,pipelines,total_lignes,duree_sec,error_count) VALUES (?,?,?,?,?,?,?,?,?)",
                     (run_id, started_at, finished_at, mode, status, pipelines, total_lignes, duree_sec, error_count))
        conn.commit(); conn.close()
    except Exception:
        pass


class PipelineResult:
    def __init__(self, name):
        self.name = name; self.status = "PENDING"; self.started_at = None
        self.ended_at = None; self.duree_sec = 0.0; self.nb_lignes = 0
        self.nb_retries = 0; self.error_msg = ""


def _run_subprocess(name, run_id):
    cfg         = PIPELINE_REGISTRY[name]
    module_name = cfg["module"]
    pipe_dir    = PIPELINE_PATHS.get(name)

    if not pipe_dir:
        return f"Dossier {name} introuvable", 0

    runner = os.path.join(_AGENT_DIR, "pipeline_runner.py")
    if not os.path.isfile(runner):
        return f"pipeline_runner.py manquant dans {_AGENT_DIR}", 0

    log(f"  Subprocess {name} depuis : {pipe_dir}")

    try:
        proc = subprocess.run(
            [sys.executable, runner, name, pipe_dir, module_name],
            capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=cfg.get("timeout", 3600), cwd=pipe_dir,
        )
    except subprocess.TimeoutExpired:
        return f"{name} timeout", 0
    except Exception as e:
        return str(e), 0

    output = proc.stdout or ""
    for line in output.splitlines():
        if not line.startswith("RESULT:"):
            print(line, flush=True)

    error_msg = ""; nb_lignes = 0
    for line in output.splitlines():
        if line.startswith("RESULT:"):
            try:
                data = json.loads(line[7:])
                nb_lignes = data.get("nb_lignes", 0)
                if data.get("status") == "FAILED":
                    error_msg = data.get("error", "Erreur inconnue")
                    tb = data.get("traceback", "")
                    if tb:
                        os.makedirs(LOG_DIR, exist_ok=True)
                        with open(os.path.join(LOG_DIR, f"{name}_tb_{run_id}.txt"), "w", encoding="utf-8") as f:
                            f.write(tb)
            except Exception:
                pass

    if proc.returncode != 0 and not error_msg:
        error_msg = ((proc.stderr or "").strip()[:300] or f"Code retour {proc.returncode}")

    return error_msg, nb_lignes


def run_single(name, run_id, retry=True):
    result = PipelineResult(name)
    result.started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cfg = PIPELINE_REGISTRY[name]
    log(f"  Demarrage {name} — {cfg['description']}")

    max_tries = MAX_RETRIES if retry else 1
    no_retry_kw = ["introuvable", "manquant", "FileNotFoundError",
                   "No module named", "No objects to concatenate", "pipeline_runner"]

    for attempt in range(1, max_tries + 1):
        t0 = time.time()
        try:
            error_msg, nb_lignes = _run_subprocess(name, run_id)
            result.duree_sec = round(time.time() - t0, 2)
            result.nb_lignes = nb_lignes
            if error_msg:
                raise RuntimeError(error_msg)
            result.status = "SUCCESS"; result.nb_retries = attempt - 1
            result.ended_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log(f"  OK {name} termine en {result.duree_sec:.1f}s", "OK")
            break
        except Exception as e:
            result.duree_sec = round(time.time() - t0, 2)
            result.error_msg = str(e)[:500]
            err = str(e)
            is_fatal = any(kw in err for kw in no_retry_kw)
            if attempt < max_tries and not is_fatal:
                log(f"  !! {name} echec {attempt}/{max_tries} — retry {RETRY_DELAY}s", "WARN")
                log(f"     {err[:150]}", "WARN")
                time.sleep(RETRY_DELAY)
            else:
                result.status = "FAILED"; result.ended_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log(f"  ECHEC {name} : {err[:200]}", "ERROR")
                break

    log_pipeline_db(run_id, name, result.status, result.started_at, result.ended_at or "",
                    result.duree_sec, result.nb_lignes, result.nb_retries, result.error_msg)
    return result


def run_parallel(pipelines, run_id):
    section("MODE PARALLELE")
    results = {}; lock = threading.Lock()

    independent = [n for n in pipelines
                   if not PIPELINE_REGISTRY[n]["depends_on"]
                   or all(d not in pipelines for d in PIPELINE_REGISTRY[n]["depends_on"])]
    dependent   = [n for n in pipelines if n not in independent]

    log(f"  Independants (parallele) : {independent}")
    log(f"  Dependants   (apres)     : {dependent}")
    log("\n  -- Phase 1 : lancement parallele --")

    with ThreadPoolExecutor(max_workers=max(len(independent), 1)) as ex:
        futures = {ex.submit(run_single, name, run_id): name for name in independent}
        for future in as_completed(futures):
            name = futures[future]
            try:
                res = future.result()
            except Exception as e:
                res = PipelineResult(name); res.status = "FAILED"; res.error_msg = str(e)
            with lock:
                results[name] = res

    if dependent:
        log("\n  -- Phase 2 : pipelines dependants --")
        for name in dependent:
            deps = PIPELINE_REGISTRY[name]["depends_on"]
            if all(results.get(d, PipelineResult(d)).status == "SUCCESS" for d in deps if d in pipelines):
                results[name] = run_single(name, run_id)
            else:
                log(f"  SKIP {name} — dependances en echec : {deps}", "WARN")
                res = PipelineResult(name); res.status = "SKIPPED"; results[name] = res

    return results


def run_smart(pipelines, run_id):
    section("MODE SMART (recommande)")
    log("  Strategie : BO2+BO3+BO4 en parallele -> BO5 ensuite")
    return run_parallel(pipelines, run_id)


def run_sequential(pipelines, run_id):
    section("MODE SEQUENTIEL")
    results = {}
    for name in pipelines:
        results[name] = run_single(name, run_id)
    return results


def print_report(results, run_id, mode, t_total):
    section(f"RAPPORT FINAL — {run_id}")
    seen = {name: res for name, res in results.items()}
    print(f"\n  {'Pipeline':<8} {'Statut':<10} {'Duree':>8} {'Lignes':>10} {'Retry':>6}")
    print(f"  {'-'*8} {'-'*10} {'-'*8} {'-'*10} {'-'*6}")
    total = 0; errors = 0
    for name, res in seen.items():
        c = Color.GREEN if res.status == "SUCCESS" else (Color.RED if res.status == "FAILED" else Color.YELLOW)
        tag = {"SUCCESS": "OK", "FAILED": "ECHEC", "SKIPPED": "SKIP"}.get(res.status, "?")
        print(f"  {c}{tag} {name:<6} {res.status:<10} {res.duree_sec:>7.1f}s {res.nb_lignes:>10,} {res.nb_retries:>6}{Color.RESET}")
        total += res.nb_lignes
        if res.status == "FAILED": errors += 1
    print(f"\n  Mode         : {mode}\n  Duree totale : {t_total:.1f}s\n  Total lignes : {total:,}")
    print(f"  Succes       : {sum(1 for r in seen.values() if r.status=='SUCCESS')}/{len(seen)}")
    if errors:
        print(f"  {Color.RED}Erreurs : {errors}{Color.RESET}  Logs : {LOG_DIR}")
    for name, res in seen.items():
        if res.status == "FAILED":
            print(f"\n  {Color.RED}ECHEC {name} : {res.error_msg[:250]}{Color.RESET}")


def print_history(n=10):
    section(f"HISTORIQUE — {n} derniers runs")
    runs = []
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.execute("SELECT run_id,started_at,mode,status,total_lignes,duree_sec FROM runs ORDER BY started_at DESC LIMIT ?", (n,))
        cols = [d[0] for d in cur.description]
        runs = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
    except Exception:
        pass
    if not runs:
        print("  Aucun run enregistre.")
        return
    for r in runs:
        tag = "OK" if r["status"] == "SUCCESS" else "ECHEC"
        print(f"  {tag} [{r['started_at']}] {r['run_id']} | {r['mode']:<12} | {r['status']:<8} | {r.get('total_lignes',0):>7,} lignes | {r.get('duree_sec',0):>7.1f}s")


def _run_and_log(pipelines, mode):
    run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    init_db()
    section(f"AGENT COLLECTE v{AGENT_VERSION} — {run_id}")
    log(f"  Mode       : {mode}")
    log(f"  Pipelines  : {pipelines}")
    log(f"  DB         : {DB_PATH}")
    t0 = time.time(); started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if mode == "sequential":
        results = run_sequential(pipelines, run_id)
    elif mode == "parallel":
        results = run_parallel(pipelines, run_id)
    else:
        results = run_smart(pipelines, run_id)

    t_total = round(time.time() - t0, 2)
    n_ok = sum(1 for r in results.values() if r.status == "SUCCESS")
    n_fail = sum(1 for r in results.values() if r.status == "FAILED")
    status = "SUCCESS" if n_fail == 0 else ("PARTIAL" if n_ok > 0 else "FAILED")
    log_run_db(run_id, started_at, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
               mode, status, ",".join(pipelines),
               sum(r.nb_lignes for r in results.values()), t_total, n_fail)
    print_report(results, run_id, mode, t_total)
    return results


def main():
    parser = argparse.ArgumentParser(description="Agent Collecte — BO2/BO3/BO4/BO5")
    parser.add_argument("--mode",     choices=["smart","sequential","parallel"], default="smart")
    parser.add_argument("--only",     nargs="+", choices=list(PIPELINE_REGISTRY.keys()), default=None)
    parser.add_argument("--schedule", action="store_true")
    parser.add_argument("--report",   action="store_true")
    parser.add_argument("--no-retry", action="store_true")
    args = parser.parse_args()

    if args.report:
        init_db(); print_history(10); return

    pipelines = args.only or list(PIPELINE_REGISTRY.keys())
    if args.no_retry:
        global MAX_RETRIES; MAX_RETRIES = 1
    if args.schedule:
        try:
            from apscheduler.schedulers.blocking import BlockingScheduler
            s = BlockingScheduler()
            s.add_job(lambda: _run_and_log(pipelines,"smart"), "cron", day_of_week="mon", hour=2, minute=0)
            log("Scheduler actif — chaque lundi 02h00", "OK"); s.start()
        except ImportError:
            log("pip install apscheduler requis", "WARN"); _run_and_log(pipelines, "smart")
        return

    _run_and_log(pipelines, args.mode)


if __name__ == "__main__":
    main()