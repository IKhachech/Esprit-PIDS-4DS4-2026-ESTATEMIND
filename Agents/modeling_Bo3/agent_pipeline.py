#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agent prédictif — Pipeline immobil immobilier tunisien
Orchestre les 4 étapes de modelisation
"""

import argparse
import hashlib
import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd


# CONSTANTES
BASE_DIR = Path(__file__).parent
BO3_DIR = BASE_DIR.parent.parent / "BO3"
MODELS_DIR = BASE_DIR / "models_v2"
LOGS_DIR = MODELS_DIR / "logs"
REPORTS_DIR = MODELS_DIR / "reports"
DATA_HASHES_FILE = MODELS_DIR / "data_hashes.json"

BO3_FILES = [
    BO3_DIR / "residentiel_BO3.xlsx",
    BO3_DIR / "foncier_BO3.xlsx",
    BO3_DIR / "commercial_BO3.xlsx",
]

SCRIPTS = [
    ("segmentation.py", BASE_DIR / "segmentation.py"),
    ("TCN.py", BASE_DIR / "TCN.py"),
    ("arima.py", BASE_DIR / "arima.py"),
    ("xgboost_causal.py", BASE_DIR / "xgboost_causal.py"),
]


class StepFailedError(Exception):
    """Exception raised when a pipeline step fails."""
    def __init__(self, step_name: str, output: str = ""):
        self.step_name = step_name
        self.output = output
        super().__init__(f"Step '{step_name}' failed")


# SECTION 1 - DETECTION DE NOUVEAUX FICHIERS BO3
def compute_file_hash(file_path: Path) -> str:
    """Calcule le hash MD5 d'un fichier."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def check_new_data(logger: logging.Logger) -> tuple:
    """
    Detects if any BO3 file has changed since last run.
    Returns (has_changed: bool, modified_files: list, all_hashes: dict)
    """
    logger.info("Verification des fichiers BO3...")

    if not DATA_HASHES_FILE.exists():
        logger.info("Pas de cache de hashs trouve - considere tout comme nouveau")
        saved_hashes = {}
    else:
        with open(DATA_HASHES_FILE, "r") as f:
            saved_hashes = json.load(f)

    modified_files = []
    all_hashes = {}

    for file_path in BO3_FILES:
        if not file_path.exists():
            logger.warning(f"MISSING: {file_path.name}")
            all_hashes[file_path.name] = None
            continue

        file_hash = compute_file_hash(file_path)
        all_hashes[file_path.name] = file_hash

        saved_hash = saved_hashes.get(file_path.name)
        if saved_hash != file_hash:
            logger.info(f"HASH CHANGED: {file_path.name}")
            modified_files.append(file_path.name)
        else:
            logger.info(f"HASH UNCHANGED: {file_path.name}")

    has_changed = len(modified_files) > 0
    return has_changed, modified_files, all_hashes


def update_data_hashes(hashes: dict, logger: logging.Logger) -> None:
    """Update data_hashes.json file"""
    with open(DATA_HASHES_FILE, "w") as f:
        json.dump(hashes, f, indent=2)
    logger.info(f"Cache de hashs mis a jour: {DATA_HASHES_FILE}")


# SECTION 2 - ORCHESTRATEUR DES 4 ETAPES
def run_step(
    step_name: str,
    script_path: Path,
    logger: logging.Logger,
) -> tuple:
    """
    Execute a Python script via subprocess.
    Raises StepFailedError on failure.
    Returns (duration: float, output: str) on success
    """
    logger.info(f"Lancement de {step_name}...")
    start_time = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=600,
        )

        duration = time.time() - start_time
        output = result.stdout + result.stderr

        if result.returncode != 0:
            logger.error(f"{step_name} a échoué (code {result.returncode})")
            logger.error(f"STDOUT/STDERR:\n{output}")
            raise StepFailedError(step_name, output)

        logger.info(f"{step_name} terminée en {duration:.1f}s")
        return duration, output

    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        msg = f"{step_name} timeout (600s)"
        logger.error(msg)
        raise StepFailedError(step_name, msg)
    except StepFailedError:
        raise  # Re-raise StepFailedError as-is
    except Exception as e:
        duration = time.time() - start_time
        msg = f"Erreur execution {step_name}: {e}"
        logger.error(msg)
        raise StepFailedError(step_name, msg)


def run_full_pipeline(logger: logging.Logger) -> tuple:
    """
    Execute all 4 steps sequentially.
    Returns (success: bool, step_results: dict)
    Raises StepFailedError on first failure.
    """
    step_results = {}
    start_time = time.time()

    for idx, (step_name, script_path) in enumerate(SCRIPTS, 1):
        total_steps = len(SCRIPTS)
        print(f"\n> STEP {idx}/{total_steps} -- {step_name}")
        logger.info(f"STEP {idx}/{total_steps}: {step_name}")

        try:
            duration, output = run_step(step_name, script_path, logger)
            step_results[step_name] = {"success": True, "duration": duration}
            print(f"[OK] STEP {idx} terminee en {duration:.1f}s")

        except StepFailedError as e:
            print(f"[FAIL] STEP {idx} ECHOUÉE: {e.step_name}")
            logger.error(f"STEP {idx} FAILED at {e.step_name}")
            
            # Display last 30 lines of output for debugging
            print(f"\n--- Dernières lignes stdout/stderr ---")
            if e.output:
                lines = e.output.splitlines()[-30:]
                print("\n".join(lines))
            
            raise  # Re-raise to stop pipeline execution

    total_duration = time.time() - start_time
    logger.info(f"Pipeline completed in {total_duration:.1f}s")
    return True, step_results


# SECTION 3 - GENERATION DU RAPPORT FINAL
def generate_report(run_timestamp: str, logger: logging.Logger) -> str:
    """
    Load CSVs and generate consolidated report.
    Returns path to created report.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"report_{run_timestamp}.txt"

    logger.info(f"Generation du rapport: {report_path}")

    report_lines = [
        "========================================",
        "AGENT PRÉDICTIF — TENDANCES MARCHE TUNISIER",
        f"Run: {run_timestamp}",
        "========================================",
        "",
        "RESUME EXECUTIF",
        "---------------",
    ]

    # Load available data
    arima_summary_path = MODELS_DIR / "arima_forecasts" / "arima_summary.csv"
    cluster_summary_path = MODELS_DIR / "cluster_summary_by_gouvernorat_real.csv"
    shap_perm_path = MODELS_DIR / "xgboost_causal" / "shap_vs_permutation.csv"

    try:
        if arima_summary_path.exists():
            arima_df = pd.read_csv(arima_summary_path)
            segments = arima_df["segment"].unique()
            report_lines.append(f"+ Segments traites: {', '.join(map(str, sorted(segments)))}")

            # Clusters by recommendation
            if "recommendation_usage" in arima_df.columns:
                central = arima_df[arima_df["recommendation_usage"] != "INDICATIF_SEULEMENT"]["cluster_name"].unique()
                indicatif = arima_df[arima_df["recommendation_usage"] == "INDICATIF_SEULEMENT"]["cluster_name"].unique()
                if len(central) > 0:
                    report_lines.append(f"+ Clusters CENTRAL: {', '.join(map(str, central[:5]))}")
                if len(indicatif) > 0:
                    report_lines.append(f"+ Clusters INDICATIF_SEULEMENT: {', '.join(map(str, indicatif[:3]))}")

            # Dominant trend
            if "forecast_change_pct" in arima_df.columns:
                mean_change = arima_df["forecast_change_pct"].mean()
                if mean_change > 5:
                    tendance = "hausse"
                elif mean_change < -2:
                    tendance = "baisse"
                else:
                    tendance = "stable"
                report_lines.append(f"+ Tendance dominante: {tendance} (+{mean_change:.1f}% moyen)")

        report_lines.extend(["", "PREVISIONS PAR SEGMENT", "-----------------------", ""])

        if arima_summary_path.exists():
            arima_df = pd.read_csv(arima_summary_path)
            for segment in sorted(arima_df["segment"].unique()):
                seg_data = arima_df[arima_df["segment"] == segment]
                report_lines.append(f"\n[{segment}]")

                for _, row in seg_data.iterrows():
                    cluster_name = row.get("cluster_name", "")
                    forecast = row.get("forecast", 0)
                    change_pct = row.get("forecast_change_pct", 0)
                    reco = row.get("recommendation_usage", "SCENARIO")
                    mape = row.get("mape_pct", 0)

                    report_lines.append(
                        f"  {cluster_name} [reco={reco}] -> "
                        f"Prev: {forecast:.0f} TND (+-{change_pct:.1f}%) | MAPE: {mape:.1f}%"
                    )

        report_lines.extend(["", "ALERTES", "-------", ""])

        if arima_summary_path.exists():
            arima_df = pd.read_csv(arima_summary_path)
            alerts = []

            for _, row in arima_df.iterrows():
                if row.get("forecast_change_pct", 0) > 25:
                    alerts.append(f"! {row.get('cluster_name', '')}: FORTE HAUSSE DETECTÉE (+{row['forecast_change_pct']:.1f}%)")
                elif row.get("forecast_change_pct", 0) < -10:
                    alerts.append(f"! {row.get('cluster_name', '')}: BAISSE DETECTÉE ({row['forecast_change_pct']:.1f}%)")

                if row.get("recommendation_usage") == "INDICATIF_SEULEMENT":
                    alerts.append(f"! {row.get('cluster_name', '')}: DONNEES INSUFFISANTES")

                if row.get("mape_pct", 0) > 30:
                    alerts.append(f"! {row.get('cluster_name', '')}: FIABILITE FAIBLE (MAPE {row['mape_pct']:.1f}%)")

            if alerts:
                report_lines.extend(alerts)
            else:
                report_lines.append("[OK] Aucune alerte detecte")

        report_lines.extend(["", "QUALITE DU RUN", "--------------", ""])
        report_lines.append(f"+ Rapport genere: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"+ Fichiers sources: {', '.join([f.name for f in BO3_FILES if f.exists()])}")

        report_text = "\n".join(report_lines)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_text)

        logger.info(f"Rapport ecrit: {report_path}")
        return str(report_path)

    except Exception as e:
        logger.error(f"Erreur generation rapport: {e}")
        raise


# SECTION 4 - SETUP DU LOGGING
def setup_logger(debug: bool = False) -> tuple:
    """Configure logger with file and console."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    log_filename = f"agent_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_path = LOGS_DIR / log_filename

    logger = logging.getLogger("agent_pipeline")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG if debug else logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if debug else logging.INFO)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger, log_path


# SECTION 5 - POINT D'ENTREE PRINCIPAL
def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Agent predictif - Pipeline immobilier tunisien"
    )
    parser.add_argument("--force", action="store_true", help="Force pipeline execution")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    parser.add_argument("--report-only", action="store_true", help="Generate report only")

    args = parser.parse_args()

    # Setup logging
    logger, log_path = setup_logger(debug=args.debug)

    # Banner
    print("\n" + "=" * 40)
    print("  AGENT PREDICTIF - TUNISIE IMMO")
    print("  Pipeline: Seg->TCN->ETS->Causal")
    print("=" * 40 + "\n")

    logger.info("-" * 50)
    logger.info("LANCEMENT DU PIPELINE AGENT")
    logger.info("-" * 50)

    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pipeline_start = time.time()

    try:
        # Report-only mode
        if args.report_only:
            logger.info("Mode --report-only: generation rapport uniquement")
            report_path = generate_report(run_timestamp, logger)
            print(f"[OK] Rapport genere: {report_path}")
            return 0

        # Detect new files
        has_changed, modified_files, all_hashes = check_new_data(logger)

        if not has_changed and not args.force:
            print("\n[!] Aucune nouvelle donnee detecte.")
            print("    Utilisez --force pour forcer l'execution du pipeline.\n")
            logger.info("Aucun changement detecte - generation rapport depuis donnees existantes")

            if (MODELS_DIR / "arima_summary.csv").exists():
                report_path = generate_report(run_timestamp, logger)
                print(f"[OK] Rapport genere: {report_path}\n")
            else:
                print("(Pas de donnees existantes pour generer un rapport)\n")

            return 0

        if has_changed or args.force:
            if modified_files:
                print(f"\n[*] Fichiers modifies detectes:")
                for f in modified_files:
                    print(f"    - {f}")
            elif args.force:
                print("\n[*] Execution forcee (--force)")

            print()

            # Launch pipeline
            logger.info("Lancement du pipeline complet...")
            try:
                success, step_results = run_full_pipeline(logger)
            except StepFailedError as e:
                print(f"\n[FAIL] Pipeline arrête à STEP: {e.step_name}")
                logger.error(f"Pipeline arrêté: {e.step_name} a échoué")
                return 1

            # Update hashes after success
            update_data_hashes(all_hashes, logger)

            # Generate report
            report_path = generate_report(run_timestamp, logger)

            pipeline_duration = time.time() - pipeline_start
            print(f"\n[OK] Pipeline termine en {pipeline_duration:.1f}s")
            print(f"     Rapport: {report_path}\n")

            logger.info(f"Pipeline completed successfully in {pipeline_duration:.1f}s")

            return 0

    except KeyboardInterrupt:
        print("\n[!] Pipeline interrompu par l'utilisateur\n")
        logger.warning("Pipeline interrupted by user (Ctrl+C)")
        return 130

    except Exception as e:
        logger.error(f"Erreur non geree: {e}", exc_info=True)
        print(f"\n[ERROR] Erreur: {e}\n")
        return 1


if __name__ == "__main__":
    exit(main())
