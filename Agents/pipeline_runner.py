"""
pipeline_runner.py — Runner isolé pour un pipeline.
Appelé par agent_collecte.py via subprocess.
Usage: python pipeline_runner.py <bo_name> <pipe_dir> <module_name>
"""
import sys, os, json, time

bo_name     = sys.argv[1]   # ex: BO2
pipe_dir    = sys.argv[2]   # ex: C:\...\BO2
module_name = sys.argv[3]   # ex: pipeline

# Changer vers le dossier du pipeline
os.chdir(pipe_dir)
sys.path.insert(0, pipe_dir)

result = {"bo": bo_name, "status": "FAILED", "nb_lignes": 0, "error": ""}
t0 = time.time()

try:
    import importlib
    module = importlib.import_module(module_name)
    fn     = getattr(module, "run_pipeline")
    fn()
    result["status"]   = "SUCCESS"
    result["duree"]    = round(time.time() - t0, 2)
except Exception as e:
    result["error"]  = str(e)
    result["duree"]  = round(time.time() - t0, 2)
    import traceback
    result["traceback"] = traceback.format_exc()

print("RESULT:" + json.dumps(result, ensure_ascii=False))