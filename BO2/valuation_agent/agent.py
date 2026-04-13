"""
agent.py — Agent Valuation Multi-Segment (BO2)
Arborescence : BO2/valuation_agent/
Modèles      : BO2/valuation_agent/agent/
"""
import os, json, time, logging
import numpy as np

from .schema   import ValuationInput, ValuationOutput
from .features import build_feature_vector, build_description_bien, HAS_FEATURES, CYCLE_MULT
from .model    import get_feature_importance
from .confidence import compute_interval

logger = logging.getLogger("valuation_agent")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

AGENT_VERSION = "2.0.0"
_HERE     = os.path.dirname(os.path.abspath(__file__))
SAVED_DIR = os.path.join(_HERE, "agent")

# Noms gouvernorats
GOV_NAMES = {
    1:'Ariana',2:'Béja',3:'Ben Arous',4:'Bizerte',5:'Gabès',
    6:'Gafsa',7:'Jendouba',8:'Kairouan',9:'Kasserine',10:'Kébili',
    11:'Le Kef',12:'Mahdia',13:'Manouba',14:'Médenine',15:'Monastir',
    16:'Nabeul',17:'Sfax',18:'Sidi Bouzid',19:'Siliana',20:'Sousse',
    21:'Tataouine',22:'Tozeur',23:'Tunis',24:'Zaghouan',
}
TYPE_BIEN_LABELS = {
    1:'Appartement',2:'Autre',3:'Bureau',4:'Chambre',
    5:'Ferme',6:'Local Commercial',7:'Maison',8:'Terrain',9:'Villa',
}

# Routing type_bien → segment ML
TYPE_BIEN_SEGMENT = {
    1:'residentiel',   # Appartement
    2:'divers',        # Autre
    3:'commercial',    # Bureau
    4:'residentiel',   # Chambre
    5:'foncier',       # Ferme
    6:'commercial',    # Local
    7:'residentiel',   # Maison
    8:'foncier',       # Terrain
    9:'residentiel',   # Villa
}

_MODELS   = {}
_FEATS    = {}
_METAS    = {}
_PM2      = {}
_FALLBACK = {}


# ================================================================
# CHARGEMENT
# ================================================================
def _load_segment(segment: str) -> bool:
    if segment in _MODELS:
        return True
    try:
        import xgboost as xgb
        model_path = os.path.join(SAVED_DIR, f'xgb_{segment}.json')
        feats_path = os.path.join(SAVED_DIR, f'feature_cols_{segment}.json')
        meta_path  = os.path.join(SAVED_DIR, f'xgb_metrics_{segment}.json')
        pm2_path   = os.path.join(SAVED_DIR, f'pm2_stats_{segment}.json')

        if not os.path.exists(model_path):
            logger.warning(f"Modele {segment} introuvable : {model_path}")
            return False

        model = xgb.XGBRegressor()
        model.load_model(model_path)
        _MODELS[segment] = model
        _FEATS[segment]  = json.load(open(feats_path)) if os.path.exists(feats_path) else []
        _METAS[segment]  = json.load(open(meta_path))  if os.path.exists(meta_path)  else {}
        _PM2[segment]    = json.load(open(pm2_path))   if os.path.exists(pm2_path)   else {}

        r2   = _METAS[segment].get('r2', 0.0)
        mae  = _METAS[segment].get('mae', 0.0)
        mape = _METAS[segment].get('mape', 0.0)
        logger.info(f"Modele {segment} — R²={r2:.4f} | MAE={mae:,.0f} | MAPE={mape:.2f}%")
        return True
    except Exception as e:
        logger.error(f"Erreur chargement {segment} : {e}")
        return False


def _load_fallback(segment: str):
    if segment in _FALLBACK:
        return
    for fname, key in [('xgb_commercial_rule.json','commercial'),
                       ('divers_fallback.json','divers')]:
        path = os.path.join(SAVED_DIR, fname)
        if os.path.exists(path):
            _FALLBACK[key] = json.load(open(path))


# ================================================================
# PREDICTION ML
# ================================================================
def _predict_ml(segment: str, inp: ValuationInput):
    import pandas as pd

    model        = _MODELS[segment]
    feature_cols = _FEATS[segment]
    metrics      = _METAS[segment]
    pm2_stats    = _PM2.get(segment, {})
    target_mode  = metrics.get('target_mode', 'prix_m2')

    X = build_feature_vector(inp)

    # ── Enrichir avec stats prix/m² depuis pm2_stats ──────────────
    if pm2_stats:
        gov_key   = str(int(inp.gouvernorat))
        ville_key = str(float(inp.ville_enc or 0.5))
        cf        = CYCLE_MULT.get(int(inp.cycle_marche or 2), 1.0)

        pm2_gov   = float(pm2_stats.get('prix_m2_gov_map',   {}).get(gov_key,
                          pm2_stats.get('median_pm2', 2500)))
        pm2_ville = float(pm2_stats.get('prix_m2_ville_map', {}).get(ville_key, pm2_gov))

        lat = float(inp.lat or 36.8); lon = float(inp.lon or 10.2)
        micro_key = str(round(round(lat,2)*1000 + round(lon,2), 1))
        pm2_micro = float(pm2_stats.get('prix_m2_micro_map', {}).get(micro_key, pm2_ville))

        surf_log  = float(np.log1p(inp.surface_m2 or 100))
        pm2_max   = max(pm2_ville, pm2_micro, 1)
        nb_pieces = float(inp.nb_pieces or 3)
        type_bien = int(inp.type_bien or 1)
        type_tx   = int(inp.type_transaction or 2)

        enrich = {
            'prix_m2_gov':       pm2_gov,
            'prix_m2_ville':     pm2_ville,
            'prix_m2_micro':     pm2_micro,
            'pm2_x_cycle':       pm2_ville * cf,
            'surf_x_ville':      surf_log * (pm2_ville / (pm2_max + 1)),
            'surf_x_micro':      surf_log * (pm2_micro / (pm2_max + 1)),
            'pm2_x_pieces':      pm2_ville / max(nb_pieces, 1),
            'type_x_pm2ville':   type_bien * (pm2_ville / (pm2_max + 1)),
            'tx_x_pm2':          type_tx * (pm2_ville * cf),
        }
        for col, val in enrich.items():
            if col in feature_cols:
                X[col] = val

    # Remplir colonnes manquantes
    for col in feature_cols:
        if col not in X.columns:
            X[col] = 0.0

    X_model = X.reindex(columns=feature_cols, fill_value=0.0)

    # Gérer colonnes 2D
    for col in X_model.columns:
        if isinstance(X_model[col], pd.DataFrame):
            X_model[col] = X_model[col].iloc[:, 0]

    raw_pred = float(model.predict(X_model)[0])

    if target_mode == 'prix_m2':
        prix_m2 = float(np.expm1(raw_pred))
        prix    = prix_m2 * float(inp.surface_m2 or 100)
    else:
        prix = float(np.expm1(raw_pred))

    if prix <= 0 or prix > 100_000_000:
        prix = pm2_stats.get('median_pm2', 2500) * float(inp.surface_m2 or 100)
        logger.warning(f"Prix aberrant corrigé → {prix:,.0f}")

    return round(prix), feature_cols, metrics


# ================================================================
# HEURISTIQUES
# ================================================================
def _predict_commercial(inp: ValuationInput):
    _load_fallback('commercial')
    rule    = _FALLBACK.get('commercial', {})
    gov_key = str(int(inp.gouvernorat))
    pm2_gov = float(rule.get('pm2_res_gov', {}).get(gov_key,
                    rule.get('pm2_res_global', 2500)))
    pm2_micro = float(rule.get('pm2_res_micro', {}).get(gov_key, pm2_gov))
    facteur   = float(np.clip(pm2_micro / max(pm2_gov, 1), 0.5, 2.0))
    prix      = pm2_gov * float(inp.surface_m2 or 100) * facteur
    return round(prix), rule.get('r2', 0.35), rule.get('mape', 50.0)


def _predict_divers(inp: ValuationInput):
    _load_fallback('divers')
    rule    = _FALLBACK.get('divers', {})
    gov_key = str(int(inp.gouvernorat))
    pm2_div = float(rule.get('pm2_div_gov', {}).get(gov_key, 0))
    if pm2_div <= 0:
        pm2_res = float(rule.get('pm2_res_gov', {}).get(gov_key,
                        rule.get('pm2_res_global', 2500)))
        pm2_div = pm2_res * 0.6
    facteur = float(np.clip(0.8, 0.5, 1.2))
    prix    = pm2_div * float(inp.surface_m2 or 100) * facteur
    return round(prix), 0.0, 0.0


# ================================================================
# LOCALISATION
# ================================================================
def _build_localisation(inp: ValuationInput) -> str:
    type_label = TYPE_BIEN_LABELS.get(int(inp.type_bien), 'Bien')
    gov_name   = GOV_NAMES.get(int(inp.gouvernorat), f'Gov {inp.gouvernorat}')
    if inp.quartier and inp.quartier.lower() not in ('unknown','none',''):
        quartier = inp.quartier.replace('_',' ').title()
        return f"{type_label} à {quartier}, {gov_name}"
    return f"{type_label} à {gov_name}"


# ================================================================
# POINT D'ENTREE
# ================================================================
def run(inp: ValuationInput) -> ValuationOutput:
    t0      = time.time()
    segment = TYPE_BIEN_SEGMENT.get(int(inp.type_bien), 'residentiel')

    localisation    = _build_localisation(inp)
    description_bien = build_description_bien(inp)

    logger.info(f"Segment : {segment} | {localisation} | {description_bien}")

    # ── Divers → fallback ─────────────────────────────────────────
    if segment == 'divers':
        prix, r2, mape = _predict_divers(inp)
        source       = "Fallback heuristique (divers)"
        top_features = {}

    # ── Commercial → heuristique ──────────────────────────────────
    elif segment == 'commercial':
        prix, r2, mape = _predict_commercial(inp)
        source       = "Heuristique pm2 × facteur (commercial)"
        top_features = {}

    # ── ML XGBoost ────────────────────────────────────────────────
    else:
        loaded = _load_segment(segment)
        if not loaded:
            logger.warning(f"{segment} indisponible → fallback residentiel")
            segment = 'residentiel'
            loaded  = _load_segment(segment)
        if not loaded:
            raise RuntimeError(
                f"Aucun modèle dans {SAVED_DIR}\n"
                f"Lancez : python -m BO2.valuation_agent.train_model"
            )
        prix, feature_cols, metrics = _predict_ml(segment, inp)
        r2   = float(metrics.get('r2',   0.63))
        mape = float(metrics.get('mape', 25.0))
        source       = f"XGBoost+BERT ({segment})"
        top_features = get_feature_importance(_MODELS[segment], feature_cols, top_n=5)

    interval = compute_interval(
        prix=prix, r2=r2 or 0.4,
        type_transaction=inp.type_transaction,
        type_bien=inp.type_bien,
        gouvernorat=inp.gouvernorat,
        cycle_marche=inp.cycle_marche,
    )

    output = ValuationOutput(
        prix_estime      = prix,
        fourchette_min   = interval["fourchette_min"],
        fourchette_max   = interval["fourchette_max"],
        confiance        = interval["confiance"],
        r2_score         = round(r2, 4) if r2 is not None else 0.0,
        mape             = round(mape, 2) if mape is not None else 0.0,
        top_features     = top_features,
        source_model     = source,
        localisation     = localisation,
        description_bien = description_bien,
        version          = AGENT_VERSION,
    )

    elapsed = round(time.time() - t0, 3)
    logger.info(
        f"OK [{segment}] — {localisation} — {prix:,} TND "
        f"[{output.fourchette_min:,} – {output.fourchette_max:,}] "
        f"conf={output.confiance} | {elapsed}s"
    )
    return output


def reload_model():
    _MODELS.clear(); _FEATS.clear(); _METAS.clear()
    _PM2.clear();    _FALLBACK.clear()
    logger.info("Modèles rechargés.")


# ================================================================
# TEST CLI
# ================================================================
if __name__ == "__main__":
    print("\n" + "="*60)
    print("  TEST AGENT VALUATION — BO2/valuation_agent/")
    print("="*60)

    tests = [
        {"desc": "Appartement S3 Ennasr, Ariana (vente)",
         "inp": ValuationInput(
             gouvernorat=1, type_bien=1, type_transaction=2,
             surface_m2=95, nb_pieces=3, nb_chambres=2,
             score_attractivite=0.72, market_tension=0.65, cycle_marche=2,
             quartier="ennasr2",
             has_ascenseur=1, has_garage=1, has_securite=1)},

        {"desc": "Villa S5 Kantaoui, Sousse (vente)",
         "inp": ValuationInput(
             gouvernorat=20, type_bien=9, type_transaction=2,
             surface_m2=300, nb_pieces=5, nb_chambres=4,
             score_attractivite=0.75, market_tension=0.65, cycle_marche=2,
             quartier="kantaoui",
             has_piscine=1, has_jardin=1, has_vue_mer=1, has_garage=1)},

        {"desc": "Maison S4 Sfax (vente)",
         "inp": ValuationInput(
             gouvernorat=17, type_bien=7, type_transaction=2,
             surface_m2=180, nb_pieces=4, nb_chambres=3,
             score_attractivite=0.55, market_tension=0.50, cycle_marche=2,
             has_jardin=1, has_garage=1)},

        {"desc": "Appartement S2 Lac 2, Tunis (location)",
         "inp": ValuationInput(
             gouvernorat=23, type_bien=1, type_transaction=1,
             surface_m2=75, nb_pieces=2, nb_chambres=1,
             score_attractivite=0.80, market_tension=0.75, cycle_marche=2,
             quartier="lac2",
             has_meuble=1, has_ascenseur=1, has_climatisation=1)},

        {"desc": "Terrain 500m² Nabeul (vente)",
         "inp": ValuationInput(
             gouvernorat=16, type_bien=8, type_transaction=2,
             surface_m2=500,
             score_attractivite=0.55, market_tension=0.50, cycle_marche=2)},
    ]

    for t in tests:
        print(f"\n  {t['desc']}")
        try:
            res = run(t['inp'])
            print(f"    Bien         : {res.description_bien}")
            print(f"    Localisation : {res.localisation}")
            print(f"    Modèle       : {res.source_model}")
            print(f"    Prix         : {res.prix_estime:,} TND")
            print(f"    Fourchette   : {res.fourchette_min:,} – {res.fourchette_max:,} TND")
            print(f"    Confiance    : {res.confiance} | R²={res.r2_score}")
            if res.top_features:
                print(f"    Top features : {res.top_features}")
        except Exception as e:
            print(f"    [!] {e}")
