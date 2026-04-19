"""
api_bo2.py — API FastAPI pour EstateMind BO2
EstateMind — Tunisian Real Estate

Lance :
    pip install fastapi uvicorn python-multipart pillow
    python api_bo2.py

Endpoints :
    GET  /              → interface HTML
    POST /estimer       → estimation avec JSON
    POST /estimer-image → estimation avec photos uploadées
    GET  /health        → status API
"""

import os, base64, io, json
from typing import Optional, List
from PIL import Image

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ================================================================
# INIT APP
# ================================================================

app = FastAPI(
    title="EstateMind BO2",
    description="Agent de valorisation immobilière tunisienne",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Charger l'agent au démarrage
agent = None

@app.on_event("startup")
async def startup():
    global agent
    print("[API] Chargement de ValuationAgent...")
    try:
        from agent_bo2 import ValuationAgent
        agent = ValuationAgent()
        print("[API] Agent pret ✔")
    except Exception as e:
        print(f"[API] Erreur chargement agent : {e}")
        agent = None


# ================================================================
# HELPERS
# ================================================================

def parse_requete(
    type_bien:      str,
    transaction:    str,
    ville:          str,
    gouvernorat:    str,
    surface_m2:     float,
    quartier:       str = "",
    nb_pieces:      int = 3,
    nb_chambres:    int = 2,
    nb_sdb:         int = 1,
    has_piscine:    int = 0,
    has_jardin:     int = 0,
    has_garage:     int = 0,
    has_vue_mer:    int = 0,
    has_terrasse:   int = 0,
    has_ascenseur:  int = 0,
    has_meuble:     int = 0,
    has_climatisation: int = 0,
    has_chauffage:  int = 0,
    has_securite:   int = 0,
    has_cuisine_equip: int = 0,
    has_concierge:  int = 0,
    has_standing:   int = 0,
    has_vue_montagne: int = 0,
    has_cheminee:   int = 0,
    has_double_vitrage: int = 0,
    has_porte_blindee: int = 0,
) -> dict:
    return {
        'type_bien':          type_bien,
        'transaction':        transaction,
        'ville':              ville,
        'gouvernorat':        gouvernorat,
        'quartier':           quartier,
        'surface_m2':         float(surface_m2),
        'nb_pieces':          int(nb_pieces),
        'nb_chambres':        int(nb_chambres),
        'nb_sdb':             int(nb_sdb),
        'has_piscine':        int(has_piscine),
        'has_jardin':         int(has_jardin),
        'has_garage':         int(has_garage),
        'has_vue_mer':        int(has_vue_mer),
        'has_terrasse':       int(has_terrasse),
        'has_ascenseur':      int(has_ascenseur),
        'has_meuble':         int(has_meuble),
        'has_climatisation':  int(has_climatisation),
        'has_chauffage':      int(has_chauffage),
        'has_securite':       int(has_securite),
        'has_cuisine_equip':  int(has_cuisine_equip),
        'has_concierge':      int(has_concierge),
        'has_standing':       int(has_standing),
        'has_vue_montagne':   int(has_vue_montagne),
        'has_cheminee':       int(has_cheminee),
        'has_double_vitrage': int(has_double_vitrage),
        'has_porte_blindee':  int(has_porte_blindee),
    }


def load_images_from_uploads(files: List[UploadFile]) -> List[Image.Image]:
    """Charge les images uploadees en objets PIL."""
    images = []
    for f in files:
        try:
            data = f.file.read()
            img  = Image.open(io.BytesIO(data)).convert('RGB')
            images.append(img)
        except Exception as e:
            print(f"  ⚠ Image ignoree ({f.filename}) : {e}")
    return images


# ================================================================
# ROUTES
# ================================================================

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "agent":  "pret" if agent else "non charge",
        "models": list(agent.models.keys()) if agent else []
    }


@app.get("/", response_class=HTMLResponse)
async def interface():
    """Retourne l interface HTML."""
    html_file = "estatemind_interface.html"
    if os.path.exists(html_file):
        with open(html_file, encoding='utf-8') as f:
            return f.read()
    return "<h1>Interface non trouvee — placez estatemind_interface.html dans le meme dossier</h1>"


@app.post("/estimer")
async def estimer_json(data: dict):
    """
    Estimation depuis JSON sans images.

    Body JSON :
    {
        "type_bien": "Appartement",
        "transaction": "vente",
        "ville": "La Marsa",
        "gouvernorat": "Tunis",
        "surface_m2": 120,
        "nb_pieces": 3,
        "nb_chambres": 2,
        "has_piscine": 0,
        "has_garage": 1,
        ...
    }
    """
    if not agent:
        raise HTTPException(503, "Agent non disponible")
    try:
        result = agent.run(data)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(500, f"Erreur estimation : {e}")


@app.post("/estimer-image")
async def estimer_avec_images(
    # Champs du formulaire
    type_bien:      str = Form(...),
    transaction:    str = Form(...),
    ville:          str = Form(...),
    gouvernorat:    str = Form(...),
    surface_m2:     float = Form(...),
    quartier:       str = Form(""),
    nb_pieces:      int = Form(3),
    nb_chambres:    int = Form(2),
    nb_sdb:         int = Form(1),
    has_piscine:    int = Form(0),
    has_jardin:     int = Form(0),
    has_garage:     int = Form(0),
    has_vue_mer:    int = Form(0),
    has_terrasse:   int = Form(0),
    has_ascenseur:  int = Form(0),
    has_meuble:     int = Form(0),
    has_climatisation: int = Form(0),
    has_chauffage:  int = Form(0),
    has_securite:   int = Form(0),
    has_cuisine_equip: int = Form(0),
    has_concierge:  int = Form(0),
    has_standing:   int = Form(0),
    has_vue_montagne: int = Form(0),
    has_cheminee:   int = Form(0),
    has_double_vitrage: int = Form(0),
    has_porte_blindee: int = Form(0),
    # Photos uploadees
    photos: List[UploadFile] = File(default=[]),
):
    """
    Estimation depuis formulaire HTML avec photos uploadees.
    Utilisé par l interface web.
    """
    if not agent:
        raise HTTPException(503, "Agent non disponible")

    requete = parse_requete(
        type_bien, transaction, ville, gouvernorat, surface_m2,
        quartier, nb_pieces, nb_chambres, nb_sdb,
        has_piscine, has_jardin, has_garage, has_vue_mer, has_terrasse,
        has_ascenseur, has_meuble, has_climatisation, has_chauffage,
        has_securite, has_cuisine_equip, has_concierge, has_standing,
        has_vue_montagne, has_cheminee, has_double_vitrage, has_porte_blindee,
    )

    # Charger les images
    images = load_images_from_uploads(photos) if photos else []
    print(f"[API] {len(images)} image(s) recue(s)")

    try:
        result = agent.run(requete, image=images if images else None)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(500, f"Erreur estimation : {e}")


# ================================================================
# MAIN
# ================================================================

if __name__ == "__main__":
    print("=" * 55)
    print("  EstateMind BO2 — API FastAPI")
    print("=" * 55)
    print("  Interface : http://localhost:8000")
    print("  Docs API  : http://localhost:8000/docs")
    print("=" * 55)
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)