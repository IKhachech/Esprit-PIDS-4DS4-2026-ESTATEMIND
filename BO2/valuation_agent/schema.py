"""
schema.py — Contrats d'interface ValuationInput / ValuationOutput
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, List

class ValuationInput(BaseModel):
    # Localisation
    gouvernorat:          int   = Field(..., ge=0, le=24)
    ville_enc:            Optional[float] = None
    lat:                  Optional[float] = None
    lon:                  Optional[float] = None
    quartier:             Optional[str]   = None
    # Bien
    type_bien:            int   = Field(..., ge=1, le=9)
    type_transaction:     int   = Field(2, ge=1, le=2)   # 1=Location 2=Vente
    surface_m2:           float = Field(..., gt=0)
    nb_pieces:            Optional[float] = None
    nb_chambres:          Optional[float] = None
    # Marché
    score_attractivite:   float = Field(0.5, ge=0, le=1)
    market_tension:       float = Field(0.5, ge=0, le=1)
    cycle_marche:         int   = Field(2, ge=0, le=5)
    prix_region_median:   Optional[float] = None
    text_embedding_score: float = Field(0.5, ge=0, le=1)
    nb_images:            int   = 1
    # Équipements
    has_ascenseur:        int = 0
    has_meuble:           int = 0
    has_terrasse:         int = 0
    has_piscine:          int = 0
    has_vue_mer:          int = 0
    has_garage:           int = 0
    has_securite:         int = 0
    has_climatisation:    int = 0
    has_chauffage:        int = 0
    has_cuisine_equip:    int = 0
    has_double_vitrage:   int = 0
    has_porte_blindee:    int = 0
    has_concierge:        int = 0
    has_jardin:           int = 0
    has_standing:         int = 0
    has_vue_montagne:     int = 0
    has_cheminee:         int = 0

class ValuationOutput(BaseModel):
    prix_estime:       float
    fourchette_min:    float
    fourchette_max:    float
    confiance:         str
    r2_score:          float = 0.0
    mape:              float = 0.0
    top_features:      Dict[str, float] = {}
    source_model:      str
    localisation:      str = ""
    description_bien:  str = ""   # "Appartement S3 | Piscine | Garage"
    version:           str = "2.0.0"
