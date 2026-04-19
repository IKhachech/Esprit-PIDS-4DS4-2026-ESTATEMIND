"""
agent_bo2.py — BO2 Multimodal Valuation Agent
EstateMind — Tunisian Real Estate

Role :
    Estimer la valeur d un bien immobilier tunisien
    a partir de ses caracteristiques.

Architecture :
    Orchestrateur → agent_bo2.run(requete) → Orchestrateur

Modeles utilises (dans agent/) :
    xgb_residentiel.json  → Appartements + Maisons + Villas
    xgb_foncier.json      → Terrains

Usage :
    from agent_bo2 import ValuationAgent
    agent = ValuationAgent()
    result = agent.run({
        "type_bien":   "Appartement",
        "transaction": "vente",
        "ville":       "La Marsa",
        "gouvernorat": "Tunis",
        "surface_m2":  120,
        "nb_pieces":   3,
        "nb_chambres": 2,
        "has_piscine": 0,
        "has_vue_mer": 1,
    })
"""

import os, json, pickle, warnings, math
import numpy as np
import pandas as pd
import requests
from io import BytesIO
from PIL import Image
warnings.filterwarnings('ignore')

from xgboost import XGBRegressor
try:
    from lightgbm import LGBMRegressor
    USE_LGBM = True
except ImportError:
    USE_LGBM = False
from sklearn.decomposition import PCA

# ================================================================
# CONFIG
# ================================================================

AGENT_DIR = 'agent'

GOV_ENC = {
    'Ariana': 1, 'Béja': 2, 'Ben Arous': 3, 'Bizerte': 4,
    'Gabès': 5, 'Gafsa': 6, 'Jendouba': 7, 'Kairouan': 8,
    'Kasserine': 9, 'Kébili': 10, 'Le Kef': 11, 'Mahdia': 12,
    'Manouba': 13, 'Médenine': 14, 'Monastir': 15, 'Nabeul': 16,
    'Sfax': 17, 'Sidi Bouzid': 18, 'Siliana': 19, 'Sousse': 20,
    'Tataouine': 21, 'Tozeur': 22, 'Tunis': 23, 'Zaghouan': 24,
}

GOV_CENTRES = {
    1:(36.865,10.160), 2:(36.730,9.182),  3:(36.752,10.228), 4:(37.274,9.865),
    5:(33.883,10.097), 6:(34.425,8.784),  7:(36.501,8.727),  8:(35.678,10.100),
    9:(35.167,8.833),  10:(33.705,8.971), 11:(36.182,8.710), 12:(35.504,11.062),
    13:(36.831,10.037),14:(33.354,10.500),15:(35.767,10.812),16:(36.456,10.735),
    17:(34.740,10.760),18:(35.040,9.485), 19:(36.085,9.370), 20:(35.825,10.636),
    21:(32.929,10.452),22:(33.919,8.139), 23:(36.818,10.180),24:(36.403,10.143),
}

HAS_FEATURES = [
    'has_ascenseur','has_meuble','has_terrasse','has_piscine',
    'has_vue_mer','has_garage','has_securite','has_climatisation',
    'has_chauffage','has_cuisine_equip','has_double_vitrage',
    'has_porte_blindee','has_concierge','has_jardin','has_standing',
    'has_vue_montagne','has_cheminee',
]

CYCLE_MULT = {0:1.0, 1:0.90, 2:1.05, 3:1.10, 4:0.95, 5:0.85}

TYPE_BIEN_LABELS = {1:'appartement', 2:'maison', 3:'villa'}
TYPE_TX_LABELS   = {1:'location', 2:'vente'}

# Valeurs par defaut pour features manquantes
DEFAULTS = {
    'lat':                  36.8,
    'lon':                  10.1,
    'score_attractivite':   0.5,
    'market_tension':       0.5,
    'cycle_marche':         2.0,
    'prix_region_median':   300000.0,
    'text_embedding_score': 0.5,
    'nb_images':            1,
    'source_enc':           0,
    'nb_pieces':            3,
    'nb_chambres':          2,
    'surface_m2':           100.0,
    'etat_score':           1,
    'etat_prob_renove':     0.9,
    'standing_score':       2,
    'standing_prob_luxe':   0.5,
}


# ================================================================
# NORMALISATION VILLE
# ================================================================

def _load_ville_normalization():
    """Charge le dictionnaire de normalisation des villes."""
    json_file = 'ville_normalization.json'
    if os.path.exists(json_file):
        with open(json_file, encoding='utf-8') as f:
            return json.load(f)
    return {}

# Charger au demarrage
_VILLE_NORM = _load_ville_normalization()


def normalize_ville(ville_str):
    """
    Normalise le nom d une ville depuis le dictionnaire pre-calcule
    (2174 variantes pour 231 villes tunisiennes).
    """
    if not ville_str:
        return ''
    v = str(ville_str).strip()

    # 1. Cherche direct dans le dictionnaire
    if v in _VILLE_NORM:
        return _VILLE_NORM[v]

    # 2. Lowercase
    v_lower = v.lower()
    if v_lower in _VILLE_NORM:
        return _VILLE_NORM[v_lower]

    # 3. Title case
    v_title = v.title()
    if v_title in _VILLE_NORM:
        return _VILLE_NORM[v_title]

    # 4. Sans accents
    import unicodedata
    v_no_acc = ''.join(c for c in unicodedata.normalize('NFD', v)
                       if unicodedata.category(c) != 'Mn').lower()
    if v_no_acc in _VILLE_NORM:
        return _VILLE_NORM[v_no_acc]

    # 5. Retourner tel quel (title case)
    return v_title


def lookup_ville_enc(ville_str, ville_map):
    """
    Cherche ville_enc dans le mapping avec normalisation.
    Essaie plusieurs variantes si la premiere echoue.
    """
    if not ville_map:
        return ville_map.get('__default__', 0.3)

    # 1. Essai direct
    if ville_str in ville_map:
        return ville_map[ville_str]

    # 2. Normalisation title case
    v_title = str(ville_str).strip().title()
    if v_title in ville_map:
        return ville_map[v_title]

    # 3. Correction via dictionnaire
    v_norm = normalize_ville(ville_str)
    if v_norm in ville_map:
        return ville_map[v_norm]

    # 4. Recherche insensible a la casse
    v_lower = str(ville_str).strip().lower()
    for key, val in ville_map.items():
        if key.lower() == v_lower:
            return val

    # 5. Recherche partielle (La Marsa → cherche "marsa")
    for key, val in ville_map.items():
        if v_lower in key.lower() or key.lower() in v_lower:
            return val

    # 6. Valeur par defaut
    return ville_map.get('__default__', 0.3)


# ================================================================
# AGENT BO2
# ================================================================

class ValuationAgent:

    def __init__(self):
        self.models      = {}
        self.feat_cols   = {}
        self.pm2_stats   = {}
        self.ville_maps  = {}
        self.bert_pcas   = {}
        self.st_model    = None
        self._load_all()

    # ── Chargement ────────────────────────────────────────────────

    def _load_all(self):
        print('[BO2] Chargement des modeles...')

        for seg in ['residentiel_vente', 'residentiel_location', 'foncier']:
            model_path = os.path.join(AGENT_DIR, f'xgb_{seg}.json')
            feat_path  = os.path.join(AGENT_DIR, f'feature_cols_{seg}.json')
            pm2_path   = os.path.join(AGENT_DIR, f'pm2_stats_{seg}.json')
            bert_path  = os.path.join(AGENT_DIR, f'bert_pca_{seg}.pkl')
            ville_path = 'ville_mapping_terrain.json' if seg == 'foncier'                          else 'ville_mapping_residentiel.json'

            if not os.path.exists(model_path):
                print(f'  ✗ {seg} : {model_path} introuvable')
                continue

            # Charger LightGBM si disponible sinon XGBoost
            lgbm_pkl = os.path.join(AGENT_DIR, f'lgbm_{seg}.pkl')
            if USE_LGBM and os.path.exists(lgbm_pkl):
                with open(lgbm_pkl, 'rb') as f:
                    model = pickle.load(f)
                print(f'  LightGBM {seg} charge')
            else:
                model = XGBRegressor()
                model.load_model(model_path)
            self.models[seg] = model

            # Charger feature columns
            if os.path.exists(feat_path):
                with open(feat_path) as f:
                    self.feat_cols[seg] = json.load(f)

            # Charger stats prix/m²
            if os.path.exists(pm2_path):
                with open(pm2_path) as f:
                    self.pm2_stats[seg] = json.load(f)

            # Charger PCA BERT
            if os.path.exists(bert_path):
                with open(bert_path, 'rb') as f:
                    self.bert_pcas[seg] = pickle.load(f)

            # Charger ville mapping
            ville_file_map = {
                'residentiel_vente':    'ville_mapping_residentiel_vente.json',
                'residentiel_location': 'ville_mapping_residentiel_location.json',
                'foncier':              'ville_mapping_terrain.json',
                'quartier_vente':       'ville_mapping_quartier_vente.json',
                'quartier_location':    'ville_mapping_quartier_location.json',
            }
            vf = ville_file_map.get(seg, ville_path)
            if os.path.exists(vf):
                with open(vf) as f:
                    self.ville_maps[seg] = json.load(f)
            else:
                self.ville_maps[seg] = {'__default__': 0.3}

            print(f'  ✔ {seg} charge ({len(self.feat_cols.get(seg,[]))} features)')

        # Charger mappings quartier depuis racine
        for q_key, q_file in [('quartier_vente',    'ville_mapping_quartier_vente.json'),
                               ('quartier_location', 'ville_mapping_quartier_location.json')]:
            if os.path.exists(q_file):
                with open(q_file) as f:
                    self.ville_maps[q_key] = json.load(f)
                print(f'  ✔ {q_key} mapping charge')
            else:
                self.ville_maps[q_key] = {'__default__': 0.3}

        # Charger BERT
        try:
            from sentence_transformers import SentenceTransformer
            self.st_model = SentenceTransformer(
                'paraphrase-multilingual-MiniLM-L12-v2'
            )
            print('  ✔ BERT charge')
        except Exception as e:
            print(f'  ⚠ BERT non disponible : {e}')

        print(f'[BO2] {len(self.models)} modeles charges\n')

    # ── Identification du segment ─────────────────────────────────

    def _identifier_segment(self, requete):
        type_bien   = str(requete.get('type_bien', '')).lower()
        transaction = str(requete.get('transaction', 'vente')).lower()

        tt_enc = 1 if transaction in ['1','location','louer','rent'] else 2

        if any(x in type_bien for x in ['terrain','lot','foncier']):
            return 'foncier', tt_enc
        # Modeles separes vente/location
        if tt_enc == 2:
            return 'residentiel_vente', tt_enc
        return 'residentiel_location', tt_enc

    # ── Construction du texte BERT ────────────────────────────────

    def _build_text(self, requete, segment, type_bien_int):
        parts = []
        if segment in ['residentiel', 'residentiel_vente', 'residentiel_location']:
            tb  = TYPE_BIEN_LABELS.get(type_bien_int, 'bien')
            tt  = 'vente' if segment == 'residentiel_vente' else 'location'
            parts.append(f'{tb} {tt}')

        equip = [c.replace('has_','').replace('_',' ')
                 for c in HAS_FEATURES if requete.get(c, 0) == 1]
        if equip:
            parts.append(' '.join(equip))

        surf = float(requete.get('surface_m2', 100))
        parts.append(f'surface {surf:.0f}m2')

        if segment == 'foncier':
            surf_val = float(requete.get('surface_m2', 500))
            if   surf_val < 200:  parts.append('urbain')
            elif surf_val < 1000: parts.append('mixte')
            else:                 parts.append('agricole')

        return ' '.join(parts) if parts else 'bien immobilier tunisie'

    # ── Preparation des features ──────────────────────────────────

    def _prepare_features(self, requete, segment, tt_enc):
        gov_str     = str(requete.get('gouvernorat', 'Tunis')).strip()
        gov_enc     = GOV_ENC.get(gov_str, 23)
        ville       = str(requete.get('ville', '')).strip()
        # Charger le bon mapping selon segment avec normalisation
        ville_map = self.ville_maps.get(segment, {})
        ville_enc = lookup_ville_enc(ville, ville_map)
        lat         = float(requete.get('lat', GOV_CENTRES.get(gov_enc, (36.8,10.1))[0]))
        lon         = float(requete.get('lon', GOV_CENTRES.get(gov_enc, (36.8,10.1))[1]))
        surface_m2  = float(requete.get('surface_m2', DEFAULTS['surface_m2']))

        # Construire dict de features de base
        feat = {
            'gouvernorat_enc':      gov_enc,
            'ville_enc':            float(ville_enc),
            'lat':                  lat,
            'lon':                  lon,
            'surface_m2':           surface_m2,
            'nb_pieces':            float(requete.get('nb_pieces', DEFAULTS['nb_pieces'])),
            'nb_chambres':          float(requete.get('nb_chambres', DEFAULTS['nb_chambres'])),
            'score_attractivite':   float(requete.get('score_attractivite', DEFAULTS['score_attractivite'])),
            'market_tension':       float(requete.get('market_tension', DEFAULTS['market_tension'])),
            'cycle_marche':         float(requete.get('cycle_marche', DEFAULTS['cycle_marche'])),
            'prix_region_median':   float(requete.get('prix_region_median', DEFAULTS['prix_region_median'])),
            'text_embedding_score': float(requete.get('text_embedding_score', DEFAULTS['text_embedding_score'])),
            'nb_images':            float(requete.get('nb_images', DEFAULTS['nb_images'])),
            'source_enc':           0,
            'type_transaction':     tt_enc,
            'type_bien':            self._get_type_bien_int(requete),
            'etat_score':           float(requete.get('etat_score', DEFAULTS['etat_score'])),
            'etat_prob_renove':     float(requete.get('etat_prob_renove', DEFAULTS['etat_prob_renove'])),
            'standing_score':       float(requete.get('standing_score', DEFAULTS['standing_score'])),
            'standing_prob_luxe':   float(requete.get('standing_prob_luxe', DEFAULTS['standing_prob_luxe'])),
            'nb_sdb':               int(requete.get('nb_sdb', 0)),
        }

        # quartier_enc depuis mapping
        quartier = str(requete.get('quartier', '')).strip()
        if quartier:
            q_map_key = segment.replace('residentiel_vente', 'quartier_vente')                                .replace('residentiel_location', 'quartier_location')
            q_map = self.ville_maps.get(q_map_key, {})
            feat['quartier_enc'] = float(lookup_ville_enc(quartier, q_map))
        else:
            feat['quartier_enc'] = feat.get('ville_enc', 0.3)

        # Equipements
        for col in HAS_FEATURES:
            feat[col] = int(requete.get(col, 0))

        return feat, gov_enc, surface_m2

    def _get_type_bien_int(self, requete):
        tb = str(requete.get('type_bien', 'Appartement')).lower()
        if 'villa'   in tb: return 3
        if 'maison'  in tb: return 2
        if 'terrain' in tb: return 4
        return 1

    # ── Features engineerees ──────────────────────────────────────

    def _engineer_features(self, feat, segment, gov_enc, surface_m2):
        ville_enc  = feat['ville_enc']
        lat        = feat['lat']
        lon        = feat['lon']

        # Micro-zone
        micro_lat  = round(lat, 2)
        micro_lon  = round(lon, 2)
        micro_zone = round(micro_lat * 1000 + micro_lon, 1)

        cycle        = feat.get('cycle_marche', 2)
        cycle_factor = CYCLE_MULT.get(int(cycle), 1.0)
        surface_log  = math.log1p(surface_m2)
        market_score = feat['score_attractivite'] * feat['market_tension']
        surf_x_attract = surface_log * feat['score_attractivite']
        ville_x_surf   = ville_enc * surface_log
        ville_x_attract= ville_enc * feat['score_attractivite']
        log_prix_region= math.log1p(feat.get('prix_region_median', 300000))

        # Score equipements
        weights = {
            'has_piscine':3,'has_vue_mer':3,'has_jardin':2,'has_garage':2,
            'has_standing':2,'has_concierge':2,'has_cheminee':2,
            'has_ascenseur':1,'has_meuble':1,'has_terrasse':1,'has_securite':1,
            'has_climatisation':1,'has_cuisine_equip':1,'has_chauffage':1,
            'has_double_vitrage':1,'has_porte_blindee':1,'has_vue_montagne':1,
        }
        total_w = sum(weights.values())
        score_equip = sum(feat.get(c,0)*w for c,w in weights.items()) / total_w

        quartier_enc_val = float(feat.get('quartier_enc', ville_enc))
        eng = {
            'micro_lat':          micro_lat,
            'micro_lon':          micro_lon,
            'micro_zone':         micro_zone,
            'surface_log':        surface_log,
            'market_score':       market_score,
            'ville_x_surf':       ville_x_surf,
            'ville_x_attract':    ville_x_attract,
            'cycle_factor':       cycle_factor,
            'surf_x_attract':     surf_x_attract,
            'log_prix_region':    log_prix_region,
            'score_equipements':  score_equip,
            'quartier_enc':       quartier_enc_val,
            'quartier_x_attract': quartier_enc_val * feat['score_attractivite'],
            'quartier_x_surf':    quartier_enc_val * surface_log,
        }

        # Residentiel specifique
        if segment in ['residentiel', 'residentiel_vente', 'residentiel_location']:
            tb_f  = float(feat.get('type_bien', 1))
            tt_f  = float(feat.get('type_transaction', 2))
            nb_p  = float(feat.get('nb_pieces', 3))
            nb_ch = float(feat.get('nb_chambres', 2))
            ville_enc_val = feat.get('ville_enc', 0.3)
            eng.update({
                'type_bien_f':        tb_f,
                'type_transaction_f': tt_f,
                'type_x_surface':     tb_f * surface_log,
                'type_x_ville':       tb_f * ville_enc_val,
                'surf_x_ville':       surface_log * ville_enc_val,
                'nb_pieces_f':        nb_p,
                'nb_chambres_f':      nb_ch,
                'pieces_surf':        surface_log * nb_p,
                'surf_per_piece':     surface_m2 / max(nb_p, 1),
            })

        # Foncier specifique
        elif segment == 'foncier':
            if   surface_m2 < 200:  type_terrain = 1.0
            elif surface_m2 < 1000: type_terrain = 2.0
            else:                   type_terrain = 3.0

            gov_c    = GOV_CENTRES.get(gov_enc, (36.8, 10.1))
            dlat     = math.radians(gov_c[0] - lat)
            dlon     = math.radians(gov_c[1] - lon)
            a        = (math.sin(dlat/2)**2 +
                        math.cos(math.radians(lat)) *
                        math.cos(math.radians(gov_c[0])) *
                        math.sin(dlon/2)**2)
            dist     = 6371 * 2 * math.asin(math.sqrt(max(0, min(1, a))))

            density_norm = 0.5
            inv_dist     = 1 / (1 + dist)
            log_dist     = math.log1p(dist)
            ville_enc_val= feat.get('ville_enc', 0.3)

            # score_urbanisation sans pm2_micro
            score_urban  = (0.5 * ville_enc_val +
                            0.3 * density_norm  +
                            0.2 * inv_dist)

            eng.update({
                'type_terrain':        type_terrain,
                'dist_centre':         dist,
                'densite_zone':        10.0,
                'score_urbanisation':  score_urban,
                'density_norm':        density_norm,
                'inv_dist':            inv_dist,
                'log_dist':            log_dist,
                'urban_x_surface':     score_urban * surface_log,
                'density_x_surface':   density_norm * surface_log,
                'surf_x_ville_fon':    surface_log * ville_enc_val,
            })

        feat.update(eng)
        return feat

    # ── Features depuis image ─────────────────────────────────────

    def _load_clip(self):
        """Charge CLIP si disponible."""
        try:
            import clip
            import torch
            device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            model, preprocess = clip.load('ViT-B/32', device=device,
                                           download_root=os.path.join(os.path.expanduser('~'), 'clip_cache'))
            model.eval()
            return model, preprocess, device
        except Exception:
            return None, None, None

    def _load_efficientnet(self):
        """Charge EfficientNet si modeles disponibles."""
        try:
            import torch
            import torch.nn as nn
            from torchvision import models
            device = torch.device('cpu')

            def build_model(n_classes, path):
                m = models.efficientnet_b0(weights=None)
                in_f = m.classifier[1].in_features
                m.classifier = nn.Sequential(
                    nn.Dropout(p=0.3),
                    nn.Linear(in_f, 128),
                    nn.ReLU(),
                    nn.Dropout(p=0.2),
                    nn.Linear(128, n_classes)
                )
                m.load_state_dict(torch.load(path, map_location=device))
                m.eval()
                return m

            m_stand = build_model(3, 'efficientnet_standing.pth')                       if os.path.exists('efficientnet_standing.pth') else None
            m_etat  = build_model(2, 'efficientnet_etat.pth')                       if os.path.exists('efficientnet_etat.pth') else None
            return m_stand, m_etat, device
        except Exception as e:
            return None, None, None

    def _load_image(self, image_input):
        """Charge une image depuis URL, chemin ou PIL.Image."""
        if isinstance(image_input, Image.Image):
            return image_input.convert('RGB')
        if isinstance(image_input, str):
            if image_input.startswith('http'):
                try:
                    r = requests.get(image_input, timeout=8,
                                     headers={'User-Agent': 'Mozilla/5.0'})
                    if r.status_code == 200:
                        return Image.open(BytesIO(r.content)).convert('RGB')
                except Exception:
                    pass
            elif os.path.exists(image_input):
                try:
                    return Image.open(image_input).convert('RGB')
                except Exception:
                    pass
        return None

    def _process_image(self, image_input):
        """
        Traite une ou plusieurs images et retourne les features
        CLIP + EfficientNet (moyenne sur toutes les images valides).

        Args:
            image_input : URL/chemin/PIL.Image ou liste de ces types

        Returns:
            dict avec cnn_features (50 dims), etat_score, standing_score
        """
        import torch
        from torchvision import transforms

        result = {
            'cnn_features':      np.zeros(50),
            'etat_score':        1,
            'etat_prob_renove':  0.9,
            'standing_score':    2,
            'standing_prob_luxe':0.5,
            'image_analysee':    False,
            'nb_images_ok':      0,
        }

        # Normaliser en liste
        if not isinstance(image_input, list):
            image_input = [image_input]

        # Charger toutes les images
        images = []
        for inp in image_input:
            img = self._load_image(inp)
            if img is not None:
                images.append(img)

        if not images:
            print('  ⚠ Aucune image disponible — features visuelles par defaut')
            return result

        result['image_analysee'] = True
        result['nb_images_ok']   = len(images)
        print(f'  {len(images)}/{len(image_input)} images chargees')

        # ── CLIP features (moyenne sur toutes les images) ─────
        clip_model, clip_preprocess, clip_device = self._load_clip()
        if clip_model is not None:
            try:
                pca_file = 'pca_models.pkl'
                if os.path.exists(pca_file):
                    with open(pca_file, 'rb') as f:
                        pca_models = pickle.load(f)
                    pca_bundle = pca_models.get('residentiel_vente',
                                 pca_models.get('residentiel',
                                 list(pca_models.values())[0] if pca_models else None))

                    if pca_bundle:
                        all_feats = []
                        for img in images:
                            img_t = clip_preprocess(img).unsqueeze(0).to(clip_device)
                            with torch.no_grad():
                                f = clip_model.encode_image(img_t)
                                all_feats.append(f.float().cpu().numpy())

                        # Moyenne des features CLIP
                        mean_feats = np.mean(all_feats, axis=0)
                        scaler = pca_bundle.get('scaler')
                        pca    = pca_bundle.get('pca')
                        if scaler and pca:
                            feats_scaled = scaler.transform(mean_feats)
                            feats_pca    = pca.transform(feats_scaled)[0]
                            result['cnn_features'] = feats_pca
                            print(f'  CLIP : {len(feats_pca)} features (moyenne {len(images)} images)')
            except Exception as e:
                print(f'  ⚠ CLIP erreur : {e}')

        # ── EfficientNet (moyenne des predictions) ────────────
        transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                 std =[0.229, 0.224, 0.225])
        ])

        m_stand, m_etat, _ = self._load_efficientnet()

        if m_etat is not None:
            try:
                all_probs = []
                for img in images:
                    img_t = transform(img).unsqueeze(0)
                    with torch.no_grad():
                        out = m_etat(img_t)
                        all_probs.append(torch.softmax(out, dim=1).numpy()[0])
                # Moyenne des probabilites
                mean_probs = np.mean(all_probs, axis=0)
                result['etat_score']       = int(mean_probs.argmax())
                result['etat_prob_renove'] = float(mean_probs[1])
                etat_label = 'Renove' if result['etat_score'] == 1 else 'Ancien'
                print(f'  Etat detecte    : {etat_label} ({mean_probs.max():.0%})')
            except Exception as e:
                print(f'  ⚠ EfficientNet etat erreur : {e}')

        if m_stand is not None:
            try:
                all_probs = []
                for img in images:
                    img_t = transform(img).unsqueeze(0)
                    with torch.no_grad():
                        out = m_stand(img_t)
                        all_probs.append(torch.softmax(out, dim=1).numpy()[0])
                mean_probs = np.mean(all_probs, axis=0)
                result['standing_score']     = int(mean_probs.argmax()) + 1
                result['standing_prob_luxe'] = float(mean_probs[2])
                stand_label = {1:'Modeste', 2:'Standard', 3:'Luxe'}.get(
                               result['standing_score'], '?')
                print(f'  Standing detecte: {stand_label} ({mean_probs.max():.0%})')
            except Exception as e:
                print(f'  ⚠ EfficientNet standing erreur : {e}')

        return result

    # ── BERT features ─────────────────────────────────────────────

    def _bert_features(self, requete, segment, type_bien_int):
        if self.st_model is None or segment not in self.bert_pcas:
            n = len(self.feat_cols.get(segment, []))
            n_bert = sum(1 for c in self.feat_cols.get(segment,[]) if c.startswith('bert_'))
            return {f'bert_{i}': 0.0 for i in range(n_bert)}

        text = self._build_text(requete, segment, type_bien_int)
        emb  = self.st_model.encode([text], convert_to_numpy=True)
        pca  = self.bert_pcas[segment]
        bert_pca = pca.transform(emb)[0]
        return {f'bert_{i}': float(v) for i, v in enumerate(bert_pca)}

    # ── Prediction ────────────────────────────────────────────────

    def _predict(self, segment, feat_dict):
        if segment not in self.models:
            return None, None, None

        model     = self.models[segment]
        feat_cols = self.feat_cols.get(segment, [])
        pm2_stats = self.pm2_stats.get(segment, {})
        median_pm2 = pm2_stats.get('median_pm2', 3000.0)

        # Construire vecteur X dans le bon ordre
        X = np.array([[feat_dict.get(col, 0.0) for col in feat_cols]])

        # Prediction prix/m²
        y_log  = float(model.predict(X)[0])
        pm2    = float(np.expm1(y_log))

        # Prix total = prix/m² × surface
        surface = float(feat_dict.get('surface_m2', 100))
        prix    = pm2 * surface

        # Fourchette de confiance depuis MAPE (~24%)
        mape_factor = 0.24
        fourchette  = prix * mape_factor

        prix_min = max(0, prix - fourchette)
        prix_max = prix + fourchette

        return round(prix, 0), round(prix_min, 0), round(prix_max, 0)

    # ── Confiance ─────────────────────────────────────────────────

    def _calculer_confiance(self, segment):
        r2_scores = {
            'residentiel_vente':    0.60,
            'residentiel_location': 0.69,
            'foncier':              0.49,
        }
        return r2_scores.get(segment, 0.5)

    # ── Point d entree principal ──────────────────────────────────

    def run(self, requete: dict, image=None) -> dict:
        """
        Point d entree de l agent BO2.

        Args:
            requete (dict) :
                type_bien    : Appartement / Maison / Villa / Terrain
                transaction  : vente / location
                ville        : La Marsa / Tunis / Hammamet...
                gouvernorat  : Tunis / Ariana / Nabeul...
                surface_m2   : surface en m²
                nb_pieces    : nombre de pieces
                nb_chambres  : nombre de chambres
                has_piscine  : 0 ou 1
                has_vue_mer  : 0 ou 1
                ... autres equipements

            image : URL, chemin fichier ou PIL.Image (optionnel)
                    Si fourni, CLIP + EfficientNet analysent la photo

        Returns:
            dict : {prix_estime, fourchette_min, fourchette_max,
                    confiance, segment, message, details}
        """
        # 1. Identifier le segment
        segment, tt_enc = self._identifier_segment(requete)

        if segment not in self.models:
            return {
                'erreur':      f'Modele non disponible : {segment}',
                'segment':     segment,
                'prix_estime': None,
            }

        # 2. Preparer les features de base
        feat, gov_enc, surface_m2 = self._prepare_features(
            requete, segment, tt_enc
        )

        # 3. Feature engineering
        feat = self._engineer_features(feat, segment, gov_enc, surface_m2)

        # 4. Features image (CLIP + EfficientNet) si image fournie
        type_bien_int = self._get_type_bien_int(requete)
        img_result = {}
        if image is not None:
            print(f'\n  Analyse image...')
            img_result = self._process_image(image)
            # Ajouter etat et standing depuis image
            feat['etat_score']          = img_result['etat_score']
            feat['etat_prob_renove']    = img_result['etat_prob_renove']
            feat['standing_score']      = img_result['standing_score']
            feat['standing_prob_luxe']  = img_result['standing_prob_luxe']
            # Ajouter CNN features
            cnn_feats = img_result['cnn_features']
            feat_cols = self.feat_cols.get(segment, [])
            cnn_cols  = [c for c in feat_cols if c.startswith('cnn_')]
            for i, col in enumerate(cnn_cols):
                if i < len(cnn_feats):
                    feat[col] = float(cnn_feats[i])

        # 5. Features BERT
        bert_feats    = self._bert_features(requete, segment, type_bien_int)
        feat.update(bert_feats)

        # 5. Prediction
        prix, prix_min, prix_max = self._predict(segment, feat)

        if prix is None:
            return {'erreur': 'Erreur prediction', 'segment': segment}

        # 6. Confiance
        confiance = self._calculer_confiance(segment)

        # 7. Message
        ville      = requete.get('ville', '')
        type_bien  = requete.get('type_bien', '')
        transaction = requete.get('transaction', 'vente')
        seg_label = segment.replace('residentiel_vente', 'residentiel (vente)')                              .replace('residentiel_location', 'residentiel (location)')
        message = (f'{type_bien} a {transaction} — {ville} — '
                   f'Estimation basee sur le segment {seg_label}')

        pm2_estime = round(prix / max(surface_m2, 1), 0)

        return {
            'prix_estime':    int(prix),
            'fourchette_min': int(prix_min),
            'fourchette_max': int(prix_max),
            'confiance':      round(confiance, 2),
            'segment':        segment,
            'message':        message,
            'details': {
                'fourchette_tnd':  int((prix_max - prix_min) // 2),
                'fourchette_pct':  round((prix_max - prix_min) / prix * 100, 1),
                'ville_enc':       round(feat.get('ville_enc', 0), 3),
                'quartier_enc':    round(feat.get('quartier_enc', 0), 3),
                'gouvernorat':     requete.get('gouvernorat', 'Tunis'),
                'prix_m2':         pm2_estime,
                'surface_m2':      surface_m2,
                'image_analysee':  image is not None,
                'etat_score':      feat.get('etat_score', None),
                'standing_score':  feat.get('standing_score', None),
                'nb_images_ok':    img_result.get('nb_images_ok', 0) if image is not None else 0,
            }
        }


# ================================================================
# DEMO
# ================================================================

# Labels equipements pour affichage
EQUIP_LABELS = {
    'has_piscine':       'Piscine',
    'has_jardin':        'Jardin',
    'has_garage':        'Garage',
    'has_vue_mer':       'Vue mer',
    'has_terrasse':      'Terrasse',
    'has_ascenseur':     'Ascenseur',
    'has_meuble':        'Meuble',
    'has_climatisation': 'Climatisation',
    'has_chauffage':     'Chauffage',
    'has_securite':      'Securite',
    'has_piscine':       'Piscine',
    'has_cuisine_equip': 'Cuisine equip.',
    'has_double_vitrage':'Double vitrage',
    'has_porte_blindee': 'Porte blindee',
    'has_concierge':     'Concierge',
    'has_standing':      'Standing',
    'has_vue_montagne':  'Vue montagne',
    'has_cheminee':      'Cheminee',
}


def afficher_resultat(requete, result):
    """Affiche les details complets d une estimation."""
    if result.get('erreur'):
        print(f'  ERREUR : {result["erreur"]}')
        return

    pe   = result['prix_estime']
    fm   = result['fourchette_min']
    fx   = result['fourchette_max']
    co   = result['confiance']
    sg   = result['segment']
    d    = result.get('details', {})
    fpct = d.get('fourchette_pct', 0)
    venc = d.get('ville_enc', 0)
    qenc = d.get('quartier_enc', 0)
    gov  = d.get('gouvernorat', '')
    pm2  = d.get('prix_m2', 0)

    sep = '-' * 55

    # ── Caracteristiques du bien ──────────────────────────────
    print(f'\n  {sep}')
    print(f'  CARACTERISTIQUES DU BIEN')
    print(f'  {sep}')
    print(f'  Type            : {requete.get("type_bien","")} '
          f'({requete.get("transaction","").upper()})')
    print(f'  Localisation    : {requete.get("ville","")} '
          f'— {requete.get("gouvernorat","")}')
    if requete.get('quartier'):
        print(f'  Quartier        : {requete["quartier"]}')
    print(f'  Surface         : {requete.get("surface_m2","")} m²')

    # Pieces
    nb_p  = requete.get('nb_pieces', None)
    nb_ch = requete.get('nb_chambres', None)
    nb_sdb= requete.get('nb_sdb', None)
    if nb_p:
        pieces_str = f'{nb_p} pieces'
        if nb_ch: pieces_str += f' dont {nb_ch} chambres'
        if nb_sdb: pieces_str += f' | {nb_sdb} salle(s) de bain'
        print(f'  Pieces          : {pieces_str}')

    # Equipements
    equips = [label for col, label in EQUIP_LABELS.items()
              if requete.get(col, 0) == 1]
    if equips:
        print(f'  Equipements     : {" | ".join(equips)}')
    else:
        print(f'  Equipements     : Aucun specifie')

    # ── Estimation ────────────────────────────────────────────
    print(f'\n  {sep}')
    print(f'  ESTIMATION DE PRIX')
    print(f'  {sep}')
    print(f'  Prix estime     : {pe:>12,} TND')
    if pm2 > 0:
        print(f'  Prix/m²         : {pm2:>12,.0f} TND/m²')
    print(f'  Fourchette min  : {fm:>12,} TND')
    print(f'  Fourchette max  : {fx:>12,} TND')
    print(f'  Incertitude     : +/-{(fx-fm)//2:,} TND ({fpct:.1f}%)')

    # ── Confiance et contexte ─────────────────────────────────
    print(f'\n  {sep}')
    print(f'  CONTEXTE MARCHE')
    print(f'  {sep}')
    conf_label = 'Eleve' if co >= 0.75 else ('Moyen' if co >= 0.55 else 'Faible')
    print(f'  Confiance R2    : {co:.2f} ({conf_label})')
    print(f'  Segment         : {sg}')
    print(f'  Ville enc.      : {venc:.3f} | Quartier enc. : {qenc:.3f}')
    print(f'  Gouvernorat     : {gov}')

    # Afficher analyse image si disponible
    img_analysee = d.get('image_analysee', False)
    etat_s   = d.get('etat_score', None)
    stand_s  = d.get('standing_score', None)
    nb_img = d.get('nb_images_ok', 0)
    if img_analysee and etat_s is not None:
        print(f'  {sep}')
        print(f'  ANALYSE IMAGES ({nb_img} photo(s))')
        print(f'  {sep}')
        etat_label  = 'Renove' if etat_s == 1 else 'Ancien'
        stand_label = {1:'Modeste', 2:'Standard', 3:'Luxe'}.get(stand_s, '?')
        print(f'  Etat visuel     : {etat_label}')
        print(f'  Standing visuel : {stand_label}')
    print(f'  {sep}')


def demo():
    agent = ValuationAgent()

    exemples = [
        {
            'type_bien':   'Appartement',
            'transaction': 'vente',
            'ville':       'La Marsa',
            'gouvernorat': 'Tunis',
            'quartier':    'Chotrana 1',
            'surface_m2':  120,
            'nb_pieces':   3,
            'nb_chambres': 2,
            'nb_sdb':      1,
            'has_vue_mer': 1,
            'has_terrasse':1,
            'has_garage':  1,
        },
        {
            'type_bien':   'Villa',
            'transaction': 'location',
            'ville':       'Hammamet',
            'gouvernorat': 'Nabeul',
            'surface_m2':  300,
            'nb_pieces':   5,
            'nb_chambres': 4,
            'nb_sdb':      3,
            'has_piscine': 1,
            'has_jardin':  1,
            'has_garage':  1,
        },
        {
            'type_bien':   'Terrain',
            'transaction': 'vente',
            'ville':       'Sousse',
            'gouvernorat': 'Sousse',
            'surface_m2':  500,
        },
        {
            'type_bien':   'Maison',
            'transaction': 'location',
            'ville':       'Ariana Ville',
            'gouvernorat': 'Ariana',
            'surface_m2':  180,
            'nb_pieces':   4,
            'nb_chambres': 3,
            'nb_sdb':      2,
            'has_jardin':  1,
            'has_garage':  1,
            'has_climatisation': 1,
        },
    ]

    print('\n' + '='*55)
    print('   BO2 — DEMONSTRATION AGENT VALUATION')
    print('='*55)

    # URLs de test — exemple 1 avec plusieurs images
    images_test = [
        [
            'https://www.mubawab-media.com/ad/8/208/658F/h/thumb_1153501_property_xlarge_80949736.avif',
            'https://www.mubawab-media.com/ad/8/208/658F/h/thumb_1153490_property_xlarge_80949737.avif',
        ],
        None,  # sans image
        None,
        None,
    ]

    for i, (requete, img_url) in enumerate(zip(exemples, images_test), 1):
        tb  = requete['type_bien']
        tr  = requete['transaction']
        vil = requete['ville']
        print(f'\n[EXEMPLE {i}] {tb} a {tr} — {vil}')
        if img_url:
            print(f'  Image fournie : {img_url[:60]}...')
        result = agent.run(requete, image=img_url)
        afficher_resultat(requete, result)


if __name__ == '__main__':
    demo()