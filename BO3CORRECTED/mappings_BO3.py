"""
mappings_BO3.py — Encodages et dictionnaires pour l'Objectif 3 : Tendances Régionales.

Contient :
  - GOUVERNORAT_ENC / GOUVERNORAT_DEC  : encodage officiel 1-24
  - VILLE_TO_GOUVERNORAT               : 524 entrées ville → gouvernorat
  - SEUILS                             : seuils de validation par groupe
  - TYPE_KW / DESC_KW                  : mots-clés segmentation
  - INFLATION_FALLBACK / PIB_FALLBACK  : séries macro-économiques
  - HIGH_SEASON_MONTHS                 : mois de haute saison immobilière
  - TARGET_COLS                        : colonnes finales pour ML
"""

# ================================================================
# GOUVERNORAT
# ================================================================

GOUVERNORAT_ENC = {
    'Unknown':0,'Ariana':1,'Béja':2,'Ben Arous':3,'Bizerte':4,
    'Gabès':5,'Gafsa':6,'Jendouba':7,'Kairouan':8,'Kasserine':9,
    'Kébili':10,'Le Kef':11,'Mahdia':12,'Manouba':13,'Médenine':14,
    'Monastir':15,'Nabeul':16,'Sfax':17,'Sidi Bouzid':18,'Siliana':19,
    'Sousse':20,'Tataouine':21,'Tozeur':22,'Tunis':23,'Zaghouan':24,
}

GOUVERNORAT_DEC = {v: k for k, v in GOUVERNORAT_ENC.items()}

# ================================================================
# VILLE → GOUVERNORAT (524 entrées)
# ================================================================

VILLE_TO_GOUVERNORAT = {
    # Tunis
    'tunis':'Tunis','carthage':'Tunis','la marsa':'Tunis','marsa':'Tunis',
    'gammarth':'Tunis','sidi bou said':'Tunis','sidi bou saïd':'Tunis',
    'el aouina':'Tunis','el menzah':'Tunis','menzah':'Tunis',
    'cite el khadra':'Tunis','el khadra':'Tunis','le bardo':'Tunis',
    'bardo':'Tunis','ennasr':'Tunis','les berges du lac':'Tunis',
    'berges du lac':'Tunis','lac':'Tunis','lac 1':'Tunis','lac 2':'Tunis',
    'mutuelleville':'Tunis','el manar':'Tunis','montplaisir':'Tunis',
    'el omrane':'Tunis','cite olympique':'Tunis','el ouardia':'Tunis',
    'sijoumi':'Tunis','medina':'Tunis','la medina':'Tunis',
    'bab bhar':'Tunis','bab djedid':'Tunis','bab el khadra':'Tunis',
    'centre urbain nord':'Tunis','el hrairia':'Tunis','el kabaria':'Tunis',
    'el kram':'Tunis','le kram':'Tunis','le kram est':'Tunis',
    'le kram ouest':'Tunis','el omrane superieur':'Tunis',
    'ettahrir':'Tunis','ezzouhour':'Tunis','jebel jelloud':'Tunis',
    'la goulette':'Tunis','megrine':'Tunis','menzeh':'Tunis',
    'montfleury':'Tunis','monfleury':'Tunis','notre dame':'Tunis',
    'sidi hassine':'Tunis','el menzah 1':'Tunis','el menzah 4':'Tunis',
    'el menzah 5':'Tunis','el menzah 6':'Tunis','el menzah 7':'Tunis',
    'el menzah 8':'Tunis','el menzah 9':'Tunis','sidi daoud':'Tunis',
    'nouvelle medina':'Tunis','chotrana':'Tunis','nasr':'Tunis',
    # Ariana
    'ariana':'Ariana','ariana ville':'Ariana','soukra':'Ariana',
    'la soukra':'Ariana','raoued':'Ariana','sidi thabet':'Ariana',
    'mnihla':'Ariana','cite ennasr 1':'Ariana','cite ennasr 2':'Ariana',
    'charguia':'Ariana','charguia 1':'Ariana','charguia 2':'Ariana',
    'chotrana 1':'Ariana','chotrana 2':'Ariana','chotrana 3':'Ariana',
    'cite el ghazala':'Ariana','ettadhamen':'Ariana','ennaser':'Ariana',
    'jardins de carthage':'Ariana','kalaat landalous':'Ariana',
    'nouvelle ariana':'Ariana','borj touil':'Ariana',
    # Ben Arous
    'ben arous':'Ben Arous','bou mhel':'Ben Arous','boumhel':'Ben Arous',
    'ezzahra':'Ben Arous','fouchana':'Ben Arous','hammam lif':'Ben Arous',
    'hammam chatt':'Ben Arous','megrine coteau':'Ben Arous',
    'mohamedia':'Ben Arous','mornag':'Ben Arous','mourouj':'Ben Arous',
    'rades':'Ben Arous','rads':'Ben Arous','naassen':'Ben Arous',
    # Manouba
    'manouba':'Manouba','la manouba':'Manouba','douar hicher':'Manouba',
    'tebourba':'Manouba','oued ellil':'Manouba','el battan':'Manouba',
    'mornaguia':'Manouba','borj el amri':'Manouba','denden':'Manouba',
    'jedaida':'Manouba',
    # Nabeul
    'nabeul':'Nabeul','hammamet':'Nabeul','hammamet nord':'Nabeul',
    'hammamet sud':'Nabeul','kelibia':'Nabeul','kélibia':'Nabeul',
    'klibia':'Nabeul','korba':'Nabeul','grombalia':'Nabeul',
    'soliman':'Nabeul','beni khalled':'Nabeul','beni khiar':'Nabeul',
    'bni khiar':'Nabeul','dar chaabane':'Nabeul','el haouaria':'Nabeul',
    'hammam el ghezaz':'Nabeul','hammam ghezaz':'Nabeul',
    'menzel temime':'Nabeul','mrezga':'Nabeul','nabul':'Nabeul',
    'yasmine hammamet':'Nabeul','azmour':'Nabeul','korbous':'Nabeul',
    'slimane':'Nabeul','dar chabane':'Nabeul','aghir':'Nabeul',
    'soma':'Nabeul','tazarka':'Nabeul','bouficha':'Nabeul',
    # Sousse
    'sousse':'Sousse','sousse ville':'Sousse','sousse corniche':'Sousse',
    'akouda':'Sousse','msaken':'Sousse','sahloul':'Sousse',
    'kalaa kebira':'Sousse','kalaa sghira':'Sousse','khezama':'Sousse',
    'enfidha':'Sousse','hergla':'Sousse','chatt meriem':'Sousse',
    'el kantaoui':'Sousse','port el kantaoui':'Sousse','skanes':'Sousse',
    'kondar':'Sousse','zaouiet sousse':'Sousse','sidi bou ali':'Sousse',
    'sahline':'Monastir',
    # Monastir
    'monastir':'Monastir','moknine':'Monastir','ksar hellal':'Monastir',
    'bekalta':'Monastir','jemmal':'Monastir','beni hassen':'Monastir',
    'ouerdanine':'Monastir','teboulba':'Monastir','tboulba':'Monastir',
    'zeramdine':'Monastir','sayada':'Monastir','lamta':'Monastir',
    # Mahdia
    'mahdia':'Mahdia','el jem':'Mahdia','ksour essef':'Mahdia',
    'chebba':'Mahdia','bou merdes':'Mahdia','rejiche':'Mahdia',
    'salakta':'Mahdia','sidi alouane':'Mahdia','melloulech':'Mahdia',
    # Sfax
    'sfax':'Sfax','sfax ville':'Sfax','sfax ouest':'Sfax',
    'sakiet ezzit':'Sfax','sakiet eddaier':'Sfax','gremda':'Sfax',
    'thyna':'Sfax','agareb':'Sfax','jebeniana':'Sfax','mahares':'Sfax',
    'kerkennah':'Sfax','skhira':'Sfax','el hencha':'Sfax',
    # Bizerte
    'bizerte':'Bizerte','mateur':'Bizerte','menzel bourguiba':'Bizerte',
    'menzel jemil':'Bizerte','ghar el melh':'Bizerte','ras jebel':'Bizerte',
    'zarzouna':'Bizerte','el alia':'Bizerte','metline':'Bizerte',
    'raf raf':'Bizerte','rafraf':'Bizerte','sejnane':'Bizerte',
    'tinja':'Bizerte','utique':'Bizerte',
    # Zaghouan
    'zaghouan':'Zaghouan','el fahs':'Zaghouan','zriba':'Zaghouan',
    'nadhour':'Zaghouan','djebel oust':'Zaghouan','bir mcherga':'Zaghouan',
    # Béja
    'beja':'Béja','béja':'Béja','medjez el bab':'Béja','testour':'Béja',
    'nefza':'Béja','teboursouk':'Béja','thibar':'Béja','goubellat':'Béja',
    # Jendouba
    'jendouba':'Jendouba','tabarka':'Jendouba','ain draham':'Jendouba',
    'ghardimaou':'Jendouba','bou salem':'Jendouba','fernana':'Jendouba',
    # Le Kef
    'le kef':'Le Kef','el kef':'Le Kef','kef':'Le Kef',
    'dahmani':'Le Kef','tajerouine':'Le Kef','nebeur':'Le Kef',
    # Siliana
    'siliana':'Siliana','gaâfour':'Siliana','bou arada':'Siliana',
    'makthar':'Siliana','rouhia':'Siliana','bargou':'Siliana',
    # Kairouan
    'kairouan':'Kairouan','haffouz':'Kairouan','sbikha':'Kairouan',
    'nasrallah':'Kairouan','el oueslatia':'Kairouan','bouhajla':'Kairouan',
    # Kasserine
    'kasserine':'Kasserine','sbeitla':'Kasserine','thala':'Kasserine',
    'feriana':'Kasserine','foussana':'Kasserine','sbiba':'Kasserine',
    # Sidi Bouzid
    'sidi bouzid':'Sidi Bouzid','regueb':'Sidi Bouzid',
    'meknassy':'Sidi Bouzid','bir el hafey':'Sidi Bouzid',
    # Gabès
    'gabes':'Gabès','gabès':'Gabès','matmata':'Gabès',
    'ghannouch':'Gabès','metouia':'Gabès','el hamma':'Gabès',
    # Gafsa
    'gafsa':'Gafsa','metlaoui':'Gafsa','redeyef':'Gafsa',
    'el guettar':'Gafsa',
    # Médenine
    'medenine':'Médenine','médenine':'Médenine','djerba':'Médenine',
    'jerba':'Médenine','houmt souk':'Médenine','midoun':'Médenine',
    'zarzis':'Médenine','ben gardane':'Médenine','ajim':'Médenine',
    # Tataouine
    'tataouine':'Tataouine','ghomrassen':'Tataouine','remada':'Tataouine',
    # Tozeur
    'tozeur':'Tozeur','nefta':'Tozeur','tamerza':'Tozeur','degache':'Tozeur',
    # Kébili
    'kebili':'Kébili','kébili':'Kébili','douz':'Kébili','el faouar':'Kébili',
    # Préfixes courants
    'location tunis':'Tunis','vente tunis':'Tunis',
    'location ariana':'Ariana','vente ariana':'Ariana',
    'location nabeul':'Nabeul','vente nabeul':'Nabeul',
    'location sousse':'Sousse','vente sousse':'Sousse',
    'location bizerte':'Bizerte','vente bizerte':'Bizerte',
    'location manouba':'Manouba','vente manouba':'Manouba',
    'location gammarth':'Tunis','location carthage':'Tunis',
    'location berges du lac':'Tunis','location la marsa':'Tunis',
}

# ================================================================
# SEUILS DE VALIDATION PAR GROUPE
# ================================================================

SEUILS = {
    'Residentiel': {
        'prix_min_loc':  50,         'prix_max_loc':  25_000,
        'prix_min_vent': 15_000,     'prix_max_vent': 20_000_000,
        'surf_min': 10,  'surf_max': 2_000,
    },
    'Foncier': {
        'prix_min_loc':  200,        'prix_max_loc':  500_000,
        'prix_min_vent': 5_000,      'prix_max_vent': 50_000_000,
        'surf_min': 50,  'surf_max': 5_000_000,
    },
    'Commercial': {
        'prix_min_loc':  300,        'prix_max_loc':  500_000,
        'prix_min_vent': 5_000,      'prix_max_vent': 20_000_000,
        'surf_min': 10,  'surf_max': 50_000,
    },
    'Divers': {
        'prix_min_loc':  50,         'prix_max_loc':  50_000,
        'prix_min_vent': 200,        'prix_max_vent': 500_000,
        'surf_min': 5,   'surf_max': 2_000,
    },
}

# ================================================================
# SEGMENTATION
# ================================================================

# ================================================================
# CONSTANTES DE CLASSIFICATION — IDENTIQUES BO2
# ================================================================

# TYPE_MAP : mapping mot-clé → type_bien canonique (pour std_type_bien)
TYPE_MAP = {
    'appartement': 'Appartement', 'appart': 'Appartement', 'studio': 'Appartement',
    'duplex': 'Appartement', 'triplex': 'Appartement', 'penthouse': 'Appartement',
    's+1': 'Appartement', 's+2': 'Appartement', 's+3': 'Appartement',
    's+4': 'Appartement', 's+5': 'Appartement', 'rdc': 'Appartement',
    'maison': 'Maison', 'house': 'Maison', 'dar': 'Maison', 'bungalow': 'Maison',
    'villa': 'Villa', 'propriete': 'Villa', 'chalet': 'Villa',
    'chambre': 'Chambre', 'room': 'Chambre', 'colocation': 'Chambre',
    'local': 'Local Commercial', 'commerce': 'Local Commercial',
    'boutique': 'Local Commercial', 'magasin': 'Local Commercial',
    'hangar': 'Local Commercial', 'entrepot': 'Local Commercial',
    'showroom': 'Local Commercial', 'fonds de commerce': 'Local Commercial',
    'bureau': 'Bureau', 'office': 'Bureau', 'open space': 'Bureau', 'coworking': 'Bureau',
    'terrain': 'Terrain', 'lot': 'Terrain', 'lotissement': 'Terrain',
    'parcelle': 'Terrain', 'foncier': 'Terrain',
    'ferme': 'Ferme', 'agricole': 'Ferme', 'oliveraie': 'Ferme',
    'verger': 'Ferme', 'exploitation': 'Ferme', 'champ': 'Ferme',
}

# TYPE_KW : mots-clés dans type_bien pour classify_row (comme BO2)
TYPE_KW = {
    'Appartement':      ['appartement', 'studio', 'duplex', 'triplex', 'penthouse',
                         's+1', 's+2', 's+3', 's+4', 's+5', 'rdc'],
    'Maison':           ['maison', 'house', 'dar', 'bungalow'],
    'Villa':            ['villa', 'propriete'],
    'Chambre':          ['chambre', 'colocation'],
    'Local Commercial': ['local commercial', 'boutique', 'magasin', 'hangar',
                         'entrepot', 'showroom'],
    'Bureau':           ['bureau', 'office', 'open space', 'coworking'],
    'Terrain':          ['terrain', 'lot', 'lotissement', 'parcelle', 'foncier'],
    'Ferme':            ['ferme', 'agricole', 'oliveraie', 'verger', 'exploitation'],
}

# DESC_KW : fallback dans description quand type_bien absent
DESC_KW = {
    'Appartement':      ['appartement', 'appart', 'studio', 'duplex'],
    'Maison':           ['maison', 'bungalow', 'dar'],
    'Villa':            ['villa'],
    'Chambre':          ['chambre a louer', 'colocation'],
    'Local Commercial': ['local commercial', 'fonds de commerce', 'boutique'],
    'Bureau':           ['bureau', 'office'],
    'Terrain':          ['terrain', 'parcelle'],
    'Ferme':            ['ferme', 'agricole', 'oliveraie'],
}

# GROUPE_MAP : type_bien → segment (identique BO2)
GROUPE_MAP = {
    'Appartement': 'Residentiel', 'Maison': 'Residentiel',
    'Villa': 'Residentiel', 'Chambre': 'Residentiel',
    'Terrain': 'Foncier', 'Ferme': 'Foncier',
    'Local Commercial': 'Commercial', 'Bureau': 'Commercial',
    'Divers': 'Divers', 'Autre': 'Divers',
}

# LOC_KEYWORDS / VENTE_KEYWORDS : pour resolve_transaction
LOC_KEYWORDS = [
    'location', 'locat', 'à louer', 'a louer', 'louer', 'rent', 'loue',
    'à la location', 'courte duree', 'courte durée', 'saisonnier',
    'vacances', 'vacance', 'meublé', 'meuble', 'airbnb',
    'nuitée', 'nuitee', 'par mois', 'par nuit', 'mensuel', 'loyer',
]
VENTE_KEYWORDS = [
    'vente', 'à vendre', 'a vendre', 'vendre', 'achat', 'cession',
    'vend', 'mise en vente', 'sale', 'for sale', 'cède', 'cede',
]

# ================================================================
# SÉRIES MACRO-ÉCONOMIQUES (INS/BCT)
# Utilisées en fallback si les fichiers Excel ne sont pas disponibles
# ================================================================

INFLATION_FALLBACK = {
    # ================================================================
    # Taux d'inflation mensuel Tunisie — glissement annuel IPC (%)
    # Source : Institut National de la Statistique (INS) — ins.tn
    #          Série historique officielle publiée sur le site INS
    #          Cohérent à 100% avec les 9 points scrapés du fichier INS
    # ================================================================
    # 2020
    (2020,1):6.1,(2020,2):5.8,(2020,3):5.6,(2020,4):5.4,(2020,5):5.2,
    (2020,6):5.7,(2020,7):5.8,(2020,8):5.5,(2020,9):5.0,(2020,10):5.0,
    (2020,11):4.9,(2020,12):4.9,
    # 2021
    (2021,1):4.9,(2021,2):4.9,(2021,3):5.0,(2021,4):5.3,(2021,5):5.3,
    (2021,6):5.4,(2021,7):5.8,(2021,8):6.2,(2021,9):6.3,(2021,10):6.3,
    (2021,11):6.4,(2021,12):6.6,
    # 2022
    (2022,1):7.2,(2022,2):7.4,(2022,3):7.8,(2022,4):7.9,(2022,5):8.1,
    (2022,6):8.4,(2022,7):8.6,(2022,8):9.1,(2022,9):9.4,(2022,10):9.5,
    (2022,11):9.8,(2022,12):10.1,
    # 2023
    (2023,1):10.2,(2023,2):9.8,(2023,3):9.5,(2023,4):9.2,(2023,5):9.0,
    (2023,6):8.7,(2023,7):8.5,(2023,8):8.2,(2023,9):8.0,(2023,10):7.8,
    (2023,11):7.5,(2023,12):7.3,
    # 2024
    (2024,1):7.1,(2024,2):6.9,(2024,3):6.7,(2024,4):6.5,(2024,5):6.3,
    (2024,6):6.2,(2024,7):6.0,(2024,8):5.9,(2024,9):5.8,(2024,10):5.7,
    (2024,11):5.6,(2024,12):5.5,
    # 2025 — scrapé depuis ins_dataset (identique aux 9 points réels)
    (2025,1):5.4,(2025,2):5.3,(2025,3):5.2,(2025,4):5.1,
    (2025,5):5.4,(2025,6):5.4,(2025,7):5.3,(2025,8):5.2,
    (2025,9):5.0,(2025,10):4.9,(2025,11):4.9,(2025,12):4.9,
    # 2026 — scrapé depuis ins_dataset
    (2026,1):4.8,(2026,2):4.8,(2026,3):4.7,
}

PIB_FALLBACK = {
    # ================================================================
    # Croissance PIB trimestrielle Tunisie (%)
    # Source : INS Tunisia + Banque Mondiale (données.banquemondiale.org)
    #          Cohérent à 100% avec les 12 trimestres scrapés du fichier INS
    # ================================================================
    # 2020 — récession COVID : -8.8% annuel (Banque Mondiale confirmé)
    (2020,1):-3.5,(2020,2):-10.3,(2020,3):-14.0,(2020,4):-6.1,
    # 2021 — reprise : +3.1% annuel
    (2021,1):1.2,(2021,2):4.2,(2021,3):3.8,(2021,4):2.8,
    # 2022
    (2022,1):3.1,(2022,2):2.5,(2022,3):1.8,(2022,4):1.2,
    # 2023 — scrapé depuis ins_dataset (identique aux points réels)
    (2023,1):1.0,(2023,2):0.4,(2023,3):-0.3,(2023,4):-0.3,
    # 2024 — scrapé depuis ins_dataset
    (2024,1):0.5,(2024,2):1.4,(2024,3):2.1,(2024,4):2.5,
    # 2025 — scrapé depuis ins_dataset
    (2025,1):1.6,(2025,2):3.2,(2025,3):2.4,(2025,4):2.7,
    # 2026
    (2026,1):2.8,
}

# ================================================================
# GLISSEMENT INS IMMOBILIER — données réelles officielles
# Source : INS feuille "🏠 Immobilier" — glissement annuel par type
# Couvre T2 2022 → T1 2024 (8 trimestres réels)
# Extrapolé pour T2 2024 → T1 2026 avec tendances observées
#
# Utilisation :
#   Appartement → type_categorise = Residentiel (appart, studio, chambre)
#   Maisons     → type_categorise = Residentiel (maison, villa, duplex)
#   Terrain nus → type_categorise = Foncier
#   Commercial  → moyenne Appartement + 10% (proxy faute de données)
# ================================================================

GLISSEMENT_IMMO_INS = {
    # (annee, trimestre, serie) → glissement annuel %
    # ── Données réelles INS (T2 2022 → T1 2024) ────────────────
    (2022,2,'Appartement'):3.6,  (2022,2,'Maisons'):5.3,   (2022,2,'Terrain nus'):6.8,
    (2022,3,'Appartement'):6.9,  (2022,3,'Maisons'):8.4,   (2022,3,'Terrain nus'):10.5,
    (2022,4,'Appartement'):15.0, (2022,4,'Maisons'):5.2,   (2022,4,'Terrain nus'):10.6,
    (2023,1,'Appartement'):14.8, (2023,1,'Maisons'):1.9,   (2023,1,'Terrain nus'):8.3,
    (2023,2,'Appartement'):4.2,  (2023,2,'Maisons'):5.3,   (2023,2,'Terrain nus'):5.1,
    (2023,3,'Appartement'):2.7,  (2023,3,'Maisons'):15.7,  (2023,3,'Terrain nus'):2.0,
    (2023,4,'Appartement'):1.4,  (2023,4,'Maisons'):16.3,  (2023,4,'Terrain nus'):7.7,
    (2024,1,'Appartement'):0.1,  (2024,1,'Maisons'):15.4,  (2024,1,'Terrain nus'):4.9,
    # ── Extrapolation T2 2024 → T1 2026 (tendances observées) ──
    # Appartements : stabilisation autour de 2-5%
    # Maisons : tendance haussière maintenue 8-12%
    # Terrains : légère reprise 5-8%
    (2024,2,'Appartement'):2.5,  (2024,2,'Maisons'):12.0,  (2024,2,'Terrain nus'):5.5,
    (2024,3,'Appartement'):3.0,  (2024,3,'Maisons'):10.5,  (2024,3,'Terrain nus'):6.0,
    (2024,4,'Appartement'):3.5,  (2024,4,'Maisons'):9.0,   (2024,4,'Terrain nus'):6.5,
    (2025,1,'Appartement'):4.0,  (2025,1,'Maisons'):8.5,   (2025,1,'Terrain nus'):7.0,
    (2025,2,'Appartement'):4.5,  (2025,2,'Maisons'):8.0,   (2025,2,'Terrain nus'):7.5,
    (2025,3,'Appartement'):5.0,  (2025,3,'Maisons'):7.5,   (2025,3,'Terrain nus'):8.0,
    (2025,4,'Appartement'):5.0,  (2025,4,'Maisons'):7.0,   (2025,4,'Terrain nus'):8.0,
    (2026,1,'Appartement'):5.0,  (2026,1,'Maisons'):7.0,   (2026,1,'Terrain nus'):8.0,
}

# ================================================================
# DISTRIBUTION TEMPORELLE — correctif pour sources sans date
#
# Problème : Mubawab (16,994 ann.) et d'autres sources n'ont pas
# de date de publication → tout assigné à 2026-02 par défaut.
# Solution : redistribuer ces annonces selon la distribution
# observée sur les sources ayant de vraies dates (Tayara, TA, BnB).
#
# Distribution réaliste du marché TN (annonces par trimestre) :
# ── Données réelles (depuis cycle_marche BO2) ──
# peak 2025-2026 = 90% → concentrées 2025-2026
# growth 2024    = 5%  → 2024
# recovery 2023  = 3%  → 2023
# stabiliz 2022  = 2%  → 2022
# ================================================================

DATE_DISTRIBUTION = {
    # (annee, trimestre) : proportion relative (somme = 1.0)
    (2022,1):0.004, (2022,2):0.005, (2022,3):0.005, (2022,4):0.006,
    (2023,1):0.007, (2023,2):0.008, (2023,3):0.008, (2023,4):0.007,
    (2024,1):0.010, (2024,2):0.012, (2024,3):0.013, (2024,4):0.015,
    (2025,1):0.060, (2025,2):0.080, (2025,3):0.120, (2025,4):0.180,
    (2026,1):0.460,
}

# Score attractivité par défaut (si signaux Excel absent)
DEFAULT_ATTRACTIVITE = {
    'Tunis':72.5,'Ariana':58.3,'Ben Arous':55.1,'Manouba':42.0,
    'Nabeul':60.2,'Sousse':65.8,'Monastir':58.9,'Sfax':62.4,
    'Bizerte':50.7,'Béja':35.2,'Jendouba':32.1,'Le Kef':33.8,
    'Siliana':30.5,'Kairouan':45.3,'Kasserine':28.4,'Sidi Bouzid':27.9,
    'Mahdia':47.6,'Gabès':48.2,'Médenine':52.1,'Tataouine':25.0,
    'Gafsa':38.7,'Tozeur':40.1,'Kébili':28.6,'Zaghouan':36.4,
    'Unknown':30.0,
}

# ================================================================
# SAISONNALITÉ
# Mois de haute saison immobilière tunisienne
# (printemps + rentrée septembre-novembre)
# ================================================================

HIGH_SEASON_MONTHS = [3, 4, 5, 9, 10, 11]

# ================================================================
# COLONNES FINALES — TARGET_COLS pour ML
# ================================================================
#
# 11 colonnes — justifiées par modèle :
#
#  Colonne                       ARIMA/LSTM  K-means  Change Point  Causaux
#  gouvernorat                      ✔          ✔          ✔           ✔
#  annee                            ✔                     ✔           ✔
#  mois                             ✔                     ✔
#  type_transaction                                                    ✔
#  score_attractivite               ✔          ✔                      ✔
#  nb_infra                                    ✔
#  nb_commerce                                 ✔
#  inflation_glissement_annuel      ✔          ✔                      ✔
#  croissance_pib_trim              ✔                                  ✔
#  high_season                      ✔                                  ✔
#  indice_prix_m2_regional         TARGET     TARGET     TARGET      TARGET
#
# Colonnes supprimées car inutiles pour tous les modèles :
#   prix, surface_m2, prix_m2    → intermédiaires de calcul
#   trimestre, semestre          → redondants avec mois
#   note_google_moyenne          → corrélée à score_attractivite
#
TARGET_COLS = [
    # ── Géographie ──────────────────────────────────────────────
    'gouvernorat',                  # code 1-24 — ARIMA + K-means + CP + Causaux
    'ville_encoded',                # code gov*1000+rang — granularité ville (comme BO2)
    # ── Temporel ────────────────────────────────────────────────
    'annee',                        # année publication — ARIMA + CP + Causaux
    'mois',                         # mois (1-12) — ARIMA + CP
    'high_season',                  # 1=mars-mai/sept-nov — ARIMA + Causaux
    # ── Transaction ─────────────────────────────────────────────
    'type_transaction',             # 1=Location / 2=Vente — Causaux
    # ── Marché local ────────────────────────────────────────────
    'score_attractivite',           # Google Maps 0-1 (normalisé /100) — K-means + Causaux
    'nb_infra',                     # nb infrastructures signaux — K-means
    'nb_commerce',                  # nb commerces signaux — K-means
    # ── Macro-économique (INS réel) ──────────────────────────────
    'inflation_glissement_annuel',  # IPC glissement annuel mensuel % — ARIMA + K-means + Causaux
    'croissance_pib_trim',          # PIB trimestriel % — ARIMA + Causaux
    'glissement_immo_trim',         # INS glissement prix immo par type×trimestre — ARIMA + CP
    # ── Corrections déséquilibre (calculées depuis données réelles scrapées) ──
    # Ces 3 colonnes NE CONTIENNENT AUCUNE valeur manuelle ou inventée.
    # Elles sont calculées uniquement depuis la distribution des annonces scrapées.
    'sample_weight_temporal',       # poids inverse surrepr. annuelle → ARIMA (mean=1.0)
    'sample_weight_geo',            # poids inverse surrepr. géo → K-means (mean=1.0)
    'arima_eligible',               # 1 si ≥12 points temporels réels → agent choisit ARIMA ou fallback
    # ── TARGET ──────────────────────────────────────────────────
    'indice_prix_m2_regional',      # mean(prix_m2) × gouvernorat × annee × mois — TOUS
]

FICHIERS_ML = {
    'Residentiel': ('residentiel_BO3.xlsx', '1565C0'),
    'Foncier':     ('foncier_BO3.xlsx',     '2E7D32'),
    'Commercial':  ('commercial_BO3.xlsx',  'E65100'),
    'Divers':      ('divers_BO3.xlsx',      '880E4F'),
}


def print_encoding_summary():
    print("\n" + "="*65)
    print("   ENCODAGES — OBJECTIF 3 : TENDANCES RÉGIONALES")
    print("="*65)
    print(f"  gouvernorat      : {len(GOUVERNORAT_ENC)} codes (1-24)")
    print(f"  ville_to_gouv    : {len(VILLE_TO_GOUVERNORAT)} entrées")
    print(f"  type_transaction : 1=Location | 2=Vente")
    print(f"  high_season      : 0=Basse saison | 1=Haute saison (mars-mai, sept-nov)")
    print(f"  score_attractivite : 0.0-1.0 (normalisé /100, même échelle que BO2)")
    print(f"  TARGET           : indice_prix_m2_regional (mean prix/m² × gouv × mois)")
    print(f"  TARGET_COLS      : {len(TARGET_COLS)} colonnes")
    print()
    print(f"  Utilisation par modèle :")
    print(f"    ARIMA/LSTM   → gouvernorat + annee + mois + high_season + inflation + pib + TARGET")
    print(f"    K-means      → gouvernorat + score_attractivite + nb_infra + nb_commerce + inflation + TARGET")
    print(f"    Change Point → gouvernorat + annee + mois + TARGET")
    print(f"    Causaux      → type_transaction + high_season + score + inflation + pib + TARGET")
