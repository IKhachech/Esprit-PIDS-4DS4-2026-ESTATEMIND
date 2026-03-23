"""
mappings.py — Centralized categorical mappings for the Tunisia Real Estate Pipeline.

Rules enforced here:
  - Unknown = 0 is kept as encoding key but rows with gouvernorat=0 or type_bien=0
    are DROPPED in cleaning.py after geocoding correction attempts.
  - MARKET_CONTEXT and NEGO_BASE are NOT hardcoded — computed from BCT/INS via external_data.py.
"""

import pandas as pd
import numpy as np

# ================================================================
# 1. GOUVERNORAT — 24 official governorates (Unknown=0 dropped after correction)
# ================================================================
GOUVERNORAT_ENC: dict[str, int] = {
    'Unknown': 0, 'Ariana': 1, 'Béja': 2, 'Ben Arous': 3, 'Bizerte': 4,
    'Gabès': 5, 'Gafsa': 6, 'Jendouba': 7, 'Kairouan': 8, 'Kasserine': 9,
    'Kébili': 10, 'Le Kef': 11, 'Mahdia': 12, 'Manouba': 13, 'Médenine': 14,
    'Monastir': 15, 'Nabeul': 16, 'Sfax': 17, 'Sidi Bouzid': 18, 'Siliana': 19,
    'Sousse': 20, 'Tataouine': 21, 'Tozeur': 22, 'Tunis': 23, 'Zaghouan': 24,
}
GOUVERNORAT_DEC: dict[int, str] = {v: k for k, v in GOUVERNORAT_ENC.items()}

# ================================================================
# 2. VILLE_TO_GOUVERNORAT — full city/district/variant mapping
# ================================================================
VILLE_TO_GOUVERNORAT: dict[str, str | None] = {
    # ── Tunis ─────────────────────────────────────────────────────
    'tunis': 'Tunis', 'carthage': 'Tunis', 'la marsa': 'Tunis', 'marsa': 'Tunis',
    'gammarth': 'Tunis', 'sidi bou said': 'Tunis', 'sidi bou saïd': 'Tunis',
    'sidi bou sad': 'Tunis', 'el aouina': 'Tunis', 'aouina': 'Tunis',
    'el menzah': 'Tunis', 'menzah': 'Tunis', 'cite el khadra': 'Tunis',
    'cité el khadra': 'Tunis', 'cite khadra': 'Tunis', 'el khadra': 'Tunis',
    'le bardo': 'Tunis', 'bardo': 'Tunis', 'ennasr': 'Tunis', 'nasr': 'Tunis',
    'ain zaghouan': 'Tunis', 'aïn zaghouan': 'Tunis', 'ain zagouan': 'Tunis',
    'ain zagouane': 'Tunis', 'les berges du lac': 'Tunis', 'berges du lac': 'Tunis',
    'lac': 'Tunis', 'lac 1': 'Tunis', 'lac 2': 'Tunis', 'el lac': 'Tunis',
    'chotrana': 'Tunis', 'chotrana 1': 'Tunis', 'chotrana 2': 'Tunis', 'chotrana 3': 'Tunis',
    'mutuelleville': 'Tunis', 'el manar': 'Tunis', 'el manar 1': 'Tunis', 'el manar 2': 'Tunis',
    'montplaisir': 'Tunis', 'el omrane': 'Tunis', 'cite olympique': 'Tunis',
    'el ouardia': 'Tunis', 'sijoumi': 'Tunis', 'medina': 'Tunis', 'la medina': 'Tunis',
    'el omrane superieur': 'Tunis', 'el omrane inferieur': 'Tunis',
    'cite ibn khaldoun': 'Tunis', 'cite sportive': 'Tunis',
    'el gorjani': 'Tunis', 'el kabaria': 'Tunis', 'sidi hassine': 'Tunis',
    'el hrairia': 'Tunis', 'el mellassine': 'Tunis', 'ettahrir': 'Tunis',
    'bab bhar': 'Tunis', 'bab souika': 'Tunis', 'el hafsia': 'Tunis',
    'el menzah 1': 'Tunis', 'el menzah 2': 'Tunis', 'el menzah 3': 'Tunis',
    'el menzah 4': 'Tunis', 'el menzah 5': 'Tunis', 'el menzah 6': 'Tunis',
    'el menzah 7': 'Tunis', 'el menzah 8': 'Tunis', 'el menzah 9': 'Tunis',
    'el menzah 10': 'Tunis', 'el menzah 11': 'Tunis',
    'el manar 3': 'Tunis', 'el manar 4': 'Tunis',
    'ennasr 1': 'Tunis', 'ennasr 2': 'Tunis',
    'el mourouj 1': 'Tunis', 'el mourouj 2': 'Tunis',
    'bir el bey': 'Tunis', 'cite el ghazela': 'Tunis', 'ghazela': 'Tunis',
    'l aouina': 'Tunis', 'cite ettahrir': 'Tunis', 'cite ennour': 'Tunis',
    'cite ezzouhour': 'Tunis', 'cite el wafa': 'Tunis',
    'cite ibn rachiq': 'Tunis', 'cite ibn sina': 'Tunis',
    'el mourouj': 'Tunis', 'ezzouhour': 'Tunis',
    'charguia': 'Tunis', 'charguia 1': 'Tunis', 'charguia 2': 'Tunis',
    'cite jardin': 'Tunis', 'cite el intilaka': 'Tunis',
    'le kram': 'Tunis', 'le kram est': 'Tunis', 'le kram ouest': 'Tunis',
    'la goulette': 'Tunis', 'la goultte': 'Tunis',
    'sidi daoud': 'Tunis', 'jardins de cart': 'Tunis',
    'cite du stade': 'Tunis', 'zone urbaine no': 'Tunis',
    'mutuelle ville': 'Tunis', 'monfleury': 'Tunis', 'montfleury': 'Tunis',
    'republique': 'Tunis', 'bab el jazira': 'Tunis',
    'cite el hana': 'Tunis', 'cite bhar lazre': 'Tunis',
    'hedi chaker': 'Tunis', 'cite des juges': 'Tunis',
    'sidi bousaid': 'Tunis', 'amilcar': 'Tunis', 'amilcar carthage': 'Tunis',
    'cite oplympique': 'Tunis', 'cite el khalil': 'Tunis',
    'kheireddine pac': 'Tunis', 'kheireddine': 'Tunis',
    'jardin de': 'Tunis',
    # ── Ariana ────────────────────────────────────────────────────
    'ariana': 'Ariana', 'raoued': 'Ariana', 'la soukra': 'Ariana', 'soukra': 'Ariana',
    'kalaat el andalous': 'Ariana', 'cite el wafa ariana': 'Ariana',
    'cit el wafa afh2': 'Ariana', 'borj louzir': 'Ariana',
    'sidi thabet': 'Ariana', 'mnihla': 'Ariana',
    'ariana ville': 'Ariana', 'ariana medina': 'Ariana',
    'ettadhamen': 'Ariana', 'cité ettadhamen': 'Ariana',
    'cite ennasr ariana': 'Ariana', 'ennasr ariana': 'Ariana',
    'jardins d el menzah': 'Ariana', 'jardins menzah': 'Ariana',
    'cite el ghazala': 'Ariana', 'ghazala': 'Ariana',
    'borj el amri': 'Ariana', 'el battan': 'Ariana',
    'el agba': 'Ariana', 'raoued plage': 'Ariana',
    'riadh landlous': 'Ariana', 'cite ennouzha': 'Ariana',
    'cite ennkhilet': 'Ariana', 'borj el baccouc': 'Ariana',
    'sidi frej': 'Ariana', 'cebalet ben amm': 'Ariana',
    'jabbes': 'Ariana', 'dar fadhal': 'Ariana',
    'aryanah': 'Ariana', 'kalaat landalous': 'Ariana', 'kalaat el andalous': 'Ariana',
    # ── Ben Arous ─────────────────────────────────────────────────
    'ben arous': 'Ben Arous', 'ezzahra': 'Ben Arous', 'hammam lif': 'Ben Arous',
    'borj cedria': 'Ben Arous', 'borj cédria': 'Ben Arous',
    'megrine': 'Ben Arous', 'mégrine': 'Ben Arous',
    'mourouj': 'Ben Arous', 'mourou': 'Ben Arous',
    'rades': 'Ben Arous', 'radès': 'Ben Arous',
    'fouchana': 'Ben Arous', 'mohamedia': 'Ben Arous', 'mornag': 'Ben Arous',
    'boumhel': 'Ben Arous', 'bou mhel': 'Ben Arous', 'bou mhel el basset': 'Ben Arous',
    'el mourouj': 'Ben Arous', 'nouvelle medina': 'Ben Arous',
    'el mourouj 3': 'Ben Arous', 'el mourouj 4': 'Ben Arous',
    'el mourouj 5': 'Ben Arous', 'el mourouj 6': 'Ben Arous',
    'hammam chatt': 'Ben Arous', 'bou argoub ben arous': 'Ben Arous',
    'cite nouvelle': 'Ben Arous', 'khalidia': 'Ben Arous',
    'medina jedida': 'Ben Arous', 'cite ennasr ben arous': 'Ben Arous',
    'cite el wafa ben arous': 'Ben Arous',
    'cite el hidhab': 'Ben Arous', 'el yasminette': 'Ben Arous',
    'naassen': 'Ben Arous', 'village mediter': 'Ben Arous',
    'zone industriel': 'Ben Arous',
    # ── Manouba ───────────────────────────────────────────────────
    'manouba': 'Manouba', 'tebourba': 'Manouba', 'oued ellil': 'Manouba',
    'el mornaguia': 'Manouba', 'douar hicher': 'Manouba', 'cite ettadhamen manouba': 'Manouba',
    'el battan manouba': 'Manouba', 'jedaida': 'Manouba', 'djedeida': 'Manouba',
    'khlidia': 'Manouba', 'cite el bassatine': 'Manouba',
    'cite ennour manouba': 'Manouba', 'cite el wafa manouba': 'Manouba',
    'tantana': 'Manouba', 'la mannouba': 'Manouba',
    'denden': 'Manouba', 'mohamadia manouba': 'Manouba',
    # ── Nabeul ────────────────────────────────────────────────────
    'nabeul': 'Nabeul', 'hammamet': 'Nabeul', 'kelibia': 'Nabeul', 'kélibia': 'Nabeul', 'klibia': 'Nabeul', 'klibya': 'Nabeul',
    'korba': 'Nabeul', 'grombalia': 'Nabeul', 'bou argoub': 'Nabeul',
    'nabeul city': 'Nabeul', 'menzel temime': 'Nabeul', 'soliman': 'Nabeul',
    'el haouaria': 'Nabeul', 'takelsa': 'Nabeul', 'beni khalled': 'Nabeul',
    'hammam ghezaz': 'Nabeul', 'dar chaabane': 'Nabeul', 'el mida': 'Nabeul',
    'hammamet nord': 'Nabeul', 'hammamet sud': 'Nabeul', 'hammamet centre': 'Nabeul',
    'yasmine hammamet': 'Nabeul', 'mrezga': 'Nabeul', 'bir bou regba': 'Nabeul',
    'menzel bou zelfa': 'Nabeul', 'beni khiar': 'Nabeul', 'el alaa nabeul': 'Nabeul',
    'cite erriadh nabeul': 'Nabeul', 'nabeul nord': 'Nabeul', 'nabeul sud': 'Nabeul',
    'hammamet yasmine': 'Nabeul',
    'hammam el ghezaz': 'Nabeul', 'hammam ghezze': 'Nabeul', 'hammam el gheza': 'Nabeul',
    'dar allouche': 'Nabeul', 'dar chabane': 'Nabeul',
    'bni khiar': 'Nabeul', 'tazarka': 'Nabeul', 'kerkouane': 'Nabeul',
    'cite touristiqu': 'Nabeul', 'zone hoteliere': 'Nabeul',
    'cite afh': 'Nabeul', 'cite universita': 'Nabeul',
    'chaabet el mrez': 'Nabeul', 'menzel bouzelfa': 'Nabeul',
    'el maamoura': 'Nabeul', 'barraket essahel': 'Nabeul',
    'bir bouregba': 'Nabeul',
    # ── Sousse ────────────────────────────────────────────────────
    'sousse': 'Sousse', 'skanes': 'Sousse', 'msaken': 'Sousse', 'ksar hellal': 'Sousse',
    'hammam sousse': 'Sousse', 'akouda': 'Sousse', 'kantaoui': 'Sousse',
    'port el kantaoui': 'Sousse', 'enfidha': 'Sousse',
    'sousse center': 'Sousse', 'sousse centre': 'Sousse',
    'sidi bou ali': 'Sousse', 'kalaa kebira': 'Sousse', 'kalaa sghira': 'Sousse',
    'kondar': 'Sousse', 'sousse corniche': 'Sousse', 'sousse ville': 'Sousse',
    'sousse medina': 'Sousse', 'sousse riadh': 'Sousse',
    'sahloul': 'Sousse', 'khezama est': 'Sousse', 'khezama ouest': 'Sousse',
    'sousse khezama': 'Sousse', 'sousse jaouhara': 'Sousse',
    'chatt meriem': 'Sousse', 'bouficha': 'Sousse',
    'cite el wafa sousse': 'Sousse', 'cite erriadh sousse': 'Sousse',
    'cite okba': 'Sousse', 'cite les pins': 'Sousse', 'el kantaoui': 'Sousse',
    'hergla': 'Sousse', 'sidi el hani': 'Sousse',
    'kalaa essghira': 'Sousse', 'kalaat essghira': 'Sousse',
    'kalaa el kebira': 'Sousse', 'kalaa el andlos': 'Sousse',
    'bou ficha': 'Sousse', 'cite de la plag': 'Sousse',
    'cite jaouhara': 'Sousse', 'cit jaouhara': 'Sousse',
    "m'saken": 'Sousse', 'm saken': 'Sousse',
    # ── Monastir ──────────────────────────────────────────────────
    'monastir': 'Monastir', 'skanes monastir': 'Monastir', 'monastir city': 'Monastir',
    'moknine': 'Monastir', 'bekalta': 'Monastir', 'jemmal': 'Monastir',
    'beni hassen': 'Monastir', 'ouerdanine': 'Monastir', 'zeramdine': 'Monastir',
    'teboulba': 'Monastir', 'ksar hellal monastir': 'Monastir',
    'bembla': 'Monastir', 'sayada': 'Monastir', 'lamta': 'Monastir',
    'monastir corniche': 'Monastir', 'monastir centre': 'Monastir',
    'sahline': 'Monastir', 'aghir': 'Monastir',
    'tezdaine': 'Monastir', 'mezraya': 'Monastir',
    'cit de la plage 1': 'Monastir', 'tboulba': 'Monastir',
    'messadine': 'Monastir',
    # ── Sfax ──────────────────────────────────────────────────────
    'sfax': 'Sfax', 'sax': 'Sfax', 'sfax city': 'Sfax', 'sakiet ezzit': 'Sfax',
    'sakiet eddaier': 'Sfax', 'thyna': 'Sfax', 'la shkira': 'Sfax', 'el ain': 'Sfax',
    'agareb': 'Sfax', 'jebeniana': 'Sfax', 'bir ali ben khalifa': 'Sfax', 'mahres': 'Sfax',
    'sfax centre': 'Sfax', 'sfax medina': 'Sfax', 'sfax nord': 'Sfax', 'sfax sud': 'Sfax',
    'route tunis sfax': 'Sfax', 'route gremda': 'Sfax', 'gremda': 'Sfax',
    'el busten': 'Sfax', 'kerkennah': 'Sfax', 'iles kerkennah': 'Sfax',
    'kerkenah': 'Sfax', 'route soukra sfax': 'Sfax',
    'cite el habib sfax': 'Sfax', 'cite erriadh sfax': 'Sfax',
    'caid mhamed': 'Sfax', 'mahrs': 'Sfax', 'menzel chaker': 'Sfax',
    # ── Bizerte ───────────────────────────────────────────────────
    'bizerte': 'Bizerte', 'mateur': 'Bizerte', 'menzel bourguiba': 'Bizerte',
    'menzel jemil': 'Bizerte', 'el alia': 'Bizerte', 'ras jebel': 'Bizerte',
    'ghar el melh': 'Bizerte', 'zarzouna': 'Bizerte',
    'bizerte nord': 'Bizerte', 'bizerte sud': 'Bizerte', 'bizerte centre': 'Bizerte',
    'tinja': 'Bizerte', 'sejnane': 'Bizerte', 'joumine': 'Bizerte',
    'utique': 'Bizerte', 'lac ichkeul': 'Bizerte',
    'raf raf': 'Bizerte', 'metline': 'Bizerte', 'borj touil': 'Bizerte',
    # ── Béja ──────────────────────────────────────────────────────
    'beja': 'Béja', 'béja': 'Béja', 'bja': 'Béja', 'beja city': 'Béja',
    'medjez el bab': 'Béja', 'testour': 'Béja', 'nefza': 'Béja',
    'amdoun': 'Béja', 'thibar': 'Béja', 'goubellat': 'Béja',
    'beja nord': 'Béja', 'beja sud': 'Béja',
    # ── Jendouba ──────────────────────────────────────────────────
    'jendouba': 'Jendouba', 'jendouba city': 'Jendouba', 'tabarka': 'Jendouba',
    'ain draham': 'Jendouba', 'fernana': 'Jendouba', 'ghardimaou': 'Jendouba',
    'bou salem': 'Jendouba', 'oued mliz': 'Jendouba',
    'ain draham ville': 'Jendouba', 'tabarka ville': 'Jendouba',
    # ── Le Kef ────────────────────────────────────────────────────
    'le kef': 'Le Kef', 'el kef': 'Le Kef', 'kef': 'Le Kef',
    'dahmani': 'Le Kef', 'tajerouine': 'Le Kef', 'sers': 'Le Kef',
    'nebeur': 'Le Kef', 'sakiet sidi youssef': 'Le Kef',
    # ── Siliana ───────────────────────────────────────────────────
    'siliana': 'Siliana', 'siliana city': 'Siliana',
    'gaafour': 'Siliana', 'bou arada': 'Siliana', 'makthar': 'Siliana',
    'rouhia': 'Siliana', 'el krib': 'Siliana',
    # ── Kairouan ──────────────────────────────────────────────────
    'kairouan': 'Kairouan', 'kairouan city': 'Kairouan', 'haffouz': 'Kairouan',
    'el ala': 'Kairouan', 'sbikha': 'Kairouan', 'oueslatia': 'Kairouan',
    'nasrallah': 'Kairouan', 'kairouan nord': 'Kairouan', 'kairouan sud': 'Kairouan',
    'el oueslatia': 'Kairouan', 'chebika': 'Kairouan',
    # ── Kasserine ─────────────────────────────────────────────────
    'kasserine': 'Kasserine', 'kasserine city': 'Kasserine', 'sbeitla': 'Kasserine',
    'thala': 'Kasserine', 'feriana': 'Kasserine', 'foussana': 'Kasserine',
    'hassi el ferid': 'Kasserine', 'ezzouhour kasserine': 'Kasserine',
    # ── Sidi Bouzid ───────────────────────────────────────────────
    'sidi bouzid': 'Sidi Bouzid', 'sidi bou zid': 'Sidi Bouzid',
    'sidi bouzid city': 'Sidi Bouzid', 'meknassy': 'Sidi Bouzid',
    'regueb': 'Sidi Bouzid', 'bir el hafey': 'Sidi Bouzid',
    'cebbala ouled asker': 'Sidi Bouzid', 'jilma': 'Sidi Bouzid',
    # ── Mahdia ────────────────────────────────────────────────────
    'mahdia': 'Mahdia', 'mahdia city': 'Mahdia', 'el jem': 'Mahdia',
    'ksour essef': 'Mahdia', 'chebba': 'Mahdia', 'bou merdes': 'Mahdia',
    'hebira': 'Mahdia', 'mahdia plage': 'Mahdia', 'mahdia corniche': 'Mahdia',
    'sidi alouane': 'Mahdia', 'ouled chamekh': 'Mahdia',
    'salakta': 'Mahdia', 'rejiche': 'Mahdia', 'soma': 'Mahdia',
    # ── Gabès ─────────────────────────────────────────────────────
    'gabes': 'Gabès', 'gabès': 'Gabès', 'gabes city': 'Gabès',
    'matmata': 'Gabès', 'mareth': 'Gabès', 'nouvelle matmata': 'Gabès', 'el hamma': 'Gabès',
    'gabes centre': 'Gabès', 'gabes plage': 'Gabès', 'gabes sud': 'Gabès',
    'menzel habib': 'Gabès', 'chenini nahal': 'Gabès',
    'gabs': 'Gabès', 'zrig': 'Gabès',
    # ── Médenine ──────────────────────────────────────────────────
    'medenine': 'Médenine', 'médenine': 'Médenine', 'médénine': 'Médenine',
    'mdenine': 'Médenine', 'zarzis': 'Médenine', 'djerba': 'Médenine',
    'jerba': 'Médenine', 'djerba houmt souk': 'Médenine', 'djerba midoun': 'Médenine',
    'djerba ajim': 'Médenine', 'ben gardane': 'Médenine', 'beni khedache': 'Médenine',
    'houmt souk': 'Médenine', 'midoun': 'Médenine', 'ajim': 'Médenine',
    'el jorf': 'Médenine', 'zarzis plage': 'Médenine', 'zarzis ville': 'Médenine',
    'jerba hood': 'Médenine', 'jerba homt souk': 'Médenine',
    # ── Tataouine ─────────────────────────────────────────────────
    'tataouine': 'Tataouine', 'tataouine city': 'Tataouine',
    'remada': 'Tataouine', 'ghomrassen': 'Tataouine', 'bir lahmar': 'Tataouine',
    'dehiba': 'Tataouine', 'smar': 'Tataouine',
    # ── Gafsa ─────────────────────────────────────────────────────
    'gafsa': 'Gafsa', 'gafsa city': 'Gafsa', 'moularès': 'Gafsa', 'moulares': 'Gafsa',
    'redeyef': 'Gafsa', 'metlaoui': 'Gafsa', 'el ksar': 'Gafsa',
    'om el araies': 'Gafsa', 'sned': 'Gafsa',
    # ── Tozeur ────────────────────────────────────────────────────
    'tozeur': 'Tozeur', 'tozeur city': 'Tozeur',
    'nefta': 'Tozeur', 'tamerza': 'Tozeur', 'degache': 'Tozeur',
    'hazoua': 'Tozeur',
    # ── Kébili ────────────────────────────────────────────────────
    'kebili': 'Kébili', 'kébili': 'Kébili', 'kebili city': 'Kébili',
    'douz': 'Kébili', 'souk lahad': 'Kébili', 'faouar': 'Kébili',
    'el golaa': 'Kébili',
    # ── Zaghouan ──────────────────────────────────────────────────
    'zaghouan': 'Zaghouan', 'zaghouan city': 'Zaghouan', 'zriba': 'Zaghouan',
    'nadhour': 'Zaghouan', 'bir mcherga': 'Zaghouan', 'hammam zriba': 'Zaghouan',
    'ennadhour': 'Zaghouan', 'el fahs': 'Zaghouan',
    # ── Generics → None (treated as Unknown, row dropped) ─────────
    'tunisie': None, 'tunisia': None, 'inconnu': None, 'unknown': None,
    'non precise': None, 'non précisé': None, 'non defini': None,
    'autre': None, 'autres': None, '': None,
}

# ================================================================
# 3. TYPE_BIEN — 9 types + Unknown=0 (rows with 0 are DROPPED)
# ================================================================
TYPE_BIEN_ENC: dict[str, int] = {
    'Unknown': 0, 'Appartement': 1, 'Autre': 2, 'Bureau': 3,
    'Chambre': 4, 'Ferme': 5, 'Local Commercial': 6,
    'Maison': 7, 'Terrain': 8, 'Villa': 9,
}
TYPE_BIEN_DEC: dict[int, str] = {v: k for k, v in TYPE_BIEN_ENC.items()}

TYPE_MAP: dict[str, str] = {
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

TYPE_KW: dict[str, list[str]] = {
    'Appartement':      ['appartement', 'studio', 'duplex', 'triplex', 'penthouse',
                         's+1', 's+2', 's+3', 's+4', 's+5', 'rdc'],
    'Maison':           ['maison', 'house', 'dar', 'bungalow'],
    'Villa':            ['villa', 'propriete'],
    'Chambre':          ['chambre', 'colocation'],
    'Local Commercial': ['local commercial', 'boutique', 'magasin', 'hangar', 'entrepot', 'showroom'],
    'Bureau':           ['bureau', 'office', 'open space', 'coworking'],
    'Terrain':          ['terrain', 'lot', 'lotissement', 'parcelle', 'foncier'],
    'Ferme':            ['ferme', 'agricole', 'oliveraie', 'verger', 'exploitation'],
}

DESC_KW: dict[str, list[str]] = {
    'Appartement':      ['appartement', 'appart', 'studio', 'duplex'],
    'Maison':           ['maison', 'bungalow', 'dar'],
    'Villa':            ['villa'],
    'Chambre':          ['chambre a louer', 'colocation'],
    'Local Commercial': ['local commercial', 'fonds de commerce', 'boutique'],
    'Bureau':           ['bureau', 'office'],
    'Terrain':          ['terrain', 'parcelle'],
    'Ferme':            ['ferme', 'agricole', 'oliveraie'],
}

TYPE_TRANSACTION_ENC: dict[str, int] = {'Location': 1, 'Vente': 2}
TYPE_TRANSACTION_DEC: dict[int, str] = {v: k for k, v in TYPE_TRANSACTION_ENC.items()}

LOC_KEYWORDS: list[str] = [
    'location', 'locat', 'à louer', 'a louer', 'louer', 'rent', 'loue',
    'à la location', 'courte duree', 'courte durée', 'saisonnier', 'saisonniere',
    'vacances', 'vacance', 'meublé', 'meuble', 'meublée', 'airbnb',
    'nuitée', 'nuitee', 'par mois', 'par nuit', 'mensuel', 'mensuelle', 'loyer',
]
VENTE_KEYWORDS: list[str] = [
    'vente', 'à vendre', 'a vendre', 'vendre', 'achat', 'cession',
    'vend', 'mise en vente', 'sale', 'for sale', 'cède', 'cede',
]

CYCLE_MARCHE_ENC: dict[str, int] = {
    'Unknown': 0, 'stabilization': 1, 'growth': 2, 'peak': 3, 'recovery': 4, 'decline': 5,
}
CYCLE_MARCHE_DEC: dict[int, str] = {v: k for k, v in CYCLE_MARCHE_ENC.items()}

GROUPE_MAP: dict[str, str] = {
    'Appartement': 'Residentiel', 'Maison': 'Residentiel',
    'Villa': 'Residentiel', 'Chambre': 'Residentiel',
    'Terrain': 'Foncier', 'Ferme': 'Foncier',
    'Local Commercial': 'Commercial', 'Bureau': 'Commercial',
    'Divers': 'Divers',
}

# Seuils séparés location / vente — hors seuil = ligne SUPPRIMÉE (jamais imputée)
SEUILS: dict[str, dict[str, float]] = {
    'Residentiel': {
        'prix_min_loc':  150,        # loyer min  : 150 TND/mois
        'prix_max_loc':  25_000,     # loyer max  : 25 000 TND/mois
        'prix_min_vent': 15_000,     # vente min  : 15 000 TND
        'prix_max_vent': 20_000_000, # vente max  : 20M TND
        'surf_min': 10, 'surf_max': 2_000,
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

RESEGMENT_MAP: dict[str, str] = {
    'Terrain': 'Foncier', 'Ferme': 'Foncier',
    'Appartement': 'Residentiel', 'Maison': 'Residentiel',
    'Villa': 'Residentiel', 'Chambre': 'Residentiel',
    'Local Commercial': 'Commercial', 'Bureau': 'Commercial',
}

NLP_KW: dict[str, list[str]] = {
    'nlp_parking':   ['parking', 'garage'],
    'nlp_piscine':   ['piscine', 'pool'],
    'nlp_standing':  ['standing', 'luxe', 'prestige'],
    'nlp_neuf':      ['neuf', 'nouvelle construction'],
    'nlp_terrasse':  ['terrasse', 'balcon'],
    'nlp_jardin':    ['jardin', 'garden'],
    'nlp_ascenseur': ['ascenseur'],
    'nlp_climatise': ['climatise', 'climatisation'],
    'nlp_securise':  ['gardiennage', 'securise', 'residence fermee', 'residence securisee'],
    'nlp_meuble':    ['meuble', 'furnished'],
    'nlp_vue_mer':   ['vue mer', 'bord de mer'],
    'nlp_renove':    ['renove', 'refait', 'renovation', 'remis a neuf'],
}


# ================================================================
# ENCODING FUNCTIONS
# ================================================================

def normalize_gouvernorat(val) -> str:
    if pd.isna(val) or str(val).strip() in ('', 'nan', 'None', 'NaN', 'none'):
        return 'Unknown'
    v = str(val).strip().lower()
    if v in VILLE_TO_GOUVERNORAT:
        result = VILLE_TO_GOUVERNORAT[v]
        return result if result else 'Unknown'
    for key, gov in VILLE_TO_GOUVERNORAT.items():
        if not key or not gov:
            continue
        if v.startswith(key) or key.startswith(v) or key in v:
            return gov
    for gov_name in GOUVERNORAT_ENC:
        if gov_name.lower() == v:
            return gov_name
    return 'Unknown'


def encode_gouvernorat(gov_str) -> int:
    if pd.isna(gov_str):
        return 0
    return GOUVERNORAT_ENC.get(str(gov_str).strip(), 0)


def encode_type_bien(val) -> int:
    if pd.isna(val):
        return 0
    return TYPE_BIEN_ENC.get(str(val).strip(), 0)


def std_transaction(val):
    if pd.isna(val):
        return np.nan
    v = str(val).lower().strip()
    if any(x in v for x in ['courte', 'vacance', 'saisonn', 'airbnb', 'nuit']):
        return 1
    if any(x in v for x in ['location', 'locat', 'rent', 'louer', 'loue', 'loyer', 'mensuel']):
        return 1
    if any(x in v for x in ['vent', 'sale', 'achat', 'cession', 'vendre', 'vend']):
        return 2
    return np.nan


def encode_cycle_marche(val) -> int:
    if pd.isna(val):
        return 0
    return CYCLE_MARCHE_ENC.get(str(val).strip(), 0)


def print_encoding_summary():
    print("\n" + "=" * 65)
    print("   ENCODAGES MANUELS FIXES")
    print("=" * 65)
    print("gouvernorat      : " + str({k: v for k, v in list(GOUVERNORAT_ENC.items())[:6]}) + " ...")
    print("type_bien        : " + str(TYPE_BIEN_ENC))
    print("type_transaction : {1: 'Location', 2: 'Vente'}")
    print("cycle_marche     : " + str(CYCLE_MARCHE_ENC))
    print(f"VILLE_TO_GOUVERNORAT : {len(VILLE_TO_GOUVERNORAT)} entrées")
    print("MARKET_CONTEXT   : calculé depuis BCT + INS (external_data.py)")
    print("NEGO_BASE        : calculé depuis INS Immobilier (external_data.py)")
