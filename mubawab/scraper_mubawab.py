#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║   MUBAWAB TUNISIE SCRAPER — Version avec Reprise Automatique        ║
║   Inspiré du BnB Scraper — même structure, même fiabilité           ║
╠══════════════════════════════════════════════════════════════════════╣
║  TYPES SCRAPÉS :                                                    ║
║  ✅ Vente    : Appartements, Maisons, Villas, Bureaux,              ║
║                Locaux, Terrains, Fermes, Divers                     ║
║  ✅ Location : Appartements, Maisons, Villas, Chambres,             ║
║                Bureaux, Locaux, Terrains, Fermes, Divers            ║
║  ✅ Vacances : Appartements, Maisons, Villas, Chambres, Divers      ║
╠══════════════════════════════════════════════════════════════════════╣
║  INSTALLATION :                                                     ║
║  pip install selenium webdriver-manager pandas openpyxl             ║
║                                                                     ║
║  LANCEMENT :                                                        ║
║  python mubawab_scraper.py                                          ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
import json
import time
import re
import os
import logging

# ── Logging (même style BnB)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── Fichier de progression (comme BnB)
PROGRESS_FILE = 'mubawab_progress.json'
DATA_FILE_JSON = 'mubawab_annonces.json'
DATA_FILE_CSV  = 'mubawab_annonces.csv'
DATA_FILE_XLSX = 'mubawab_annonces.xlsx'

BASE_URL = "https://www.mubawab.tn"


# ══════════════════════════════════════════════════════════════════════
# 🏠  TOUTES LES CATÉGORIES À SCRAPER
# ══════════════════════════════════════════════════════════════════════
#
#  Format URL Mubawab :
#  /fr/sc/appartements-a-vendre         → page 1
#  /fr/sc/appartements-a-vendre:p:2     → page 2
#
# ══════════════════════════════════════════════════════════════════════

CATEGORIES = [

    # ── VENTE ─────────────────────────────────────────────────────
    {"slug": "appartements-a-vendre",     "categorie": "vente",    "type_bien": "Appartement"},
    {"slug": "maisons-a-vendre",          "categorie": "vente",    "type_bien": "Maison"},
    {"slug": "villas-a-vendre",           "categorie": "vente",    "type_bien": "Villa"},
    {"slug": "bureaux-a-vendre",          "categorie": "vente",    "type_bien": "Bureau"},
    {"slug": "locaux-commerciaux-a-vendre","categorie": "vente",   "type_bien": "Local commercial"},
    {"slug": "terrains-a-vendre",         "categorie": "vente",    "type_bien": "Terrain"},
    {"slug": "fermes-a-vendre",           "categorie": "vente",    "type_bien": "Ferme"},
    {"slug": "immobilier-divers-a-vendre","categorie": "vente",    "type_bien": "Divers"},

    # ── LOCATION ──────────────────────────────────────────────────
    {"slug": "appartements-a-louer",      "categorie": "location", "type_bien": "Appartement"},
    {"slug": "maisons-a-louer",           "categorie": "location", "type_bien": "Maison"},
    {"slug": "villas-a-louer",            "categorie": "location", "type_bien": "Villa"},
    {"slug": "chambres-a-louer",          "categorie": "location", "type_bien": "Chambre"},
    {"slug": "bureaux-a-louer",           "categorie": "location", "type_bien": "Bureau"},
    {"slug": "locaux-commerciaux-a-louer","categorie": "location", "type_bien": "Local commercial"},
    {"slug": "terrains-a-louer",          "categorie": "location", "type_bien": "Terrain"},
    {"slug": "fermes-a-louer",            "categorie": "location", "type_bien": "Ferme"},
    {"slug": "immobilier-divers-a-louer", "categorie": "location", "type_bien": "Divers"},

    # ── VACANCES ──────────────────────────────────────────────────
    {"slug": "appartements-vacational",   "categorie": "vacances", "type_bien": "Appartement"},
    {"slug": "maisons-vacational",        "categorie": "vacances", "type_bien": "Maison"},
    {"slug": "villas-vacational",         "categorie": "vacances", "type_bien": "Villa"},
    {"slug": "chambres-vacational",       "categorie": "vacances", "type_bien": "Chambre"},
    {"slug": "immobilier-divers-vacational","categorie": "vacances","type_bien": "Divers"},
]


# ══════════════════════════════════════════════════════════════════════
# 🗺️  MAPPING VILLE → GOUVERNORAT (24 gouvernorats)
# ══════════════════════════════════════════════════════════════════════

VILLE_GOV = {
    "tunis":"Tunis","la marsa":"Tunis","carthage":"Tunis","sidi bou saïd":"Tunis",
    "bardo":"Tunis","la goulette":"Tunis","le kram":"Tunis","mégrine":"Tunis",
    "ariana":"Ariana","raoued":"Ariana","la soukra":"Ariana","soukra":"Ariana",
    "sidi thabet":"Ariana","ettadhamen":"Ariana","mnihla":"Ariana",
    "ben arous":"Ben Arous","radès":"Ben Arous","rades":"Ben Arous",
    "hammam lif":"Ben Arous","fouchana":"Ben Arous","mornag":"Ben Arous",
    "manouba":"Manouba","den den":"Manouba","oued ellil":"Manouba","tebourba":"Manouba",
    "nabeul":"Nabeul","hammamet":"Nabeul","kelibia":"Nabeul",
    "korba":"Nabeul","grombalia":"Nabeul","soliman":"Nabeul",
    "zaghouan":"Zaghouan","el fahs":"Zaghouan",
    "bizerte":"Bizerte","menzel bourguiba":"Bizerte","mateur":"Bizerte",
    "béja":"Béja","beja":"Béja","testour":"Béja",
    "jendouba":"Jendouba","tabarka":"Jendouba","ain draham":"Jendouba",
    "le kef":"Kef","kef":"Kef",
    "siliana":"Siliana","makthar":"Siliana",
    "sousse":"Sousse","hammam sousse":"Sousse","el kantaoui":"Sousse",
    "kantaoui":"Sousse","msaken":"Sousse","akouda":"Sousse","enfidha":"Sousse",
    "monastir":"Monastir","moknine":"Monastir","ksar hellal":"Monastir",
    "mahdia":"Mahdia","ksour essef":"Mahdia","el jem":"Mahdia",
    "kasserine":"Kasserine","sbeitla":"Kasserine",
    "sidi bouzid":"Sidi Bouzid",
    "kairouan":"Kairouan",
    "sfax":"Sfax","sakiet ezzit":"Sfax",
    "gabès":"Gabès","gabes":"Gabès","matmata":"Gabès",
    "médenine":"Médenine","medenine":"Médenine","djerba":"Médenine",
    "houmt souk":"Médenine","zarzis":"Médenine","ben gardane":"Médenine",
    "tataouine":"Tataouine",
    "gafsa":"Gafsa","métlaoui":"Gafsa",
    "tozeur":"Tozeur","nefta":"Tozeur",
    "kébili":"Kébili","kebili":"Kébili","douz":"Kébili",
}


class MubawabScraper:
    """
    Scraper Mubawab — même architecture que BnBSeleniumScraper.
    Reprise automatique, sauvegarde incrémentale, tous types.
    """

    def __init__(self, headless=True):
        self.base_url    = BASE_URL
        self.all_annonces = []
        self.driver      = None
        self.headless    = headless
        self.progress    = self._charger_progression()

    # ══════════════════════════════════════════════════════════
    # PROGRESSION (identique BnB)
    # ══════════════════════════════════════════════════════════

    def _charger_progression(self):
        try:
            if os.path.exists(PROGRESS_FILE):
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    p = json.load(f)
                    logger.info(
                        f"📥 Progression chargée: "
                        f"Catégorie {p.get('cat_index',0)+1}/{len(CATEGORIES)}, "
                        f"Page {p.get('last_page',0)}"
                    )
                    return p
        except Exception as e:
            logger.warning(f"Impossible de charger la progression: {e}")
        return {'cat_index': 0, 'last_page': 0, 'annonces_count': 0, 'completed': False}

    def _sauvegarder_progression(self, cat_index, page, total_pages):
        try:
            p = {
                'cat_index':     cat_index,
                'last_page':     page,
                'total_pages':   total_pages,
                'annonces_count': len(self.all_annonces),
                'completed':     (cat_index >= len(CATEGORIES) - 1 and page >= total_pages),
                'timestamp':     time.strftime('%Y-%m-%d %H:%M:%S'),
            }
            with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(p, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Impossible de sauvegarder la progression: {e}")

    def _charger_donnees_existantes(self):
        try:
            if os.path.exists(DATA_FILE_JSON):
                with open(DATA_FILE_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"📥 {len(data)} annonces existantes chargées")
                    return data
        except Exception as e:
            logger.warning(f"Impossible de charger les données existantes: {e}")
        return []

    def reset_progression(self):
        for f in [PROGRESS_FILE, DATA_FILE_JSON, DATA_FILE_CSV, DATA_FILE_XLSX]:
            if os.path.exists(f):
                os.remove(f)
        self.progress = {'cat_index': 0, 'last_page': 0, 'annonces_count': 0, 'completed': False}
        logger.info("✓ Progression réinitialisée")

    # ══════════════════════════════════════════════════════════
    # SELENIUM
    # ══════════════════════════════════════════════════════════

    def setup_driver(self):
        try:
            opts = Options()
            if self.headless:
                opts.add_argument('--headless=new')
            opts.add_argument('--disable-blink-features=AutomationControlled')
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option('useAutomationExtension', False)
            opts.add_argument('--disable-gpu')
            opts.add_argument('--no-sandbox')
            opts.add_argument('--disable-dev-shm-usage')
            opts.add_argument('--window-size=1920,1080')
            opts.add_argument(
                'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=opts)
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            logger.info("✓ Navigateur Chrome démarré")
            return True
        except Exception as e:
            logger.error(f"Erreur démarrage navigateur: {e}")
            return False

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            logger.info("Navigateur fermé")

    def _attendre_chargement(self, timeout=15):
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(1.5)
            return True
        except TimeoutException:
            logger.warning("Timeout chargement page")
            return False

    def _fermer_popup(self):
        for xpath in [
            "//button[contains(text(),'Accepter')]",
            "//button[contains(text(),'Fermer')]",
            "//div[@class='fancybox-close']",
            "//button[@aria-label='Close']",
        ]:
            try:
                self.driver.find_element(By.XPATH, xpath).click()
                time.sleep(0.5)
                return
            except:
                pass

    # ══════════════════════════════════════════════════════════
    # DÉTECTION NOMBRE DE PAGES
    # ══════════════════════════════════════════════════════════

    def _detecter_nb_pages(self, slug):
        """
        Mubawab stocke le total dans :
        <input type="hidden" id="totalElements" value="304" />
        <input type="hidden" id="pageSize" value="33" />
        """
        url = f"{BASE_URL}/fr/sc/{slug}"
        logger.info(f"Détection pages : {url}")
        self.driver.get(url)
        self._attendre_chargement()
        self._fermer_popup()

        # Méthode 1 : champs hidden Mubawab (le plus fiable)
        try:
            total = int(self.driver.find_element(
                By.CSS_SELECTOR, 'input#totalElements').get_attribute('value'))
            size  = int(self.driver.find_element(
                By.CSS_SELECTOR, 'input#pageSize').get_attribute('value') or 33)
            nb = (total + size - 1) // size
            logger.info(f"✓ {total} annonces → {nb} pages (via totalElements)")
            return nb
        except:
            pass

        # Méthode 2 : pagination :p:N dans le HTML
        src = self.driver.page_source
        nums = re.findall(rf'/fr/sc/{re.escape(slug)}:p:(\d+)', src)
        if nums:
            nb = max(int(n) for n in nums)
            logger.info(f"✓ {nb} pages (via liens pagination)")
            return nb

        # Méthode 3 : dernier lien de pagination via XPath
        try:
            links = self.driver.find_elements(By.XPATH, "//ul[@class='pagination']//a")
            nums  = [int(l.text.strip()) for l in links if l.text.strip().isdigit()]
            if nums:
                nb = max(nums)
                logger.info(f"✓ {nb} pages (via boutons pagination)")
                return nb
        except:
            pass

        logger.warning("Une seule page assumée")
        return 1

    # ══════════════════════════════════════════════════════════
    # EXTRACTION PAGE DE LISTE
    # ══════════════════════════════════════════════════════════

    def _scraper_page_liste(self, slug, page, categorie, type_bien):
        """
        Scrape une page de liste Mubawab.
        Retourne une liste d'annonces (infos basiques + URL).
        """
        if page == 1:
            url = f"{BASE_URL}/fr/sc/{slug}"
        else:
            url = f"{BASE_URL}/fr/sc/{slug}:p:{page}"

        logger.info(f"  📄 Page {page} : {url}")
        self.driver.get(url)
        self._attendre_chargement()

        # Scroll pour charger les images lazy
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(1)

        annonces = []

        # Chercher les blocs listingBox (structure HTML Mubawab fournie)
        try:
            blocs = self.driver.find_elements(By.CSS_SELECTOR, 'div.listingBox')
            logger.info(f"    {len(blocs)} annonces trouvées")
        except:
            blocs = []

        if not blocs:
            logger.warning(f"    ⚠️ Aucun bloc trouvé sur page {page}")
            return annonces

        for bloc in blocs:
            try:
                annonce = self._extraire_listing(bloc, categorie, type_bien)
                if annonce:
                    annonces.append(annonce)
            except Exception as e:
                logger.debug(f"Erreur extraction bloc: {e}")
                continue

        return annonces

    def _extraire_listing(self, bloc, categorie, type_bien):
        """
        Extrait les données depuis un bloc listingBox de la page de liste.
        Utilise la structure HTML réelle de Mubawab.
        """
        data = {
            'id_annonce':   '',
            'url':          '',
            'titre':        '',
            'prix':         '',
            'prix_tnd':     '',
            'ville':        '',
            'quartier':     '',
            'gouvernorat':  '',
            'surface_m2':   '',
            'nb_pieces':    '',
            'nb_chambres':  '',
            'nb_sdb':       '',
            'capacite':     '',
            'nuits_min':    '',
            'equipements':  '',
            'image_url':    '',
            'categorie':    categorie,
            'type_bien':    type_bien,
            'source':       'Mubawab.tn',
        }

        # ── ID et URL (depuis linkRef ou lien h2)
        try:
            link_ref = bloc.get_attribute('linkRef')
            if link_ref:
                data['url'] = link_ref
        except:
            pass

        if not data['url']:
            try:
                a = bloc.find_element(By.CSS_SELECTOR, 'h2.listingTit a')
                data['url'] = a.get_attribute('href')
            except:
                pass

        # ID depuis input hidden ou URL
        try:
            data['id_annonce'] = bloc.find_element(
                By.CSS_SELECTOR, 'input.adId').get_attribute('value')
        except:
            if data['url']:
                m = re.search(r'/a/(\d+)/', data['url'])
                if m: data['id_annonce'] = m.group(1)

        # ── Titre
        try:
            data['titre'] = bloc.find_element(
                By.CSS_SELECTOR, 'h2.listingTit a').text.strip()
        except:
            pass

        # ── Prix
        try:
            prix_el  = bloc.find_element(By.CSS_SELECTOR, 'span.priceTag')
            prix_txt = prix_el.text.strip()
            data['prix'] = prix_txt
            m = re.search(r'([\d\s,]+)\s*TND', prix_txt, re.I)
            if m:
                data['prix_tnd'] = m.group(1).replace(' ', '').replace(',', '')
        except:
            pass

        # ── Localisation (format : "El Kantaoui, Hammam Sousse")
        try:
            loc = bloc.find_element(By.CSS_SELECTOR, 'span.listingH3').text.strip()
            loc = re.sub(r'\s+', ' ', loc.replace('\n', ' '))
            parties = [p.strip() for p in loc.split(',')]
            if len(parties) >= 2:
                data['quartier'] = parties[0]
                data['ville']    = parties[1]
            elif parties:
                data['ville'] = parties[0]
            # Gouvernorat
            v = data['ville'].lower().strip()
            data['gouvernorat'] = VILLE_GOV.get(v, '')
        except:
            pass

        # ── Caractéristiques (surface, pièces, chambres, SDB, capacité, nuits min)
        try:
            features = bloc.find_elements(By.CSS_SELECTOR, 'div.adDetailFeature span')
            for feat in features:
                t = feat.text.strip()
                if 'm²' in t:
                    m = re.search(r'([\d,\.]+)', t)
                    if m: data['surface_m2'] = m.group(1).replace(',', '.')
                elif 'Pièce' in t:
                    m = re.search(r'(\d+)', t)
                    if m: data['nb_pieces'] = m.group(1)
                elif 'Chambre' in t:
                    m = re.search(r'(\d+)', t)
                    if m: data['nb_chambres'] = m.group(1)
                elif 'Salle de bain' in t:
                    m = re.search(r'(\d+)', t)
                    if m: data['nb_sdb'] = m.group(1)
                elif 'Capacité' in t:
                    m = re.search(r'(\d+)', t)
                    if m: data['capacite'] = m.group(1)
                elif 'nuit' in t.lower():
                    m = re.search(r'(\d+)', t)
                    if m: data['nuits_min'] = m.group(1)
        except:
            pass

        # ── Équipements (icônes adFeature)
        try:
            equips = bloc.find_elements(By.CSS_SELECTOR, 'div.adFeature span')
            liste_equips = [e.text.strip() for e in equips if e.text.strip()]
            data['equipements'] = ' | '.join(liste_equips)
        except:
            pass

        # ── Image
        try:
            img = bloc.find_element(By.CSS_SELECTOR, 'img.sliderImage.firstPicture')
            src = img.get_attribute('data-lazy') or img.get_attribute('src')
            if src: data['image_url'] = src
        except:
            try:
                img = bloc.find_element(By.TAG_NAME, 'img')
                data['image_url'] = img.get_attribute('src') or ''
            except:
                pass

        # Valider : au moins titre ou URL
        if not data['titre'] and not data['url']:
            return None

        return data

    # ══════════════════════════════════════════════════════════
    # EXTRACTION DÉTAIL ANNONCE
    # ══════════════════════════════════════════════════════════

    def scraper_detail(self, url):
        """
        Scrape les détails complets d'une annonce.
        Visite la page individuelle et extrait tous les champs.
        """
        try:
            self.driver.get(url)
            self._attendre_chargement()
            self._fermer_popup()

            details = {}

            # ── Titre
            try:
                details['titre_detail'] = self.driver.find_element(
                    By.CSS_SELECTOR, 'h1').text.strip()
            except: pass

            # ── Prix
            try:
                prix_el = self.driver.find_element(
                    By.CSS_SELECTOR, 'span.priceTag, div.adPrice, [class*="price"]')
                t = prix_el.text.strip()
                details['prix_detail'] = t
                m = re.search(r'([\d\s,]+)\s*(?:TND|DT)', t, re.I)
                if m: details['prix_tnd_detail'] = m.group(1).replace(' ','').replace(',','')
            except: pass

            # ── Caractéristiques générales (tableau ou liste)
            caract = {}
            try:
                # Format : label | valeur dans des li ou div
                rows = self.driver.find_elements(
                    By.CSS_SELECTOR, 'ul.adCharact li, div.adCharact div, div.caracteristiques li')
                for row in rows:
                    t = row.text.strip()
                    if ':' in t:
                        parts = t.split(':', 1)
                        caract[parts[0].strip()] = parts[1].strip()
                    elif '\n' in t:
                        parts = t.split('\n', 1)
                        caract[parts[0].strip()] = parts[1].strip()
            except: pass

            if caract:
                details['caracteristiques'] = caract
                # Mapper les champs importants
                for k, v in caract.items():
                    kl = k.lower()
                    if 'type' in kl and 'bien' in kl:
                        details['type_bien_detail'] = v
                    elif 'etat' in kl:
                        details['etat'] = v
                    elif 'standing' in kl:
                        details['standing'] = v
                    elif 'étage' in kl or 'etage' in kl:
                        details['etage'] = v
                    elif 'orientation' in kl:
                        details['orientation'] = v
                    elif 'sol' in kl:
                        details['type_sol'] = v
                    elif 'ancienneté' in kl or 'age' in kl:
                        details['anciennete'] = v

            # ── Équipements (checkboxes ou icônes)
            try:
                equip_els = self.driver.find_elements(
                    By.CSS_SELECTOR, 'div.adFeature span, li.adFeature span, '
                                     'ul.amenities li, div.equipements span')
                equips = [e.text.strip() for e in equip_els if e.text.strip()]
                if equips:
                    details['equipements_detail'] = ' | '.join(equips)
            except: pass

            # ── Description
            try:
                desc = self.driver.find_element(
                    By.CSS_SELECTOR, 'div.adDesc, div.description, p.adDescription')
                details['description'] = desc.text.strip()
            except: pass

            # ── Téléphone (après affichage)
            try:
                # Cliquer pour révéler
                btn_tel = self.driver.find_element(
                    By.CSS_SELECTOR, 'a.contactPhoneClick, button.phoneBtn, [class*="phone"]')
                self.driver.execute_script("arguments[0].click();", btn_tel)
                time.sleep(2)

                # Lire le numéro affiché
                tel_els = self.driver.find_elements(
                    By.CSS_SELECTOR, 'p.phoneText, div#phoneCol p, span.phoneNum')
                tels = [t.text.strip() for t in tel_els if re.search(r'\d{7,}', t.text)]
                if tels:
                    details['telephones'] = ' | '.join(tels)
            except: pass

            # ── Agence
            try:
                agence = self.driver.find_element(
                    By.CSS_SELECTOR, 'a[href*="/fr/b/"], div.agencyName a, div.agencyBox h2')
                details['agence'] = agence.text.strip()
                details['agence_url'] = agence.get_attribute('href') or ''
            except: pass

            # ── Images
            try:
                imgs = self.driver.find_elements(
                    By.CSS_SELECTOR, '#masonryPhoto img, div.adGallery img')
                urls = [img.get_attribute('src') for img in imgs if img.get_attribute('src')]
                if urls:
                    details['images'] = ' | '.join(urls[:10])
                    details['nb_images'] = len(urls)
            except: pass

            # ── GPS
            try:
                scripts = self.driver.find_elements(By.TAG_NAME, 'script')
                for sc in scripts:
                    txt = sc.get_attribute('innerHTML') or ''
                    lat_m = re.search(r'"latitude"\s*:\s*"?([\d\.\-]+)"?', txt)
                    lng_m = re.search(r'"longitude"\s*:\s*"?([\d\.\-]+)"?', txt)
                    if lat_m and lng_m:
                        details['latitude']  = lat_m.group(1)
                        details['longitude'] = lng_m.group(1)
                        break
            except: pass

            return details

        except Exception as e:
            logger.error(f"Erreur détail {url}: {e}")
            return {}

    # ══════════════════════════════════════════════════════════
    # SCRAPING PRINCIPAL — avec reprise automatique
    # ══════════════════════════════════════════════════════════

    def scraper_tout(self, max_pages=None, inclure_details=False, sauvegarde_incrementale=True):
        """
        Scrape toutes les catégories, toutes les pages.
        Reprend automatiquement là où on s'est arrêté.
        """
        if not self.setup_driver():
            logger.error("Impossible de démarrer le navigateur")
            return []

        try:
            logger.info("🚀 Démarrage du scraping Mubawab...")

            # Charger données existantes (reprise)
            self.all_annonces = self._charger_donnees_existantes()

            # Index de départ (reprise)
            cat_start  = self.progress.get('cat_index', 0)
            page_start = self.progress.get('last_page', 0) + 1

            for cat_idx in range(cat_start, len(CATEGORIES)):
                cat        = CATEGORIES[cat_idx]
                slug       = cat['slug']
                categorie  = cat['categorie']
                type_bien  = cat['type_bien']

                logger.info(f"\n{'='*60}")
                logger.info(f"📂 [{cat_idx+1}/{len(CATEGORIES)}] {categorie.upper()} — {type_bien}")
                logger.info(f"{'='*60}")

                # Détecter le nombre de pages
                nb_pages = self._detecter_nb_pages(slug)
                if max_pages:
                    nb_pages = min(nb_pages, max_pages)

                # Si reprise sur la même catégorie, reprendre à la bonne page
                p_debut = page_start if cat_idx == cat_start else 1

                for page in range(p_debut, nb_pages + 1):
                    logger.info(f"📄 Page {page}/{nb_pages}")

                    annonces = self._scraper_page_liste(slug, page, categorie, type_bien)

                    # Scraper les détails si demandé
                    if inclure_details:
                        for ann in annonces:
                            if ann.get('url'):
                                time.sleep(1)
                                det = self.scraper_detail(ann['url'])
                                if det:
                                    ann.update(det)

                    self.all_annonces.extend(annonces)

                    # Sauvegarde incrémentale (comme BnB)
                    if sauvegarde_incrementale:
                        self.sauvegarder_json()
                        self.sauvegarder_csv()
                        self._sauvegarder_progression(cat_idx, page, nb_pages)
                        logger.info(
                            f"💾 {len(self.all_annonces)} annonces | "
                            f"Catégorie {cat_idx+1}/{len(CATEGORIES)} | "
                            f"Page {page}/{nb_pages}"
                        )

                    if page < nb_pages:
                        time.sleep(2)

                # Réinitialiser page_start pour les prochaines catégories
                page_start = 1

                # Pause entre catégories
                if cat_idx < len(CATEGORIES) - 1:
                    time.sleep(3)

            logger.info(f"✓ Scraping terminé. Total: {len(self.all_annonces)} annonces")
            return self.all_annonces

        except KeyboardInterrupt:
            logger.warning("\n⚠️  Interruption (Ctrl+C)")
            logger.info("💾 Données sauvegardées automatiquement")
            logger.info(f"📊 {len(self.all_annonces)} annonces collectées")
            logger.info("ℹ️  Relancez pour reprendre")
            return self.all_annonces

        except Exception as e:
            logger.error(f"Erreur: {e}")
            if self.all_annonces:
                self.sauvegarder_json('mubawab_partiel.json')
                self.sauvegarder_csv('mubawab_partiel.csv')
            return self.all_annonces

        finally:
            self.close_driver()

    # ══════════════════════════════════════════════════════════
    # SAUVEGARDE
    # ══════════════════════════════════════════════════════════

    def sauvegarder_json(self, fichier=DATA_FILE_JSON):
        try:
            with open(fichier, 'w', encoding='utf-8') as f:
                json.dump(self.all_annonces, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Erreur sauvegarde JSON: {e}")

    def sauvegarder_csv(self, fichier=DATA_FILE_CSV):
        try:
            if not self.all_annonces: return
            df = pd.DataFrame(self.all_annonces)
            df.to_csv(fichier, index=False, encoding='utf-8-sig')
        except Exception as e:
            logger.error(f"Erreur sauvegarde CSV: {e}")

    def sauvegarder_excel(self, fichier=DATA_FILE_XLSX):
        """Sauvegarde Excel avec onglets par catégorie"""
        try:
            if not self.all_annonces:
                logger.warning("Aucune donnée à sauvegarder")
                return

            df = pd.DataFrame(self.all_annonces)
            df['prix_tnd'] = pd.to_numeric(df.get('prix_tnd', pd.Series()), errors='coerce')
            df['surface_m2'] = pd.to_numeric(df.get('surface_m2', pd.Series()), errors='coerce')

            with pd.ExcelWriter(fichier, engine='openpyxl') as writer:

                # Toutes les annonces
                df.to_excel(writer, sheet_name='📋 Toutes', index=False)

                # Par catégorie
                for cat in ['vente', 'location', 'vacances']:
                    df_cat = df[df.get('categorie', pd.Series()) == cat]
                    if not df_cat.empty:
                        labels = {'vente': '🏷️ Vente', 'location': '🔑 Location', 'vacances': '🌴 Vacances'}
                        df_cat.to_excel(writer, sheet_name=labels[cat], index=False)

                # Stats par gouvernorat
                if 'gouvernorat' in df.columns:
                    stats = df[df['gouvernorat'] != ''].groupby('gouvernorat').agg(
                        nb_annonces  = ('id_annonce', 'count'),
                        prix_moyen   = ('prix_tnd', 'mean'),
                        surface_moy  = ('surface_m2', 'mean'),
                    ).round(1).sort_values('nb_annonces', ascending=False)
                    stats.to_excel(writer, sheet_name='🗺️ Stats Gouvernorats')

            logger.info(f"✓ Excel sauvegardé → {fichier}")
            return df

        except Exception as e:
            logger.error(f"Erreur sauvegarde Excel: {e}")


# ══════════════════════════════════════════════════════════════════════
# 🚀  MAIN (même style que BnB)
# ══════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  MUBAWAB TUNISIE SCRAPER — Reprise Automatique")
    print("=" * 60)
    print()

    scraper = MubawabScraper()

    # ── Reprise détectée (comme BnB)
    if scraper.progress.get('cat_index', 0) > 0 or scraper.progress.get('last_page', 0) > 0:
        cat_idx = scraper.progress.get('cat_index', 0)
        page    = scraper.progress.get('last_page', 0)
        cat     = CATEGORIES[cat_idx] if cat_idx < len(CATEGORIES) else {}

        print("🔄 REPRISE DÉTECTÉE")
        print("=" * 60)
        print(f"  Catégorie  : {cat.get('categorie','?')} — {cat.get('type_bien','?')}")
        print(f"  Dernière page : {page}")
        print(f"  Annonces déjà collectées : {scraper.progress.get('annonces_count', 0)}")
        print(f"  Date : {scraper.progress.get('timestamp', 'N/A')}")
        print("=" * 60)
        print()
        print("  1. Reprendre où j'en étais")
        print("  2. Recommencer à zéro")
        choix = input("\nVotre choix (1 ou 2) [défaut: 1]: ").strip() or "1"

        if choix == "2":
            scraper.reset_progression()
            print("✓ Redémarrage à zéro")
        else:
            print(f"✓ Reprise à la page {page + 1}")
        print()

    # ── Mode navigateur
    print("Mode d'exécution:")
    print("  1. Mode visible (voir le navigateur)")
    print("  2. Mode invisible (headless)")
    mode = input("\nVotre choix (1 ou 2) [défaut: 2]: ").strip() or "2"
    scraper.headless = (mode == "2")

    # ── Nombre de pages
    print("\nNombre de pages par catégorie:")
    nb_pages_input = input("Nombre (Entrée = toutes les pages): ").strip()
    max_pages = int(nb_pages_input) if nb_pages_input.isdigit() else None

    # ── Détails complets
    print("\nScraper les détails complets de chaque annonce?")
    print("  (téléphone, description, images, GPS, équipements complets)")
    det_input = input("Oui/Non [défaut: Non]: ").strip().lower()
    inclure_details = det_input in ['oui', 'o', 'yes', 'y']

    # ── Résumé
    print("\n" + "=" * 60)
    print("CONFIGURATION :")
    print(f"  Mode          : {'Invisible' if scraper.headless else 'Visible'}")
    print(f"  Pages max     : {max_pages if max_pages else 'Toutes'}")
    print(f"  Détails       : {'Oui' if inclure_details else 'Non'}")
    print(f"  Sauvegarde    : Après chaque page ✓")
    print(f"  Reprise auto  : Activée ✓")
    print(f"  Catégories    : {len(CATEGORIES)} (vente + location + vacances)")
    print("=" * 60)
    print()
    print("💡 Astuce : Ctrl+C pour interrompre — relancez pour reprendre !")
    print()

    # ── Lancer
    annonces = scraper.scraper_tout(
        max_pages=max_pages,
        inclure_details=inclure_details,
        sauvegarde_incrementale=True,
    )

    # ── Sauvegarde finale
    if annonces:
        scraper.sauvegarder_json()
        scraper.sauvegarder_csv()
        scraper.sauvegarder_excel()

        print("\n" + "=" * 60)
        print("RÉSUMÉ FINAL")
        print("=" * 60)
        print(f"  Total annonces  : {len(annonces)}")
        print(f"  Vente           : {sum(1 for a in annonces if a.get('categorie')=='vente')}")
        print(f"  Location        : {sum(1 for a in annonces if a.get('categorie')=='location')}")
        print(f"  Vacances        : {sum(1 for a in annonces if a.get('categorie')=='vacances')}")
        print()
        print("✓ Fichiers créés :")
        print(f"  - {DATA_FILE_JSON}")
        print(f"  - {DATA_FILE_CSV}")
        print(f"  - {DATA_FILE_XLSX}")
        print(f"  - {PROGRESS_FILE}")
        print("=" * 60)

        # Premier exemple
        print("\nPremier exemple :")
        print(json.dumps(annonces[0], indent=2, ensure_ascii=False)[:400] + "...")

    else:
        print("\n⚠️ Aucune annonce extraite!")


if __name__ == "__main__":
    main()