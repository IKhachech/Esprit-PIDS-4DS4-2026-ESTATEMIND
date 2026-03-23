from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json, time, re, logging, os, csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROGRESS_FILE  = 'ta_scraping_progress.json'
OUTPUT_JSON    = 'ta_properties.json'
OUTPUT_CSV     = 'ta_properties.csv'
OUTPUT_VENTE_JSON  = 'ta_vente.json'
OUTPUT_VENTE_CSV   = 'ta_vente.csv'
OUTPUT_LOCATION_JSON = 'ta_location.json'
OUTPUT_LOCATION_CSV  = 'ta_location.csv'

BASE_URL      = "http://www.tunisie-annonce.com"
LISTING_URL   = (BASE_URL + "/AnnoncesImmobilier.asp"
                 "?rech_cod_cat=1&rech_cod_pay=TN&rech_order_by=31&rech_page_num={page}")
DETAIL_URL    = BASE_URL + "/Details_Annonces_Immobilier.asp?cod_ann={cod_ann}"

# ──────────────────────────────────────────────
# Extraction helpers
# ──────────────────────────────────────────────

def _extract_phones(text):
    """Extract all Tunisian phone numbers from a text block."""
    # Match formats: 98 305 239 / 99 984 810 / 71234567 / +21698...
    pattern = r'(?:\+216\s?)?(?:\d{2}[\s.\-]?\d{3}[\s.\-]?\d{3}|\d{8})'
    raw = re.findall(pattern, text)
    cleaned = []
    for p in raw:
        digits = re.sub(r'\D', '', p)
        # Keep Tunisian numbers: 8 digits local or 11 with country code
        if len(digits) in (8, 11):
            cleaned.append(digits[-8:])  # normalise to 8 digits
    # Remove duplicates while preserving order
    seen = set()
    result = []
    for p in cleaned:
        if p not in seen:
            seen.add(p)
            result.append(p)
    return result


def _extract_surface(text):
    """
    Extract surface info from description text.
    Returns dict with keys: surface_couverte, surface_jardin, surface_totale, surface_terrain
    """
    surfaces = {}
    # surface couverte / habitable / bâtie
    m = re.search(r'(\d+)\s*m[²2]?\s*(?:de\s+)?(?:surface?\s+)?(?:couverte?|habitable?|bâtie?|living)', text, re.I)
    if m:
        surfaces['surface_couverte'] = int(m.group(1))

    # jardin / terrasse
    m = re.search(r'(\d+)\s*m[²2]?\s*(?:de\s+)?(?:jardin|terrasse|balcon)', text, re.I)
    if m:
        surfaces['surface_jardin'] = int(m.group(1))

    # terrain / lot
    m = re.search(r'(\d+)\s*m[²2]?\s*(?:de\s+)?(?:terrain|lot)', text, re.I)
    if m:
        surfaces['surface_terrain'] = int(m.group(1))

    # Generic "75 m²" — if no couverte yet, assume it's the main surface
    if 'surface_couverte' not in surfaces:
        m = re.search(r'(\d+)\s*m[²2]', text, re.I)
        if m:
            surfaces['surface_couverte'] = int(m.group(1))

    return surfaces


def _extract_rooms(text):
    """Extract room/bedroom counts from description."""
    rooms = {}

    # S+N pattern (Tunisian convention: S=living room, N=bedrooms)
    m = re.search(r'\bS\+(\d+)\b', text, re.I)
    if m:
        n = int(m.group(1))
        rooms['type_logement'] = f"S+{n}"
        rooms['nombre_chambres'] = n
        rooms['nombre_pieces'] = n + 1  # +1 for the salon

    # explicit mentions
    m = re.search(r'(\d+)\s*(?:chambre?s?|bedroom)', text, re.I)
    if m and 'nombre_chambres' not in rooms:
        rooms['nombre_chambres'] = int(m.group(1))

    m = re.search(r'(\d+)\s*(?:pi[èe]ce?s?|room)', text, re.I)
    if m and 'nombre_pieces' not in rooms:
        rooms['nombre_pieces'] = int(m.group(1))

    m = re.search(r'(\d+)\s*(?:salle?s?\s*(?:de\s*bain?|d\'eau)|(?:SDB|sdb))', text, re.I)
    if m:
        rooms['nombre_salles_bain'] = int(m.group(1))

    m = re.search(r'(\d+)\s*(?:salon|living)', text, re.I)
    if m:
        rooms['nombre_salons'] = int(m.group(1))

    return rooms


def _extract_misc_features(text):
    """Extract misc features: floor, furnished, parking, pool, etc."""
    feats = {}

    # Étage
    m = re.search(r'(?:(?:au|[àa])\s+)?(\d+)[eè]?(?:r|ème|er|eme)?\s+étage?', text, re.I)
    if m:
        feats['etage'] = int(m.group(1))
    elif re.search(r'\brez\s*[-–]?\s*de\s*[-–]?\s*chauss[ée]e?\b', text, re.I):
        feats['etage'] = 0

    # Meublé
    if re.search(r'\bmeuble?|furnished\b', text, re.I):
        feats['meuble'] = True
    elif re.search(r'\bnon\s+meuble?\b', text, re.I):
        feats['meuble'] = False

    # Parking / Garage
    if re.search(r'\bparking|garage\b', text, re.I):
        feats['parking'] = True

    # Piscine
    if re.search(r'\bpiscine|pool\b', text, re.I):
        feats['piscine'] = True

    # Ascenseur
    if re.search(r'\bascenseur|elevator\b', text, re.I):
        feats['ascenseur'] = True

    # Standing
    m = re.search(r'\b(haut\s+standing|standing|luxe|luxury)\b', text, re.I)
    if m:
        feats['standing'] = m.group(1).strip().lower()

    # Année de construction
    m = re.search(r'(?:construi[t]?\s+en|ann[ée]e?\s+(?:de\s+)?construction\s*:?\s*)(\d{4})', text, re.I)
    if m:
        feats['annee_construction'] = int(m.group(1))

    # Neuf / Ancien
    if re.search(r'\bneuf\b|\bnew\b', text, re.I):
        feats['etat'] = 'neuf'
    elif re.search(r'\brénov[eé]|\bréhabilit|\brenovated\b', text, re.I):
        feats['etat'] = 'rénové'
    elif re.search(r'\bancien\b', text, re.I):
        feats['etat'] = 'ancien'

    return feats


def _is_location(prop):
    """Determine if a property is for rent (location) or sale (vente)."""
    indicators = [
        prop.get('nature', ''),
        prop.get('titre', ''),
        prop.get('titre_complet', ''),
        prop.get('description_courte', ''),
    ]
    text = ' '.join(str(x) for x in indicators).lower()

    location_keywords = ['louer', 'location', 'loué', 'à louer', 'a louer', 'mensuel',
                         'mensuelle', '/mois', 'par mois', 'rent', 'rental']
    vente_keywords    = ['vendre', 'vente', 'à vendre', 'a vendre', 'achat', 'sale']

    score_loc  = sum(1 for kw in location_keywords if kw in text)
    score_vent = sum(1 for kw in vente_keywords   if kw in text)

    if score_loc > score_vent:
        return True
    if score_vent > score_loc:
        return False
    # Default: check price unit — if DT/mois → location
    prix = str(prop.get('prix_texte', '') + prop.get('prix_complet', '')).lower()
    if 'mois' in prix or '/m' in prix:
        return True
    return False  # Default: vente


def _extract_image_urls(driver, page_source):
    """
    Extract all property image URLs from a detail page.
    Tries multiple selectors used by tunisie-annonce, fixes relative URLs,
    deduplicates, and filters out icons/logos.
    """
    found = []
    seen  = set()

    def _add(url):
        if not url or url in seen:
            return
        # Make relative URLs absolute
        if url.startswith('//'):
            url = 'http:' + url
        elif url.startswith('/'):
            url = BASE_URL + url
        # Filter out tiny icons, spacers, logos (heuristic: skip if <10 chars or common patterns)
        low = url.lower()
        if any(x in low for x in ['spacer', 'pixel', 'blank', 'logo', 'icon', 'flag',
                                    'bullet', 'arrow', 'btn_', 'button', 'loading']):
            return
        # Keep only image extensions or tunisie-annonce image paths
        if not re.search(r'\.(jpg|jpeg|png|gif|webp|bmp)(\?|$)', low) \
           and 'photo' not in low and 'image' not in low and 'img' not in low:
            return
        seen.add(url)
        found.append(url)

    # ── 1. Primary selector: PhotoAnnonce class (the main property photos) ──
    for selector in [
        "img.PhotoAnnonce",
        "img[class*='photo']",
        "img[class*='Photo']",
        "td.PhotoAnnonce img",
        "div.photos img",
        "div.gallery img",
        "a[href*='photo'] img",
        "a[rel='lightbox'] img",
        "a[data-lightbox] img",
    ]:
        try:
            imgs = driver.find_elements(By.CSS_SELECTOR, selector)
            for img in imgs:
                src = img.get_attribute("src") or ""
                # Also check data-src for lazy-loaded images
                dsrc = img.get_attribute("data-src") or ""
                _add(src)
                _add(dsrc)
        except Exception:
            pass

    # ── 2. Anchor hrefs pointing to image files (clickable photo links) ──
    try:
        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
        for a in anchors:
            href = a.get_attribute("href") or ""
            if re.search(r'\.(jpg|jpeg|png|gif|webp)(\?|$)', href, re.I):
                _add(href)
    except Exception:
        pass

    # ── 3. Regex fallback on raw page source ──
    if not found:
        for pattern in [
            r'src=["\']([^"\']*\.(?:jpg|jpeg|png|gif|webp)[^"\']*)["\']',
            r'href=["\']([^"\']*(?:photo|image|img)[^"\']*\.(?:jpg|jpeg|png|gif|webp)[^"\']*)["\']',
        ]:
            for match in re.finditer(pattern, page_source, re.I):
                _add(match.group(1))

    # ── 4. Remove duplicates caused by thumbnail vs full-size variants ──
    # tunisie-annonce often has: /photos/thumb/xxx.jpg  and  /photos/xxx.jpg
    # Prefer the full-size version (without 'thumb' in path)
    full_size = [u for u in found if 'thumb' not in u.lower() and '_t.' not in u.lower()]
    if full_size:
        return full_size
    return found


# ──────────────────────────────────────────────
# Main scraper class
# ──────────────────────────────────────────────

class TunisieAnnonceScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.all_properties = []
        self.driver = None
        self.progress = self._load_progress()

    def _load_progress(self):
        try:
            if os.path.exists(PROGRESS_FILE):
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    p = json.load(f)
                logger.info(f"📥 Progression chargée : page {p.get('last_page',0)}/{p.get('total_pages','?')}")
                return p
        except Exception as e:
            logger.warning(f"Impossible de charger la progression : {e}")
        return {'last_page': 0, 'total_pages': None, 'properties_count': 0, 'completed': False}

    def _save_progress(self, page_number, total_pages):
        try:
            p = {'last_page': page_number, 'total_pages': total_pages,
                 'properties_count': len(self.all_properties),
                 'completed': page_number >= total_pages,
                 'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')}
            with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(p, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Impossible de sauvegarder la progression : {e}")

    def reset_progress(self):
        for f in [PROGRESS_FILE, OUTPUT_JSON, OUTPUT_CSV,
                  OUTPUT_VENTE_JSON, OUTPUT_VENTE_CSV,
                  OUTPUT_LOCATION_JSON, OUTPUT_LOCATION_CSV]:
            if os.path.exists(f):
                os.remove(f)
        self.progress = {'last_page': 0, 'total_pages': None, 'properties_count': 0, 'completed': False}
        logger.info("✓ Progression réinitialisée")

    def _load_existing_data(self):
        try:
            if os.path.exists(OUTPUT_JSON):
                with open(OUTPUT_JSON, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"📥 {len(data)} annonces existantes chargées")
                return data
        except Exception as e:
            logger.warning(f"Impossible de charger les données existantes : {e}")
        return []

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
            ua = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            opts.add_argument(f'user-agent={ua}')
            self.driver = webdriver.Chrome(options=opts)
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": ua})
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("✓ Navigateur Chrome démarré")
            return True
        except Exception as e:
            logger.error(f"Erreur démarrage navigateur : {e}")
            return False

    def close_driver(self):
        if self.driver:
            self.driver.quit()

    def _wait_page(self, timeout=15):
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete")
            time.sleep(1.5)
            return True
        except TimeoutException:
            return False

    def get_total_pages(self):
        try:
            self.driver.get(LISTING_URL.format(page=1))
            self._wait_page()
            matches = re.findall(r'rech_page_num=(\d+)', self.driver.page_source)
            if matches:
                max_page = max(int(m) for m in matches)
                logger.info(f"✓ Nombre total de pages : {max_page}")
                return max_page
        except Exception as e:
            logger.error(f"Erreur get_total_pages : {e}")
        return 1

    def scrape_listing_page(self, page_number):
        url = LISTING_URL.format(page=page_number)
        logger.info(f"Scraping page {page_number} : {url}")
        self.driver.get(url)
        self._wait_page()
        properties = []
        try:
            rows = self.driver.find_elements(By.CSS_SELECTOR, "tr.Tableau1")
            logger.info(f"  → {len(rows)} lignes trouvées")
            for row in rows:
                try:
                    prop = self._extract_row_data(row)
                    if prop:
                        properties.append(prop)
                except Exception as e:
                    logger.debug(f"Erreur ligne : {e}")
        except Exception as e:
            logger.error(f"Erreur page {page_number} : {e}")
            with open(f"ta_page_{page_number}_debug.html", 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
        logger.info(f"  ✓ {len(properties)} annonces extraites")
        return properties

    def _extract_row_data(self, row):
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 12:
            return None
        prop = {}

        # Région (cellule 1)
        try:
            a = cells[1].find_element(By.TAG_NAME, "a")
            prop['region'] = a.text.strip()
            href = a.get_attribute("href") or ""
            for param in ['rech_cod_vil', 'rech_cod_loc']:
                m = re.search(rf'{param}=(\w+)', href)
                if m: prop[param] = m.group(1)
            tooltip = a.get_attribute("onmouseover") or ""
            for label, key in [('Gouvernorat', 'gouvernorat'), ('gation', 'delegation'), ('Localit', 'localite')]:
                m = re.search(rf'{label}.*?:\s*([^<&\n]+)', tooltip)
                if m:
                    val = m.group(1).strip()
                    # Strip trailing JS artifacts: '); or "); or similar
                    val = re.sub(r'''[\'")\s;]+$''', '', val).strip()
                    prop[key] = val
        except Exception:
            prop['region'] = cells[1].text.strip()

        # Nature (3), Type (5)
        try: prop['nature']    = cells[3].text.strip()
        except Exception: pass
        try: prop['type_bien'] = cells[5].text.strip()
        except Exception: pass

        # Titre + URL + cod_ann (cellule 7)
        try:
            a = cells[7].find_element(By.TAG_NAME, "a")
            prop['titre'] = a.text.strip()
            href = a.get_attribute("href") or ""
            prop['url'] = href if href.startswith("http") else BASE_URL + "/" + href.lstrip("/")
            cod_m = re.search(r'cod_ann=(\d+)', href)
            if cod_m: prop['cod_ann'] = cod_m.group(1)
            # Description courte depuis tooltip
            tooltip = a.get_attribute("onmouseover") or ""
            desc_m  = re.search(r'<br/>(.*)', tooltip, re.DOTALL)
            if desc_m:
                raw = re.sub(r'&lt;[^&]*&gt;', '', desc_m.group(1))
                raw = raw.replace('&amp;', '&').replace('&nbsp;', ' ').replace("&lt;br/&gt;", ' ').strip()
                prop['description_courte'] = raw[:500]
            # Photo / Professionnel
            try:
                cells[7].find_element(By.CSS_SELECTOR, "img[alt='avec photo']")
                prop['avec_photo'] = True
            except NoSuchElementException:
                prop['avec_photo'] = False
            try:
                cells[7].find_element(By.CSS_SELECTOR, "img[alt='professionnel']")
                prop['professionnel'] = True
            except NoSuchElementException:
                prop['professionnel'] = False
            # Thumbnail image from listing row (if present)
            try:
                thumb_imgs = cells[7].find_elements(By.CSS_SELECTOR, "img[src]")
                for img in thumb_imgs:
                    src = img.get_attribute("src") or ""
                    alt = (img.get_attribute("alt") or "").lower()
                    # Skip badge icons (avec photo / professionnel)
                    if alt in ('avec photo', 'professionnel', ''):
                        continue
                    if re.search(r'\.(jpg|jpeg|png|gif|webp)', src, re.I):
                        thumb = src if src.startswith("http") else BASE_URL + "/" + src.lstrip("/")
                        prop['thumbnail'] = thumb
                        break
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"Erreur titre : {e}")
            return None

        # Prix (cellule 9)
        try:
            prix_text = cells[9].text.strip().replace('\xa0', ' ')
            prop['prix_texte'] = prix_text
            tooltip = cells[9].get_attribute("onmouseover") or ""
            m = re.search(r'<b>(.*?)</b>', tooltip)
            if m: prop['prix_complet'] = m.group(1).strip()
            num_m = re.search(r'[\d\s]+', prix_text)
            if num_m:
                val = num_m.group(0).replace(' ', '').strip()
                if val.isdigit(): prop['prix_montant'] = int(val)
        except Exception: pass

        # Date (cellule 11)
        try:
            prop['date_modification'] = cells[11].text.strip()
            tooltip = cells[11].get_attribute("onmouseover") or ""
            ins_m = re.search(r'Ins.r.e le.*?:\s*([\d/: ]+)', tooltip)
            upd_m = re.search(r'Mise.*?jour.*?:\s*([\d/: ]+)', tooltip)
            if ins_m: prop['date_insertion']   = ins_m.group(1).strip()
            if upd_m: prop['date_mise_a_jour'] = upd_m.group(1).strip()
        except Exception: pass

        # ── Extract extra features from description_courte already available ──
        desc = prop.get('description_courte', '') + ' ' + prop.get('titre', '')
        prop.update(_extract_rooms(desc))
        prop.update(_extract_surface(desc))
        prop.update(_extract_misc_features(desc))

        # Quick phone extraction from listing tooltip (bonus)
        phones = _extract_phones(desc)
        if phones:
            prop['telephones_listing'] = phones

        if not prop.get('titre') and not prop.get('cod_ann'):
            return None
        return prop

    def scrape_detail(self, cod_ann):
        """
        Fetch the detail page and extract:
         - titre_complet
         - description_complete
         - telephones  ← properly extracted from the td.da_field_text cell
         - images
         - enriched features (rooms, surface, misc) from full description
        """
        url = DETAIL_URL.format(cod_ann=cod_ann)
        try:
            self.driver.get(url)
            self._wait_page(timeout=10)
            details = {'url_detail': url}
            source  = self.driver.page_source

            # Titre complet
            try:
                h = self.driver.find_element(By.CSS_SELECTOR, "td.SousTitre1Vert")
                details['titre_complet'] = h.text.strip()
            except Exception: pass

            # ── Description complète ──────────────────────────────────────────
            # Try the specific cell first (da_field_text), then fallback
            description_text = ''
            for selector in ["td.da_field_text", "td.CorpsAnnonce", "div.da_field_text"]:
                try:
                    elems = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elems:
                        description_text = ' '.join(e.text.strip() for e in elems if e.text.strip())
                        if description_text:
                            details['description_complete'] = description_text
                            break
                except Exception:
                    pass

            # ── Phone extraction — from visible text cells (NOT page source regex) ──
            # Collect text from all "field" cells which hold contact info
            all_text_for_phones = description_text

            # Also look in contact-specific cells
            for selector in ["td.da_field_text", "td.ContactInfo", "td.contact",
                              "span.tel", "td[class*='contact']", "td[class*='phone']"]:
                try:
                    elems = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for el in elems:
                        all_text_for_phones += ' ' + el.text
                except Exception:
                    pass

            phones = _extract_phones(all_text_for_phones)

            # Fallback: scan the full page text (but use the proper multi-phone function)
            if not phones:
                try:
                    body_text = self.driver.find_element(By.TAG_NAME, 'body').text
                    phones = _extract_phones(body_text)
                except Exception:
                    pass

            if phones:
                details['telephones'] = phones
                details['telephone_principal'] = phones[0]

            # ── Images ──────────────────────────────────────────────────────
            image_urls = _extract_image_urls(self.driver, source)
            if image_urls:
                details['images']    = image_urls
                details['nb_images'] = len(image_urls)
                details['image_principale'] = image_urls[0]
                # Also store individual columns for CSV compatibility
                for i, img_url in enumerate(image_urls, 1):
                    details[f'image_{i}'] = img_url

            # ── Enrich with extracted features from full description ─────────
            if description_text:
                details.update(_extract_rooms(description_text))
                details.update(_extract_surface(description_text))
                details.update(_extract_misc_features(description_text))

            return details
        except Exception as e:
            logger.warning(f"Erreur détail {cod_ann} : {e}")
            return None

    def _enrich_property(self, prop):
        """Merge detail fields into the top-level property dict and add transaction type."""
        details = prop.pop('details', {}) or {}
        for k, v in details.items():
            # Don't overwrite existing values from listing, except for richer fields
            if k not in prop or k in ('telephones', 'telephone_principal', 'description_complete',
                                       'images', 'nb_images', 'titre_complet'):
                prop[k] = v
        # Determine transaction type
        prop['type_transaction'] = 'location' if _is_location(prop) else 'vente'
        return prop

    def save_to_json(self, data, filename):
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Erreur JSON ({filename}) : {e}")

    def save_to_csv(self, data, filename):
        try:
            if not data: return
            all_keys = set()
            for p in data:
                all_keys.update(k for k in p if k != 'images')  # exclude raw list; use image_N cols
            # Sort with important fields first
            priority = ['cod_ann', 'titre', 'type_transaction', 'region', 'gouvernorat',
                        'delegation', 'localite', 'nature', 'type_bien',
                        'nombre_chambres', 'nombre_pieces', 'surface_couverte',
                        'surface_jardin', 'surface_terrain', 'etage', 'meuble',
                        'parking', 'piscine', 'ascenseur', 'standing', 'etat',
                        'annee_construction', 'prix_montant', 'prix_texte', 'prix_complet',
                        'telephone_principal', 'telephones', 'telephones_listing',
                        'avec_photo', 'professionnel', 'nb_images', 'thumbnail',
                        'image_principale', 'image_1', 'image_2', 'image_3',
                        'image_4', 'image_5', 'image_6', 'image_7', 'image_8',
                        'date_modification', 'date_insertion', 'date_mise_a_jour', 'url']
            remaining = sorted(all_keys - set(priority))
            fieldnames = [f for f in priority if f in all_keys] + remaining

            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore',
                                        quoting=csv.QUOTE_ALL)
                writer.writeheader()
                for p in data:
                    row = {k: p.get(k, '') for k in fieldnames}
                    # Flatten lists for CSV
                    if isinstance(row.get('telephones'), list):
                        row['telephones'] = ' / '.join(row['telephones'])
                    if isinstance(row.get('telephones_listing'), list):
                        row['telephones_listing'] = ' / '.join(row['telephones_listing'])
                    # Replace newlines inside text fields with a space (prevents broken rows)
                    for k in ('description_complete', 'description_courte', 'titre', 'titre_complet'):
                        if row.get(k):
                            row[k] = re.sub(r'[\r\n]+', ' ', str(row[k])).strip()
                    writer.writerow(row)
        except Exception as e:
            logger.error(f"Erreur CSV ({filename}) : {e}")

    def _save_all(self):
        """Save all properties + split by vente/location."""
        self.save_to_json(self.all_properties, OUTPUT_JSON)
        self.save_to_csv(self.all_properties, OUTPUT_CSV)

        vente    = [p for p in self.all_properties if p.get('type_transaction') == 'vente']
        location = [p for p in self.all_properties if p.get('type_transaction') == 'location']

        self.save_to_json(vente,    OUTPUT_VENTE_JSON)
        self.save_to_csv(vente,     OUTPUT_VENTE_CSV)
        self.save_to_json(location, OUTPUT_LOCATION_JSON)
        self.save_to_csv(location,  OUTPUT_LOCATION_CSV)

        logger.info(f"💾 Total: {len(self.all_properties)} | Vente: {len(vente)} | Location: {len(location)}")

    def scrape_all(self, max_pages=None, include_details=False, save_incremental=True):
        if not self.setup_driver():
            return []
        try:
            self.all_properties = self._load_existing_data()
            total_pages = self.get_total_pages()
            if max_pages:
                total_pages = min(total_pages, max_pages)
            start_page = self.progress.get('last_page', 0) + 1
            if start_page > 1:
                logger.info(f"🔄 REPRISE : page {start_page}/{total_pages}")

            for page_num in range(start_page, total_pages + 1):
                logger.info(f"📄 Page {page_num}/{total_pages}")
                page_props = self.scrape_listing_page(page_num)

                if include_details:
                    for prop in page_props:
                        if prop.get('cod_ann'):
                            time.sleep(1)
                            det = self.scrape_detail(prop['cod_ann'])
                            if det:
                                prop['details'] = det
                        self._enrich_property(prop)
                else:
                    for prop in page_props:
                        prop['type_transaction'] = 'location' if _is_location(prop) else 'vente'

                self.all_properties.extend(page_props)

                if save_incremental:
                    self._save_all()
                    self._save_progress(page_num, total_pages)

                if page_num < total_pages:
                    time.sleep(2)

            self._save_progress(total_pages, total_pages)
            logger.info(f"✓ Terminé. Total : {len(self.all_properties)} annonces")
            return self.all_properties

        except KeyboardInterrupt:
            logger.warning("\n⚠️  Interruption — relancez pour reprendre")
            self._save_all()
            return self.all_properties
        except Exception as e:
            logger.error(f"Erreur : {e}")
            if self.all_properties:
                self._save_all()
                self.save_to_json(self.all_properties, 'ta_properties_partial.json')
            return self.all_properties
        finally:
            self.close_driver()


# ──────────────────────────────────────────────
# Quick test with the sample HTML from the user
# ──────────────────────────────────────────────

def test_extraction():
    sample = """
    🏡 à vendre – superbe appartement s+2 aux jardins de carthage ! la marsa
    caractéristiques du bien :
    🛏 s+2 – spacieux et bien agencé
    📐 75 m² de surface couverte + 45 m² de jardin et terrasse
    ☀️ appartement lumineux, idéal pour profiter de chaque instant
    🌳 cadre calme et sécurisé, proche de toutes commodités
    💰 prix : 520 000 dt
    📞 visite gratuite & infos : 98 305 239 / 99 984 810
    ✅ ne laissez pas passer cette opportunité unique aux jardins de carthage !
    """
    print("=== Test d'extraction ===")
    print("Téléphones  :", _extract_phones(sample))
    print("Chambres    :", _extract_rooms(sample))
    print("Surfaces    :", _extract_surface(sample))
    print("Misc        :", _extract_misc_features(sample))
    prop = {'titre': sample[:80], 'nature': 'à vendre'}
    print("Transaction :", 'location' if _is_location(prop) else 'vente')


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  Tunisie-Annonce Immobilier Scraper  v2")
    print("  (reprise auto | vente & location séparés)")
    print("=" * 62)

    import sys
    if '--test' in sys.argv:
        test_extraction()
        return

    scraper = TunisieAnnonceScraper()

    if scraper.progress.get('last_page', 0) > 0:
        print(f"\n🔄 REPRISE DÉTECTÉE — dernière page : {scraper.progress['last_page']}")
        print(f"   Annonces collectées : {scraper.progress.get('properties_count', 0)}")
        c = input("\n  1=Reprendre  2=Recommencer [défaut: 1] : ").strip() or "1"
        if c == "2":
            scraper.reset_progress()

    h = input("\nMode  1=Visible  2=Headless [défaut: 2] : ").strip() or "2"
    headless = h != "1"

    p = input("Pages max (Entrée = toutes) : ").strip()
    max_pages = int(p) if p.isdigit() else None

    d = input("Détails complets ? (recommandé pour téléphone + surface) oui/non [défaut: non] : ").strip().lower()
    include_details = d in ['oui', 'o', 'yes', 'y']

    print(f"\n  Mode: {'headless' if headless else 'visible'} | Pages: {max_pages or 'toutes'} | Détails: {include_details}")
    print(f"  Fichiers de sortie :")
    print(f"    📁 {OUTPUT_JSON} / {OUTPUT_CSV}          → tout")
    print(f"    📁 {OUTPUT_VENTE_JSON} / {OUTPUT_VENTE_CSV}    → vente uniquement")
    print(f"    📁 {OUTPUT_LOCATION_JSON} / {OUTPUT_LOCATION_CSV} → location uniquement")
    print("  💡 Ctrl+C = sauvegarde + sortie propre\n")

    scraper2 = TunisieAnnonceScraper(headless=headless)
    props = scraper2.scrape_all(max_pages=max_pages, include_details=include_details)

    if props:
        scraper2._save_all()
        vente    = [p for p in props if p.get('type_transaction') == 'vente']
        location = [p for p in props if p.get('type_transaction') == 'location']
        print(f"\n✓ {len(props)} annonces total")
        print(f"  🏷  Vente    : {len(vente)} annonces → {OUTPUT_VENTE_JSON} + {OUTPUT_VENTE_CSV}")
        print(f"  🏷  Location : {len(location)} annonces → {OUTPUT_LOCATION_JSON} + {OUTPUT_LOCATION_CSV}")
    else:
        print("⚠️  Aucune annonce extraite.")


if __name__ == "__main__":
    main()