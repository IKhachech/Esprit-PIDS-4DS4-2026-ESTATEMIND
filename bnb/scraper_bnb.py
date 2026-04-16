#!/usr/bin/env python3
"""
BnB Tunisie Web Scraper - Version avec Reprise Automatique
Reprend là où le script s'est arrêté en cas d'interruption
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json
import time
import re
import logging
import os

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 🆕 Fichier de progression
PROGRESS_FILE = 'scraping_progress.json'


class BnBSeleniumScraper:
    def __init__(self, headless=True):
        """
        Initialise le scraper avec Selenium
        
        Args:
            headless (bool): True pour mode invisible, False pour voir le navigateur
        """
        self.base_url = "https://www.bnb.tn"
        self.properties_url = f"{self.base_url}/properties/"
        self.all_properties = []
        self.driver = None
        self.headless = headless
        self.progress = self.load_progress()
        
    def load_progress(self):
        """🆕 Charge la progression depuis le fichier"""
        try:
            if os.path.exists(PROGRESS_FILE):
                with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    logger.info(f"📥 Progression chargée: Page {progress.get('last_page', 0)}/{progress.get('total_pages', '?')}")
                    return progress
        except Exception as e:
            logger.warning(f"Impossible de charger la progression: {e}")
        
        return {
            'last_page': 0,
            'total_pages': None,
            'properties_count': 0,
            'completed': False
        }
    
    def save_progress(self, page_number, total_pages):
        """🆕 Sauvegarde la progression actuelle"""
        try:
            progress = {
                'last_page': page_number,
                'total_pages': total_pages,
                'properties_count': len(self.all_properties),
                'completed': page_number >= total_pages,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(progress, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.warning(f"Impossible de sauvegarder la progression: {e}")
    
    def load_existing_data(self, filename='bnb_properties.json'):
        """🆕 Charge les données existantes si elles existent"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"📥 {len(data)} annonces existantes chargées")
                    return data
        except Exception as e:
            logger.warning(f"Impossible de charger les données existantes: {e}")
        
        return []
        
    def setup_driver(self):
        """Configure et démarre le navigateur Chrome"""
        try:
            chrome_options = Options()
            
            if self.headless:
                chrome_options.add_argument('--headless=new')
            
            # Options pour éviter la détection
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Options de performance
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # User agent
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            logger.info("Démarrage du navigateur Chrome...")
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Masquer l'automation
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("✓ Navigateur démarré avec succès")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors du démarrage du navigateur: {e}")
            logger.error("Assurez-vous que Chrome et ChromeDriver sont installés")
            return False
    
    def close_driver(self):
        """Ferme le navigateur"""
        if self.driver:
            self.driver.quit()
            logger.info("Navigateur fermé")
    
    def wait_for_page_load(self, timeout=10):
        """Attend que la page soit complètement chargée"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(1)  # Pause supplémentaire pour le JavaScript
            return True
        except TimeoutException:
            logger.warning("Timeout lors du chargement de la page")
            return False
    
    def get_total_pages(self):
        """Récupère le nombre total de pages"""
        try:
            logger.info("Récupération du nombre de pages...")
            self.driver.get(self.properties_url)
            self.wait_for_page_load()
            
            # Prendre une capture d'écran pour debug
            self.driver.save_screenshot('page_accueil.png')
            logger.info("✓ Capture d'écran sauvegardée: page_accueil.png")
            
            # Méthode 1: Chercher les numéros de page dans la pagination
            try:
                pagination = self.driver.find_element(By.CSS_SELECTOR, 'nav.navigation.pagination')
                page_links = pagination.find_elements(By.CSS_SELECTOR, 'a.page-numbers')
                
                page_numbers = []
                for link in page_links:
                    text = link.text.strip()
                    if text.isdigit():
                        page_numbers.append(int(text))
                
                if page_numbers:
                    max_page = max(page_numbers)
                    logger.info(f"✓ Nombre de pages détecté: {max_page}")
                    return max_page
            except NoSuchElementException:
                logger.warning("Pagination non trouvée avec CSS selector")
            
            # Méthode 2: Chercher via XPath
            try:
                page_elements = self.driver.find_elements(By.XPATH, "//nav[contains(@class, 'pagination')]//a[contains(@class, 'page-numbers')]")
                page_numbers = []
                for elem in page_elements:
                    text = elem.text.strip()
                    if text.isdigit():
                        page_numbers.append(int(text))
                
                if page_numbers:
                    max_page = max(page_numbers)
                    logger.info(f"✓ Nombre de pages trouvé (XPath): {max_page}")
                    return max_page
            except Exception as e:
                logger.warning(f"Erreur avec XPath: {e}")
            
            # Méthode 3: Chercher dans le texte de la page
            page_source = self.driver.page_source
            matches = re.findall(r'page/(\d+)/', page_source)
            if matches:
                max_page = max([int(m) for m in matches])
                logger.info(f"✓ Nombre de pages trouvé dans le HTML: {max_page}")
                return max_page
            
            logger.warning("Une seule page assumée")
            return 1
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du nombre de pages: {e}")
            return 1
    
    def scrape_listings_page(self, page_number):
        """Scrape une page de listings"""
        try:
            if page_number == 1:
                url = self.properties_url
            else:
                url = f"{self.properties_url}page/{page_number}/"
            
            logger.info(f"Scraping page {page_number}: {url}")
            self.driver.get(url)
            self.wait_for_page_load()
            
            # Scroll pour charger le contenu dynamique
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(1)
            
            properties = []
            
            # Méthode 1: Chercher les articles
            try:
                articles = self.driver.find_elements(By.CSS_SELECTOR, 'article.property-row')
                logger.info(f"Trouvé {len(articles)} articles avec 'article.property-row'")
            except:
                articles = []
            
            # Méthode 2: Si pas d'articles, chercher autrement
            if not articles:
                try:
                    articles = self.driver.find_elements(By.TAG_NAME, 'article')
                    logger.info(f"Trouvé {len(articles)} articles avec tag 'article'")
                except:
                    articles = []
            
            # Méthode 3: Chercher via XPath
            if not articles:
                try:
                    articles = self.driver.find_elements(By.XPATH, "//article[contains(@class, 'property')]")
                    logger.info(f"Trouvé {len(articles)} articles avec XPath")
                except:
                    articles = []
            
            if not articles:
                logger.warning("Aucun article trouvé sur cette page")
                # Sauvegarder la page pour debug
                with open(f'page_{page_number}_debug.html', 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                logger.info(f"HTML de la page sauvegardé: page_{page_number}_debug.html")
                return properties
            
            # Extraire les données de chaque article
            for idx, article in enumerate(articles, 1):
                try:
                    property_data = self.extract_listing_data(article, idx)
                    if property_data:
                        properties.append(property_data)
                        logger.debug(f"✓ Annonce {idx} extraite: {property_data.get('titre', '')[:50]}")
                except Exception as e:
                    logger.warning(f"Erreur lors de l'extraction de l'annonce {idx}: {e}")
                    continue
            
            logger.info(f"✓ Page {page_number}: {len(properties)} annonces extraites")
            return properties
            
        except Exception as e:
            logger.error(f"Erreur lors du scraping de la page {page_number}: {e}")
            return []
    
    def extract_listing_data(self, article, idx):
        """Extrait les données d'une annonce depuis l'élément Selenium"""
        data = {}
        
        try:
            # Titre et URL
            try:
                title_element = article.find_element(By.CSS_SELECTOR, 'h3.property-row-title a')
                data['titre'] = title_element.text.strip()
                data['url'] = title_element.get_attribute('href')
            except NoSuchElementException:
                # Essayer avec XPath
                try:
                    title_element = article.find_element(By.XPATH, ".//h3[contains(@class, 'property-row-title')]//a")
                    data['titre'] = title_element.text.strip()
                    data['url'] = title_element.get_attribute('href')
                except:
                    logger.debug(f"Titre non trouvé pour l'annonce {idx}")
            
            # Image
            try:
                img_element = article.find_element(By.CSS_SELECTOR, 'img.wp-post-image')
                data['image'] = img_element.get_attribute('src')
            except NoSuchElementException:
                try:
                    img_element = article.find_element(By.TAG_NAME, 'img')
                    data['image'] = img_element.get_attribute('src')
                except:
                    pass
            
            # Prix
            try:
                price_element = article.find_element(By.CSS_SELECTOR, 'div.property-row-price')
                price_text = price_element.text.strip()
                data['prix'] = price_text
                
                # Extraire le montant numérique
                price_match = re.search(r'([\d,\s]+)\s*TND', price_text)
                if price_match:
                    data['prix_montant'] = price_match.group(1).replace(',', '').replace(' ', '')
            except NoSuchElementException:
                pass
            
            # Localisation
            try:
                location_element = article.find_element(By.CSS_SELECTOR, 'div.property-row-location')
                data['localisation'] = location_element.text.strip()
            except NoSuchElementException:
                try:
                    subtitle = article.find_element(By.CSS_SELECTOR, 'div.property-row-subtitle')
                    data['localisation'] = subtitle.text.strip()
                except:
                    pass
            
            # Description
            try:
                desc_element = article.find_element(By.CSS_SELECTOR, 'p.justify')
                data['description'] = desc_element.text.strip()
            except NoSuchElementException:
                pass
            
            # Métadonnées (chambres, salles de bain, etc.)
            try:
                meta_div = article.find_element(By.CSS_SELECTOR, 'div.property-row-meta')
                field_items = meta_div.find_elements(By.CSS_SELECTOR, 'div.field-item')
                
                metadata = {}
                for field in field_items:
                    try:
                        label = field.find_element(By.CSS_SELECTOR, 'div.label').text.strip()
                        value = field.text.replace(label, '').strip()
                        metadata[label] = value
                    except:
                        continue
                
                if metadata:
                    data['metadata'] = metadata
            except NoSuchElementException:
                pass
            
            # Slug depuis l'URL
            if data.get('url'):
                url_parts = data['url'].rstrip('/').split('/')
                if url_parts:
                    data['slug'] = url_parts[-1]
            
            # Vérifier qu'on a au moins un titre ou une URL
            if not data.get('titre') and not data.get('url'):
                return None
            
            return data
            
        except Exception as e:
            logger.warning(f"Erreur lors de l'extraction: {e}")
            return None
    
    def scrape_property_details(self, property_url):
        """Scrape les détails complets d'une propriété"""
        try:
            logger.info(f"Scraping détails: {property_url}")
            self.driver.get(property_url)
            self.wait_for_page_load()
            
            details = {}
            
            # Titre
            try:
                title = self.driver.find_element(By.CSS_SELECTOR, 'h1.h3')
                details['titre'] = title.text.strip()
            except:
                pass
            
            # Prix
            try:
                price = self.driver.find_element(By.CSS_SELECTOR, 'div.property-price')
                details['prix'] = price.text.strip()
            except:
                pass
            
            # Localisation
            try:
                location = self.driver.find_element(By.CSS_SELECTOR, 'h2.mb-0 a')
                details['localisation'] = location.text.strip()
            except:
                pass
            
            # Caractéristiques (table)
            try:
                table = self.driver.find_element(By.CSS_SELECTOR, 'table.table-striped')
                rows = table.find_elements(By.TAG_NAME, 'tr')
                
                characteristics = {}
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if len(cells) == 2:
                        key = cells[0].text.strip()
                        value = cells[1].text.strip()
                        characteristics[key] = value
                
                if characteristics:
                    details['caracteristiques'] = characteristics
            except:
                pass
            
            # Description complète
            try:
                paragraphs = self.driver.find_elements(By.CSS_SELECTOR, 'div.entry-content p.justify')
                if paragraphs:
                    description_parts = [p.text.strip() for p in paragraphs if p.text.strip()]
                    details['description'] = '\n\n'.join(description_parts)
            except:
                pass
            
            # Équipements
            try:
                amenities_ul = self.driver.find_element(By.CSS_SELECTOR, 'div.property-detail-amenities ul')
                amenities_li = amenities_ul.find_elements(By.CSS_SELECTOR, 'li.yes')
                amenities = [li.text.strip() for li in amenities_li]
                if amenities:
                    details['equipements'] = amenities
            except:
                pass
            
            # Images
            try:
                gallery = self.driver.find_element(By.CSS_SELECTOR, 'div.property-detail-gallery')
                img_links = gallery.find_elements(By.CSS_SELECTOR, 'a[data-rel="property-gallery"]')
                images = [link.get_attribute('href') for link in img_links if link.get_attribute('href')]
                if images:
                    details['images'] = images
            except:
                pass
            
            return details
            
        except Exception as e:
            logger.error(f"Erreur lors du scraping des détails: {e}")
            return None
    
    def scrape_all(self, max_pages=None, include_details=False, save_incremental=True):
        """🆕 Scrape toutes les annonces avec reprise automatique"""
        if not self.setup_driver():
            logger.error("Impossible de démarrer le navigateur")
            return []
        
        try:
            logger.info("Début du scraping avec Selenium...")
            
            # 🆕 Charger les données existantes
            self.all_properties = self.load_existing_data()
            
            # Obtenir le nombre total de pages
            total_pages = self.get_total_pages()
            logger.info(f"Nombre total de pages: {total_pages}")
            
            if max_pages:
                total_pages = min(total_pages, max_pages)
                logger.info(f"Limitation à {total_pages} pages")
            
            # 🆕 Déterminer la page de départ
            start_page = self.progress.get('last_page', 0) + 1
            
            if start_page > 1:
                logger.info(f"🔄 REPRISE: Début à la page {start_page}/{total_pages}")
                logger.info(f"📊 Données existantes: {len(self.all_properties)} annonces")
            
            # Scraper chaque page
            for page_num in range(start_page, total_pages + 1):
                logger.info(f"📄 Page {page_num}/{total_pages}")
                
                properties = self.scrape_listings_page(page_num)
                
                if include_details:
                    # Scraper les détails de chaque propriété
                    for prop in properties:
                        if prop.get('url'):
                            time.sleep(1)
                            details = self.scrape_property_details(prop['url'])
                            if details:
                                prop['details'] = details
                
                self.all_properties.extend(properties)
                
                # Sauvegarde incrémentale
                if save_incremental:
                    self.save_to_json('bnb_properties.json')
                    self.save_to_csv('bnb_properties.csv')
                    self.save_progress(page_num, total_pages)
                    logger.info(f"💾 Progression: {len(self.all_properties)} annonces | Page {page_num}/{total_pages}")
                
                # Pause entre les pages
                if page_num < total_pages:
                    time.sleep(2)
            
            # 🆕 Marquer comme terminé
            self.save_progress(total_pages, total_pages)
            logger.info(f"✓ Scraping terminé. Total: {len(self.all_properties)} annonces")
            
            return self.all_properties
            
        except KeyboardInterrupt:
            logger.warning("\n⚠️  Interruption par l'utilisateur (Ctrl+C)")
            logger.info("💾 Les données ont été sauvegardées automatiquement")
            logger.info(f"📊 Progression: {len(self.all_properties)} annonces collectées")
            logger.info("ℹ️  Relancez le script pour reprendre où vous vous êtes arrêté")
            return self.all_properties
            
        except Exception as e:
            logger.error(f"Erreur pendant le scraping: {e}")
            # Sauvegarder ce qui a été récupéré avant l'erreur
            if self.all_properties:
                logger.info("Sauvegarde des données partielles...")
                self.save_to_json('bnb_properties_partial.json')
                self.save_to_csv('bnb_properties_partial.csv')
            return self.all_properties
        
        finally:
            self.close_driver()
    
    def save_to_json(self, filename='bnb_properties.json'):
        """Sauvegarde les données en JSON"""
        try:
            os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.all_properties, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde JSON: {e}")
    
    def save_to_csv(self, filename='bnb_properties.csv'):
        """Sauvegarde les données en CSV"""
        try:
            import csv
            
            if not self.all_properties:
                logger.warning("Aucune donnée à sauvegarder")
                return
            
            os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)
            
            # Aplatir les données
            all_keys = set()
            for prop in self.all_properties:
                flat_prop = {k: v for k, v in prop.items() if k not in ['metadata', 'details']}
                if 'metadata' in prop:
                    for k, v in prop['metadata'].items():
                        flat_prop[f'meta_{k}'] = v
                all_keys.update(flat_prop.keys())
            
            csv_keys = sorted(all_keys)
            
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=csv_keys)
                writer.writeheader()
                
                for prop in self.all_properties:
                    flat_prop = {k: v for k, v in prop.items() if k not in ['metadata', 'details']}
                    if 'metadata' in prop:
                        for k, v in prop['metadata'].items():
                            flat_prop[f'meta_{k}'] = v
                    
                    row = {k: flat_prop.get(k, '') for k in csv_keys}
                    writer.writerow(row)
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde CSV: {e}")
    
    def reset_progress(self):
        """🆕 Réinitialise la progression (pour recommencer à zéro)"""
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            logger.info("✓ Progression réinitialisée")
        
        self.progress = {
            'last_page': 0,
            'total_pages': None,
            'properties_count': 0,
            'completed': False
        }


def main():
    """Fonction principale"""
    print("="*60)
    print("BnB Tunisie Scraper - Version avec Reprise Automatique")
    print("="*60)
    print()
    
    # 🆕 Vérifier s'il y a une progression existante
    scraper = BnBSeleniumScraper()
    
    if scraper.progress.get('last_page', 0) > 0:
        print("🔄 REPRISE DÉTECTÉE")
        print("="*60)
        print(f"Dernière page scrapée: {scraper.progress['last_page']}")
        print(f"Annonces collectées: {scraper.progress.get('properties_count', 0)}")
        print(f"Date: {scraper.progress.get('timestamp', 'N/A')}")
        print("="*60)
        print()
        print("Options:")
        print("1. Reprendre où j'en étais")
        print("2. Recommencer à zéro")
        resume_choice = input("\nVotre choix (1 ou 2) [par défaut: 1]: ").strip() or "1"
        
        if resume_choice == "2":
            scraper.reset_progress()
            # Supprimer aussi les fichiers de données
            for file in ['bnb_properties.json', 'bnb_properties.csv']:
                if os.path.exists(file):
                    os.remove(file)
            print("✓ Redémarrage à zéro")
        else:
            print(f"✓ Reprise à la page {scraper.progress['last_page'] + 1}")
        print()
    
    # Demander à l'utilisateur
    print("Mode d'exécution:")
    print("1. Mode visible (voir le navigateur)")
    print("2. Mode invisible (headless)")
    choice = input("\nVotre choix (1 ou 2) [par défaut: 2]: ").strip() or "2"
    
    headless = choice == "2"
    
    print("\nNombre de pages à scraper:")
    max_pages_input = input("Entrez un nombre (ou appuyez sur Entrée pour toutes les pages): ").strip()
    max_pages = int(max_pages_input) if max_pages_input.isdigit() else None
    
    print("\nScraper les détails complets de chaque annonce?")
    include_details_input = input("Oui/Non [par défaut: Non]: ").strip().lower()
    include_details = include_details_input in ['oui', 'o', 'yes', 'y']
    
    print("\n" + "="*60)
    print(f"Configuration:")
    print(f"  - Mode: {'Invisible' if headless else 'Visible'}")
    print(f"  - Pages max: {max_pages if max_pages else 'Toutes'}")
    print(f"  - Détails: {'Oui' if include_details else 'Non'}")
    print(f"  - Sauvegarde: Après chaque page ✓")
    print(f"  - Reprise auto: Activée ✓")
    print("="*60)
    print("\n💡 Astuce: Vous pouvez interrompre avec Ctrl+C et reprendre plus tard!")
    print()
    
    # Créer le scraper avec la configuration
    scraper = BnBSeleniumScraper(headless=headless)
    
    # Scraper avec sauvegarde incrémentale et reprise
    properties = scraper.scrape_all(
        max_pages=max_pages, 
        include_details=include_details,
        save_incremental=True
    )
    
    # Sauvegarde finale
    if properties:
        scraper.save_to_json('bnb_properties.json')
        scraper.save_to_csv('bnb_properties.csv')
        
        # Afficher un résumé
        print("\n" + "="*60)
        print("RÉSUMÉ DU SCRAPING")
        print("="*60)
        print(f"Total d'annonces extraites: {len(properties)}")
        print(f"\nPremier exemple:")
        print(json.dumps(properties[0], indent=2, ensure_ascii=False)[:500] + "...")
        print("\n" + "="*60)
        print("✓ Scraping terminé avec succès!")
        print("✓ Fichiers créés:")
        print("  - bnb_properties.json")
        print("  - bnb_properties.csv")
        print("  - scraping_progress.json (progression)")
        print("="*60)
    else:
        print("\n⚠️  Aucune annonce extraite!")


if __name__ == "__main__":
    main()