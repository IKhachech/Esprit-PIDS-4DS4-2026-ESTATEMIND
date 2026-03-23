#!/usr/bin/env python3
"""
SCRAPER COMPLET - HomeInTunisia.com
Extraction de toutes les annonces immobilières avec Selenium
"""

import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re


class HomeInTunisiaScraper:
    """Scraper pour HomeInTunisia avec Selenium"""
    
    def __init__(self):
        self.base_url = "https://www.homeintunisia.com/fr"
        self.data = []
        self.driver = None
        
    def setup_selenium(self):
        """Configure Selenium WebDriver"""
        print("🔧 Configuration de Selenium...")
        
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Mode sans interface
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        # Installation automatique du driver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        print("✅ Selenium configuré!\n")
    
    def get_total_pages(self):
        """Obtient le nombre total de pages"""
        try:
            # Attendre que la pagination se charge
            time.sleep(3)
            
            # Chercher le dernier numéro de page
            pager = self.driver.find_elements(By.CSS_SELECTOR, "nav.pager ul li.last a")
            if pager:
                last_page_url = pager[0].get_attribute('href')
                # Extraire le numéro de page de l'URL
                match = re.search(r'page=(\d+)', last_page_url)
                if match:
                    return int(match.group(1))
            
            # Si pas de pagination, retourner 1
            return 1
            
        except Exception as e:
            print(f"⚠️  Impossible de déterminer le nombre de pages: {e}")
            return 1
    
    def extract_property_from_listing(self, property_element):
        """Extrait les données d'une annonce depuis la page de listing"""
        try:
            property_data = {
                'date_scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # ID de la propriété
            property_id = property_element.get_attribute('data-property-id')
            property_data['property_id'] = property_id
            
            # URL de la propriété
            link = property_element.find_element(By.CSS_SELECTOR, "figure a")
            property_data['url'] = link.get_attribute('href')
            
            # Image
            try:
                img = property_element.find_element(By.CSS_SELECTOR, "figure a img")
                property_data['image_url'] = img.get_attribute('src')
            except:
                property_data['image_url'] = None
            
            # Type et localisation (dans h3)
            try:
                h3 = property_element.find_element(By.CSS_SELECTOR, "article.infos h3")
                h3_text = h3.text.strip()
                # Format: "Type, Localisation"
                parts = h3_text.split(',')
                if len(parts) >= 2:
                    property_data['type'] = parts[0].strip()
                    property_data['localisation'] = parts[1].strip()
                else:
                    property_data['type'] = h3_text
                    property_data['localisation'] = None
            except:
                property_data['type'] = None
                property_data['localisation'] = None
            
            # Titre (dans h2)
            try:
                h2 = property_element.find_element(By.CSS_SELECTOR, "article.infos h2")
                property_data['titre'] = h2.text.strip()
            except:
                property_data['titre'] = None
            
            # Prix
            try:
                price = property_element.find_element(By.CSS_SELECTOR, "ul li.price")
                price_text = price.text.strip()
                # Nettoyer le prix (enlever espaces et TND)
                price_clean = price_text.replace(' ', '').replace('TND', '').replace('\xa0', '')
                property_data['prix'] = price_clean if price_clean else None
            except:
                property_data['prix'] = None
            
            # Surface
            try:
                area = property_element.find_element(By.CSS_SELECTOR, "span.area")
                # Le texte vient après le span
                area_parent = area.find_element(By.XPATH, "..")
                area_text = area_parent.text.strip()
                # Extraire les chiffres
                area_match = re.search(r'(\d+)', area_text)
                if area_match:
                    property_data['surface_m2'] = area_match.group(1)
                else:
                    property_data['surface_m2'] = None
            except:
                property_data['surface_m2'] = None
            
            # Nombre de pièces
            try:
                rooms = property_element.find_element(By.CSS_SELECTOR, "span.rooms")
                rooms_parent = rooms.find_element(By.XPATH, "..")
                rooms_text = rooms_parent.text.strip()
                rooms_match = re.search(r'(\d+)', rooms_text)
                if rooms_match:
                    property_data['nombre_pieces'] = rooms_match.group(1)
                else:
                    property_data['nombre_pieces'] = None
            except:
                property_data['nombre_pieces'] = None
            
            # Nombre de chambres
            try:
                bedrooms = property_element.find_element(By.CSS_SELECTOR, "span.bedrooms")
                bedrooms_parent = bedrooms.find_element(By.XPATH, "..")
                bedrooms_text = bedrooms_parent.text.strip()
                bedrooms_match = re.search(r'(\d+)', bedrooms_text)
                if bedrooms_match:
                    property_data['nombre_chambres'] = bedrooms_match.group(1)
                else:
                    property_data['nombre_chambres'] = None
            except:
                property_data['nombre_chambres'] = None
            
            # Nombre de salles de bain
            try:
                bathrooms = property_element.find_element(By.CSS_SELECTOR, "span.bathrooms")
                bathrooms_parent = bathrooms.find_element(By.XPATH, "..")
                bathrooms_text = bathrooms_parent.text.strip()
                bathrooms_match = re.search(r'(\d+)', bathrooms_text)
                if bathrooms_match:
                    property_data['salles_bain'] = bathrooms_match.group(1)
                else:
                    property_data['salles_bain'] = None
            except:
                property_data['salles_bain'] = None
            
            return property_data
            
        except Exception as e:
            print(f"   ❌ Erreur extraction: {e}")
            return None
    
    def scrape_page(self, page_number):
        """Scrape une page de résultats"""
        try:
            # Construire l'URL
            if page_number == 1:
                url = f"{self.base_url}/acheter"
            else:
                url = f"{self.base_url}/acheter?page={page_number}"
            
            print(f"\n📄 Page {page_number}: {url}")
            
            # Charger la page
            self.driver.get(url)
            
            # Attendre que les propriétés se chargent
            time.sleep(5)
            
            # Trouver toutes les propriétés
            properties = self.driver.find_elements(By.CSS_SELECTOR, "li.property[data-property-id]")
            
            print(f"   ✅ {len(properties)} annonces trouvées")
            
            # Extraire les données de chaque propriété
            for i, prop in enumerate(properties, 1):
                property_data = self.extract_property_from_listing(prop)
                if property_data:
                    self.data.append(property_data)
                    if i % 20 == 0:
                        print(f"   📊 {i}/{len(properties)} annonces extraites...")
            
            print(f"   ✅ Page {page_number} terminée - {len(properties)} annonces collectées")
            
            return len(properties)
            
        except Exception as e:
            print(f"   ❌ Erreur sur page {page_number}: {e}")
            return 0
    
    def scrape_all(self, max_pages=None):
        """Scrape toutes les pages"""
        try:
            self.setup_selenium()
            
            print("="*70)
            print("🏠 SCRAPING HOMEINTUNISIA.COM")
            print("="*70)
            
            # Aller sur la première page pour déterminer le nombre total
            self.driver.get(f"{self.base_url}/acheter")
            time.sleep(3)
            
            total_pages = self.get_total_pages()
            print(f"\n📊 Nombre total de pages: {total_pages}")
            
            if max_pages:
                total_pages = min(total_pages, max_pages)
                print(f"⚠️  Limitation à {max_pages} pages")
            
            # Scraper chaque page
            for page in range(1, total_pages + 1):
                self.scrape_page(page)
                time.sleep(2)  # Délai entre pages
            
            print(f"\n{'='*70}")
            print(f"✅ SCRAPING TERMINÉ!")
            print(f"📊 {len(self.data)} annonces collectées au total")
            print(f"{'='*70}\n")
            
        finally:
            if self.driver:
                self.driver.quit()
                print("✅ Navigateur fermé\n")
    
    def save_to_csv(self, filename='homeintunisia_data.csv'):
        """Sauvegarde en CSV"""
        if not self.data:
            print("❌ Aucune donnée à sauvegarder")
            return None
        
        # Créer le dossier Downloads si on est sur Windows
        downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
        filepath = os.path.join(downloads_folder, filename)
        
        df = pd.DataFrame(self.data)
        
        # Réorganiser les colonnes dans un ordre logique
        columns_order = [
            'property_id', 'titre', 'type', 'localisation', 'prix', 
            'surface_m2', 'nombre_pieces', 'nombre_chambres', 'salles_bain',
            'url', 'image_url', 'date_scraping'
        ]
        
        # Garder seulement les colonnes qui existent
        columns_order = [col for col in columns_order if col in df.columns]
        df = df[columns_order]
        
        # Sauvegarder
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        print(f"{'='*70}")
        print(f"📁 FICHIER CSV CRÉÉ")
        print(f"{'='*70}")
        print(f"✅ Fichier: {filepath}")
        print(f"📊 {len(df)} annonces")
        print(f"📋 {len(df.columns)} colonnes: {', '.join(df.columns)}")
        print(f"{'='*70}\n")
        
        # Afficher un aperçu
        print("📋 APERÇU DES DONNÉES (5 premières lignes):\n")
        print(df.head().to_string())
        print(f"\n{'='*70}\n")
        
        # Statistiques
        print("📊 STATISTIQUES:\n")
        print(f"Prix moyen: {df['prix'].astype(str).str.replace(',', '').str.extract(r'(\d+)')[0].astype(float).mean():,.0f} TND")
        print(f"Surface moyenne: {df['surface_m2'].dropna().astype(float).mean():.0f} m²")
        print(f"\nTypes de biens:")
        print(df['type'].value_counts().head(10))
        print(f"\nVilles principales:")
        print(df['localisation'].value_counts().head(10))
        
        return df


def main():
    """Fonction principale"""
    
    print("\n" + "="*70)
    print("🏠 SCRAPER HOMEINTUNISIA.COM - VERSION SELENIUM")
    print("="*70)
    print()
    
    # Demander combien de pages scraper
    try:
        max_pages = input("Combien de pages voulez-vous scraper? (appuyez sur Entrée pour toutes): ").strip()
        if max_pages:
            max_pages = int(max_pages)
        else:
            max_pages = None
    except:
        max_pages = 5  # Par défaut
        print(f"⚠️  Valeur invalide, limitation à {max_pages} pages")
    
    # Créer le scraper
    scraper = HomeInTunisiaScraper()
    
    # Scraper
    scraper.scrape_all(max_pages=max_pages)
    
    # Sauvegarder
    scraper.save_to_csv('homeintunisia_data.csv')
    
    print("\n🎉 TERMINÉ! Vérifiez votre dossier Downloads.\n")


if __name__ == "__main__":
    main()
