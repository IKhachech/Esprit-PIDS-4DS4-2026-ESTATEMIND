#!/usr/bin/env python3
"""
SCRAPER CENTURY21.TN - VERSION COMPLETE
Extraction de toutes les annonces immobilières avec Selenium
Chaque feature dans une colonne séparée
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


class Century21Scraper:
    """Scraper pour Century21.tn avec Selenium"""
    
    def __init__(self):
        self.base_url = "https://century21.tn"
        self.data = []
        self.driver = None
        
    def setup_selenium(self):
        """Configure Selenium WebDriver"""
        print("🔧 Configuration de Selenium...")
        
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')  # Mode sans interface
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Installation automatique du driver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
        print("✅ Selenium configuré!\n")
    
    def get_total_pages(self):
        """Obtient le nombre total de pages"""
        try:
            # Attendre que la pagination se charge
            time.sleep(3)
            
            # Chercher le dernier lien de pagination
            pagination = self.driver.find_elements(By.CSS_SELECTOR, "div.pagination-wrap nav ul.pagination li.page-item a")
            
            if pagination:
                # Le dernier lien (avant-dernier élément généralement)
                for link in reversed(pagination):
                    href = link.get_attribute('href')
                    if href and '/page/' in href:
                        # Extraire le numéro de page
                        match = re.search(r'/page/(\d+)/', href)
                        if match:
                            return int(match.group(1))
            
            return 1
            
        except Exception as e:
            print(f"⚠️  Impossible de déterminer le nombre de pages: {e}")
            return 1
    
    def clean_price(self, price_text):
        """Nettoie le texte du prix"""
        if not price_text or 'Prix sur demande' in price_text:
            return None, 'Prix sur demande'
        
        # Extraire le prix numérique
        price_match = re.search(r'([\d,]+)', price_text)
        if price_match:
            price = price_match.group(1).replace(',', '')
            
            # Déterminer le type (TTC, HT)
            if 'HT' in price_text:
                price_type = 'HT'
            elif 'TTC' in price_text:
                price_type = 'TTC'
            else:
                price_type = 'TTC'
            
            return price, price_type
        
        return None, None
    
    def extract_property_from_listing(self, property_element):
        """Extrait les données d'une annonce"""
        try:
            property_data = {
                'date_scraping': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # ID de la propriété (data-hz-id)
            hz_id = property_element.get_attribute('data-hz-id')
            if hz_id:
                property_data['property_id'] = hz_id.replace('hz-', '')
            
            # STATUS (Location, Vente, Direct Promoteur, etc.)
            try:
                status_elem = property_element.find_element(By.CSS_SELECTOR, "a.label-status")
                property_data['status'] = status_elem.text.strip()
            except:
                property_data['status'] = None
            
            # PRIX
            try:
                price_elem = property_element.find_element(By.CSS_SELECTOR, "li.item-price")
                price_text = price_elem.text.strip()
                price, price_type = self.clean_price(price_text)
                property_data['prix'] = price
                property_data['prix_type'] = price_type
            except:
                property_data['prix'] = None
                property_data['prix_type'] = None
            
            # URL de la propriété
            try:
                link = property_element.find_element(By.CSS_SELECTOR, "a.listing-featured-thumb")
                property_data['url'] = link.get_attribute('href')
            except:
                property_data['url'] = None
            
            # IMAGE
            try:
                img = property_element.find_element(By.CSS_SELECTOR, "img.external-img")
                property_data['image_url'] = img.get_attribute('src')
            except:
                property_data['image_url'] = None
            
            # TYPE DE BIEN (Appartement, Villa, Maison, etc.)
            try:
                type_elem = property_element.find_element(By.CSS_SELECTOR, "div.item-type")
                property_data['type'] = type_elem.text.strip()
            except:
                property_data['type'] = None
            
            # LOCALISATION
            try:
                address_elem = property_element.find_element(By.CSS_SELECTOR, "address.item-address")
                property_data['localisation'] = address_elem.text.strip()
            except:
                property_data['localisation'] = None
            
            # SURFACE (m²)
            try:
                size_elem = property_element.find_element(By.CSS_SELECTOR, "div.item-size")
                size_text = size_elem.text.strip()
                # Extraire les chiffres
                size_match = re.search(r'(\d+)', size_text)
                if size_match:
                    property_data['surface_m2'] = size_match.group(1)
                else:
                    property_data['surface_m2'] = None
            except:
                property_data['surface_m2'] = None
            
            # RÉFÉRENCE
            try:
                ref_elem = property_element.find_element(By.CSS_SELECTOR, "div.item-ref")
                ref_text = ref_elem.text.strip()
                # Extraire juste le numéro après "Réf:"
                ref_match = re.search(r'Réf:\s*(.+)', ref_text)
                if ref_match:
                    property_data['reference'] = ref_match.group(1).strip()
                else:
                    property_data['reference'] = ref_text
            except:
                property_data['reference'] = None
            
            # AGENCE
            try:
                agency_elem = property_element.find_element(By.CSS_SELECTOR, "div.item-agency__name a")
                property_data['agence'] = agency_elem.text.strip()
            except:
                property_data['agence'] = None
            
            # DESCRIPTION
            try:
                desc_elem = property_element.find_element(By.CSS_SELECTOR, "div.item-description")
                description = desc_elem.text.strip()
                # Limiter à 500 caractères
                property_data['description'] = description[:500] if description else None
            except:
                property_data['description'] = None
            
            # TÉLÉPHONE
            try:
                phone_elem = property_element.find_element(By.CSS_SELECTOR, "a.agent-phone span")
                property_data['telephone'] = phone_elem.text.strip()
            except:
                property_data['telephone'] = None
            
            return property_data
            
        except Exception as e:
            print(f"   ❌ Erreur extraction: {e}")
            return None
    
    def scrape_page(self, page_number):
        """Scrape une page de résultats"""
        try:
            # Construire l'URL
            if page_number == 1:
                url = f"{self.base_url}/search-results/"
            else:
                url = f"{self.base_url}/search-results/page/{page_number}/"
            
            print(f"\n📄 Page {page_number}: {url}")
            
            # Charger la page
            self.driver.get(url)
            
            # Attendre que les propriétés se chargent
            time.sleep(4)
            
            # Trouver toutes les propriétés
            properties = self.driver.find_elements(By.CSS_SELECTOR, "div.item-listing-wrap.hz-item-gallery-js.card")
            
            print(f"   ✅ {len(properties)} annonces trouvées")
            
            # Extraire les données de chaque propriété
            for i, prop in enumerate(properties, 1):
                property_data = self.extract_property_from_listing(prop)
                if property_data:
                    self.data.append(property_data)
                    if i % 5 == 0:
                        print(f"   📊 {i}/{len(properties)} annonces extraites...")
            
            print(f"   ✅ Page {page_number} terminée")
            
            return len(properties)
            
        except Exception as e:
            print(f"   ❌ Erreur sur page {page_number}: {e}")
            return 0
    
    def scrape_all(self, max_pages=None):
        """Scrape toutes les pages"""
        try:
            self.setup_selenium()
            
            print("="*70)
            print("🏢 SCRAPING CENTURY21.TN")
            print("="*70)
            
            # Aller sur la première page pour déterminer le nombre total
            self.driver.get(f"{self.base_url}/search-results/")
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
    
    def save_to_csv(self, filename='century21_data.csv'):
        """Sauvegarde en CSV avec colonnes séparées"""
        if not self.data:
            print("❌ Aucune donnée à sauvegarder")
            return None
        
        # Créer le dossier Downloads
        downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')
        filepath = os.path.join(downloads_folder, filename)
        
        df = pd.DataFrame(self.data)
        
        # Ordre des colonnes
        columns_order = [
            'property_id',
            'reference',
            'status',
            'type',
            'localisation',
            'prix',
            'prix_type',
            'surface_m2',
            'agence',
            'telephone',
            'description',
            'url',
            'image_url',
            'date_scraping'
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
        print(f"📋 {len(df.columns)} colonnes")
        print(f"{'='*70}\n")
        
        # Afficher un aperçu
        print("📋 APERÇU DES DONNÉES (5 premières lignes):\n")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(df.head().to_string())
        print(f"\n{'='*70}\n")
        
        # Statistiques
        print("📊 STATISTIQUES:\n")
        
        # Prix moyen
        if 'prix' in df.columns:
            prix_valides = df['prix'].dropna().astype(str)
            prix_numeriques = pd.to_numeric(prix_valides, errors='coerce').dropna()
            if len(prix_numeriques) > 0:
                print(f"Prix moyen: {prix_numeriques.mean():,.0f} TND")
                print(f"Prix min: {prix_numeriques.min():,.0f} TND")
                print(f"Prix max: {prix_numeriques.max():,.0f} TND")
        
        # Surface moyenne
        if 'surface_m2' in df.columns:
            surfaces = pd.to_numeric(df['surface_m2'], errors='coerce').dropna()
            if len(surfaces) > 0:
                print(f"\nSurface moyenne: {surfaces.mean():.0f} m²")
        
        # Répartition par status
        if 'status' in df.columns:
            print(f"\n📊 Répartition par Status:")
            print(df['status'].value_counts())
        
        # Types de biens
        if 'type' in df.columns:
            print(f"\n🏠 Types de biens:")
            print(df['type'].value_counts().head(10))
        
        # Villes principales
        if 'localisation' in df.columns:
            print(f"\n📍 Villes principales:")
            print(df['localisation'].value_counts().head(10))
        
        # Agences
        if 'agence' in df.columns:
            print(f"\n🏢 Agences:")
            print(df['agence'].value_counts().head(10))
        
        return df


def main():
    """Fonction principale"""
    
    print("\n" + "="*70)
    print("🏢 SCRAPER CENTURY21.TN - VERSION SELENIUM")
    print("="*70)
    print()
    
    # Demander combien de pages scraper
    try:
        max_pages = input("Combien de pages voulez-vous scraper? (Entrée pour toutes, ~231 pages): ").strip()
        if max_pages:
            max_pages = int(max_pages)
        else:
            max_pages = None
    except:
        max_pages = 10  # Par défaut
        print(f"⚠️  Valeur invalide, limitation à {max_pages} pages")
    
    # Créer le scraper
    scraper = Century21Scraper()
    
    # Scraper
    scraper.scrape_all(max_pages=max_pages)
    
    # Sauvegarder
    scraper.save_to_csv('century21_data.csv')
    
    print("\n🎉 TERMINÉ! Vérifiez votre dossier Downloads.\n")


if __name__ == "__main__":
    main()
