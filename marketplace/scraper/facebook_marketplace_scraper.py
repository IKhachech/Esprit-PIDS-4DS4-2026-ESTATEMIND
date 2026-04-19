from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import os
from datetime import datetime
import re
import random
import pandas as pd

def clean_price(price_text):
    """Nettoie et convertie le prix - retourne uniquement le format affiché"""
    if not price_text:
        return None
    
    # Garder le format original avec devise
    price_clean = price_text.strip()
    
    # Normaliser l'affichage
    if 'TND' in price_clean or 'DT' in price_clean or 'د.ت' in price_clean:
        # Extraire les chiffres
        numbers = re.findall(r'\d+', price_clean)
        if numbers:
            return ' '.join(numbers) + ' TND'
    
    return price_clean

def extract_gouvernorat(text):
    """Extrait le gouvernorat du texte"""
    gouvernorats = [
        "Tunis", "Ariana", "Ben Arous", "Manouba", "Nabeul", "Zaghouan", 
        "Bizerte", "Béja", "Jendouba", "Kef", "Siliana", "Kairouan", 
        "Kasserine", "Sidi Bouzid", "Sousse", "Monastir", "Mahdia", 
        "Sfax", "Gafsa", "Tozeur", "Kebili", "Gabès", "Medenine", "Tataouine"
    ]
    
    text_lower = text.lower()
    for gouv in gouvernorats:
        if gouv.lower() in text_lower:
            return gouv
    
    return "Tunisie"

def extract_ville(text, gouvernorat):
    """Extrait la ville du texte"""
    # Si on a trouvé un gouvernorat, chercher une ville dans le texte
    text_lower = text.lower()
    
    # Patterns de localisation commune
    location_patterns = [
        r'(?:à|À|in|,)\s+([A-ZÀ-Ú][a-zà-ú\s\-]+)',
        r',\s*([A-ZÀ-Ú][a-zà-ú]+)',
    ]
    
    for pattern in location_patterns:
        matches = re.findall(pattern, text)
        if matches:
            ville = matches[0].strip()
            # Si la ville n'est pas le gouvernorat lui-même
            if ville.lower() != gouvernorat.lower():
                return ville
    
    # Par défaut, retourner le gouvernorat comme ville
    return gouvernorat if gouvernorat != "Tunisie" else "Non spécifié"

def detect_property_type(text):
    """Détecte le type de bien de manière plus précise"""
    text_lower = text.lower()
    
    # Ordre d'importance pour la détection
    if any(w in text_lower for w in ["local commercial", "local comm", "magasin", "boutique"]):
        return "Local Commercial"
    
    if any(w in text_lower for w in ["bureau", "office"]):
        return "Bureau"
    
    if any(w in text_lower for w in ["entrepôt", "warehouse", "hangar"]):
        return "Entrepôt"
    
    if any(w in text_lower for w in ["terrain", "lot"]):
        return "Terrain"
    
    if any(w in text_lower for w in ["villa", "maison"]):
        return "Maison/Villa"
    
    if any(w in text_lower for w in ["duplex"]):
        return "Duplex"
    
    if any(w in text_lower for w in ["studio"]):
        return "Studio"
    
    # Détection S+X
    if re.search(r's\+?[1234]|f[1234]', text_lower):
        match = re.search(r's\+?(\d)|f(\d)', text_lower)
        if match:
            num = match.group(1) or match.group(2)
            if num == '1':
                return "Appartement S+1"
            elif num == '2':
                return "Appartement S+2"
            elif num == '3':
                return "Appartement S+3"
            elif num in ['4', '5']:
                return "Appartement S+4+"
    
    if any(w in text_lower for w in ["appartement", "appart", "flat"]):
        return "Appartement"
    
    return "Autre"

def create_facebook_driver(user_data_dir=None, headless=False):
    """Crée un driver Chrome optimisé pour Facebook"""
    options = webdriver.ChromeOptions()
    
    if user_data_dir:
        options.add_argument(f"user-data-dir={user_data_dir}")
    
    if headless:
        options.add_argument("--headless=new")
    
    # Anti-détection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Préférences
    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
    }
    options.add_experimental_option("prefs", prefs)
    
    # Options de performance
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--log-level=3')
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")
    
    service = Service(log_path=os.devnull)
    driver = webdriver.Chrome(service=service, options=options)
    
    # Masquer webdriver
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    
    driver.set_page_load_timeout(60)
    return driver

def login_facebook_manual(driver):
    """Guide l'utilisateur pour se connecter manuellement"""
    print("\n" + "="*70)
    print("🔐 CONNEXION FACEBOOK REQUISE")
    print("="*70)
    print("\n📝 INSTRUCTIONS:")
    print("  1. Une fenêtre Chrome va s'ouvrir")
    print("  2. Connectez-vous à votre compte Facebook")
    print("  3. Attendez d'être complètement connecté")
    print("  4. Revenez à ce terminal et appuyez sur ENTRÉE")
    print("\n⚠️  NE FERMEZ PAS la fenêtre Chrome!\n")
    
    driver.get("https://www.facebook.com")
    input("👉 Appuyez sur ENTRÉE quand vous êtes connecté... ")
    
    print("\n✅ Vérification de la connexion...")
    time.sleep(2)
    
    try:
        driver.find_element(By.CSS_SELECTOR, "[aria-label*='Profil'], [aria-label*='Profile']")
        print("✅ Connexion réussie!\n")
        return True
    except:
        print("⚠️  Vous ne semblez pas connecté. Continuons quand même...\n")
        return False

def smart_scroll(driver, pause_time=2):
    """Scroll intelligent pour charger le contenu"""
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    # Scroll progressif
    for i in range(5):
        driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {(i+1)/5});")
        time.sleep(0.3)
    
    # Scroll final
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(pause_time)
    
    # Petit scroll arrière pour trigger lazy load
    driver.execute_script("window.scrollBy(0, -500);")
    time.sleep(0.5)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1)
    
    new_height = driver.execute_script("return document.body.scrollHeight")
    return new_height > last_height

def extract_listing_data(card, category):
    """Extrait les données d'une annonce avec extraction améliorée"""
    try:
        # Extraire l'URL
        url = None
        try:
            link_elem = card.find_element(By.CSS_SELECTOR, "a[href*='/marketplace/item/']")
            url = link_elem.get_attribute("href")
        except:
            url = card.get_attribute("href") if card.tag_name == 'a' else None
        
        if not url or '/marketplace/item/' not in url:
            return None
        
        # Extraire l'ID
        annonce_id = re.search(r'/item/(\d+)', url)
        annonce_id = annonce_id.group(1) if annonce_id else url
        
        # Récupérer tout le texte
        full_text = card.text
        if not full_text or len(full_text) < 5:
            return None
        
        # Extraire la description (premières lignes significatives)
        lines = [l.strip() for l in full_text.split('\n') if l.strip() and len(l.strip()) > 2]
        
        # Le titre est généralement la première ligne significative
        titre = lines[0] if lines else "Sans titre"
        
        # La description est le reste du texte (limité pour clarté)
        description = ' '.join(lines[:3]) if len(lines) > 1 else titre
        description = description[:500]  # Limiter à 500 caractères
        
        # Extraire le prix
        prix_text = None
        prix_patterns = [
            r'(\d[\d\s]+)\s*(?:TND|DT|د\.ت)',
            r'(\d[\d\s]+)\s*dinar',
        ]
        
        for pattern in prix_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                prix_text = match.group(0)
                break
        
        prix_affiche = clean_price(prix_text) if prix_text else None
        
        # Type d'annonce
        type_annonce = "Location" if category == "propertyrentals" else "Vente"
        
        # Type de bien
        type_bien = detect_property_type(full_text)
        
        # Localisation
        gouvernorat = extract_gouvernorat(full_text)
        ville = extract_ville(full_text, gouvernorat)
        
        # Localisation complète
        localisation_complete = gouvernorat
        if ville and ville != gouvernorat and ville != "Non spécifié":
            localisation_complete = f"{ville}, {gouvernorat}"
        
        # Image URL
        image_url = None
        try:
            img = card.find_element(By.CSS_SELECTOR, "img")
            image_url = img.get_attribute("src")
        except:
            pass
        
        return {
            "id": annonce_id,
            "type_annonce": type_annonce,
            "type_bien": type_bien,
            "description": description,
            "prix_affiche": prix_affiche,
            "ville": ville,
            "gouvernorat": gouvernorat,
            "localisation_complete": localisation_complete,
            "image_url": image_url
        }
        
    except Exception as e:
        return None

def scrape_facebook_marketplace(driver, max_scrolls=50, category="propertyrentals", all_annonces=None, seen_ids=None):
    """Scrape Facebook Marketplace avec gestion améliorée"""
    if all_annonces is None:
        all_annonces = []
    if seen_ids is None:
        seen_ids = set()
    
    try:
        # Construire l'URL
        marketplace_url = f"https://www.facebook.com/marketplace/112236612120705/{category}?locale=fr_FR"
        
        print(f"🌐 Chargement du Marketplace...")
        print(f"🔗 URL: {marketplace_url}")
        
        driver.get(marketplace_url)
        time.sleep(5)
        
        # Attendre le chargement initial
        try:
            WebDriverWait(driver, 15).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "a[href*='/marketplace/item/']")) > 0
            )
        except TimeoutException:
            print("⚠️  Timeout lors du chargement initial")
        
        print(f"📜 Début du scraping (max {max_scrolls} scrolls)")
        print(f"   Estimation: ~{max_scrolls * 15} annonces potentielles\n")
        
        no_new_content_count = 0
        max_no_new = 10
        
        for scroll_num in range(max_scrolls):
            print(f"📜 Scroll {scroll_num + 1}/{max_scrolls}", end=" ")
            
            # Scroll
            has_new_content = smart_scroll(driver, pause_time=random.uniform(1.5, 2.5))
            
            if not has_new_content:
                no_new_content_count += 1
                print(f"(⚠️  Pas de nouveau contenu {no_new_content_count}/{max_no_new})")
                
                if no_new_content_count >= 3:
                    # Force scroll
                    driver.execute_script("window.scrollTo(0, 0);")
                    time.sleep(1)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                
                if no_new_content_count >= max_no_new:
                    print(f"\n⚠️  Arrêt: Pas de nouveau contenu après {max_no_new} tentatives")
                    break
            else:
                print("(✓ Nouveau contenu)", end="")
            
            time.sleep(random.uniform(1, 2))
            
            # Récupérer les cartes
            card_selectors = [
                "a[href*='/marketplace/item/']",
                "div[class*='x9f619'] a[href*='/marketplace/item/']",
            ]
            
            cards = []
            for selector in card_selectors:
                try:
                    cards = driver.find_elements(By.CSS_SELECTOR, selector)
                    if cards:
                        break
                except:
                    continue
            
            if not cards:
                print(f" - ⚠️  Aucune carte trouvée")
                continue
            
            # Extraire les données
            new_count = 0
            for card in cards:
                listing_data = extract_listing_data(card, category)
                
                if not listing_data:
                    continue
                
                if listing_data["id"] in seen_ids:
                    continue
                
                seen_ids.add(listing_data["id"])
                all_annonces.append(listing_data)
                new_count += 1
            
            # Réinitialiser le compteur si nouvelles annonces
            if has_new_content or new_count > 0:
                no_new_content_count = 0
            
            print(f" → +{new_count} nouvelles (Total: {len(all_annonces)})")
            
            # Checkpoint tous les 10 scrolls
            if (scroll_num + 1) % 10 == 0 and all_annonces:
                save_checkpoint(all_annonces, category)
        
        print(f"\n{'='*70}")
        print(f"✅ SCRAPING TERMINÉ POUR CETTE CATÉGORIE")
        print(f"   📊 Total annonces: {len(all_annonces)}")
        print(f"{'='*70}")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption manuelle")
        if all_annonces:
            save_checkpoint(all_annonces, category)
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        if all_annonces:
            save_checkpoint(all_annonces, category)
    
    return all_annonces

def save_checkpoint(annonces, category):
    """Sauvegarde checkpoint"""
    os.makedirs("data/checkpoints", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    checkpoint_file = f"data/checkpoints/checkpoint_{category}_{timestamp}.csv"
    
    df = pd.DataFrame(annonces)
    # Supprimer les colonnes vides
    df = df.dropna(axis=1, how='all')
    df.to_csv(checkpoint_file, index=False, encoding='utf-8-sig')
    print(f"\n💾 Checkpoint sauvegardé: {checkpoint_file}")

def save_to_csv(annonces):
    """Sauvegarde finale en CSV optimisé"""
    if not annonces:
        print("⚠️  Aucune donnée à sauvegarder")
        return None
    
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"data/facebook_marketplace_{len(annonces)}_annonces_{timestamp}.csv"
    
    # Créer DataFrame
    df = pd.DataFrame(annonces)
    
    # Ordre des colonnes comme dans la capture
    colonnes_ordre = [
        "id",
        "type_annonce",
        "type_bien",
        "description",
        "prix_affiche",
        "devise",
        "ville",
        "gouvernorat",
        "localisation_complete",
        "image_url"
    ]
    
    # S'assurer que toutes les colonnes existent
    for col in colonnes_ordre:
        if col not in df.columns:
            df[col] = None
    
    # Ajouter la colonne devise (TND par défaut)
    df['devise'] = 'TND'
    
    # Sélectionner et réorganiser
    df = df[colonnes_ordre]
    
    # Supprimer les colonnes entièrement vides
    df = df.dropna(axis=1, how='all')
    
    # Sauvegarder
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    print(f"\n📁 Fichier CSV: {csv_path}")
    print(f"📊 Total: {len(annonces)} annonces")
    
    # Afficher statistiques
    print_statistics(df)
    
    return csv_path

def print_statistics(df):
    """Affiche des statistiques"""
    print(f"\n{'='*70}")
    print(f"📈 STATISTIQUES")
    print(f"{'='*70}")
    
    print(f"\n📋 PAR TYPE D'ANNONCE:")
    for type_ann, count in df['type_annonce'].value_counts().items():
        pct = (count / len(df)) * 100
        print(f"   • {type_ann}: {count} ({pct:.1f}%)")
    
    print(f"\n🏠 TOP 5 TYPES DE BIENS:")
    for type_bien, count in df['type_bien'].value_counts().head(5).items():
        pct = (count / len(df)) * 100
        print(f"   • {type_bien}: {count} ({pct:.1f}%)")
    
    print(f"\n🏙️  TOP 5 VILLES:")
    for ville, count in df['ville'].value_counts().head(5).items():
        pct = (count / len(df)) * 100
        print(f"   • {ville}: {count} ({pct:.1f}%)")
    
    if 'prix_affiche' in df.columns:
        prix_valides = df[df['prix_affiche'].notna()]
        print(f"\n💰 PRIX:")
        print(f"   • Annonces avec prix: {len(prix_valides)}/{len(df)} ({len(prix_valides)/len(df)*100:.1f}%)")

if __name__ == "__main__":
    print("\n🏠 SCRAPER FACEBOOK MARKETPLACE - VERSION AMÉLIORÉE")
    print("="*70)
    print("✨ Améliorations:")
    print("   • ✅ Extraction de données optimisée")
    print("   • ✅ Suppression des colonnes inutiles (prix_numerique, etage, disponibilite)")
    print("   • ✅ Format CSV propre similaire à la capture d'écran")
    print("   • ✅ Meilleure détection des types de biens")
    print("   • ✅ Localisation améliorée")
    print("="*70)
    
    print("\nChoisissez la catégorie:")
    print("1. Location (propertyrentals)")
    print("2. Vente (propertyforsale)")
    print("3. Les deux (dans le même fichier)")
    
    choice = input("\nVotre choix (1, 2 ou 3): ").strip()
    
    categories = []
    if choice == "1":
        categories = [("propertyrentals", "Location")]
    elif choice == "2":
        categories = [("propertyforsale", "Vente")]
    elif choice == "3":
        categories = [("propertyrentals", "Location"), ("propertyforsale", "Vente")]
    else:
        print("❌ Choix invalide")
        exit()
    
    try:
        max_scrolls = int(input("\nNombre de scrolls par catégorie (recommandé: 100-150): ").strip() or "100")
    except:
        max_scrolls = 100
    
    print(f"\n⏱️  Estimation: ~{max_scrolls * 15} annonces par catégorie")
    print(f"   Durée: ~{max_scrolls * 0.5:.0f} minutes par catégorie\n")
    
    input("👉 Appuyez sur ENTRÉE pour commencer...")
    
    # Créer le profil Chrome
    profile_dir = os.path.join(os.getcwd(), "chrome_profile_fb")
    os.makedirs(profile_dir, exist_ok=True)
    
    driver = create_facebook_driver(user_data_dir=profile_dir)
    
    all_annonces = []
    seen_ids = set()
    
    try:
        # Login
        login_facebook_manual(driver)
        
        # Scraper chaque catégorie
        for category_id, category_name in categories:
            print(f"\n{'='*70}")
            print(f"📁 CATÉGORIE: {category_name}")
            print(f"{'='*70}\n")
            
            scrape_facebook_marketplace(
                driver=driver,
                max_scrolls=max_scrolls,
                category=category_id,
                all_annonces=all_annonces,
                seen_ids=seen_ids
            )
            
            # Pause entre catégories
            if len(categories) > 1 and category_id != categories[-1][0]:
                print(f"\n⏸️  Pause de 3 minutes avant la prochaine catégorie...")
                time.sleep(180)
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption manuelle détectée")
    finally:
        print("\n🔒 Fermeture du navigateur...")
        driver.quit()
    
    # Sauvegarde finale
    if all_annonces:
        print(f"\n{'='*70}")
        print(f"💾 SAUVEGARDE FINALE")
        print(f"{'='*70}")
        
        csv_file = save_to_csv(all_annonces)
        
        if csv_file:
            print(f"\n✨ TERMINÉ!")
            print(f"📄 Fichier CSV: {csv_file}")
            print(f"📊 Total final: {len(all_annonces)} annonces uniques")
            print(f"\n💡 Ouvrez le CSV avec:")
            print(f"   • Excel: Double-clic sur le fichier")
            print(f"   • Python: pd.read_csv('{csv_file}')")
    else:
        print("\n⚠️  Aucune annonce récupérée")