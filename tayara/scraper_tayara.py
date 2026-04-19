import time, random, pandas as pd, os, re, json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import unicodedata

# ========== CONFIGURATION ==========
BASE_URL = "https://www.tayara.tn"
MAX_PAGES = 600
OUT = "tayara_complete.csv"
STATE_FILE = "scraper_state.json"

# PARAMÈTRES
PAGE_LOAD_TIMEOUT = 15
MIN_WAIT_DETAIL = 2
MAX_WAIT_DETAIL = 3
MIN_WAIT_LIST = 3
MAX_WAIT_LIST = 5
SCROLL_WAIT = 0.5
MAX_NO_NEW_PAGES = 5

# ========== NETTOYAGE ==========
def clean_text(text):
    if not text or not isinstance(text, str):
        return ""
    cleaned = ""
    for char in text:
        if char.isprintable() and not unicodedata.category(char).startswith('So'):
            cleaned += char
        elif char in [' ', '\n', '\t', ',', '.', '!', '?', '-', '_', '/', '\\', '(', ')', '[', ']']:
            cleaned += char
    return ' '.join(cleaned.split()).strip()

# ========== ÉTAT ==========
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"list_page": 1, "detail_index": -1, "list_done": False}

def save_state(list_page, detail_idx, list_done):
    with open(STATE_FILE, 'w') as f:
        json.dump({
            "list_page": int(list_page),
            "detail_index": int(detail_idx),
            "list_done": bool(list_done)
        }, f)

def clear_state():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

# ========== SELENIUM ==========
def create_driver():
    opt = Options()
    opt.add_argument("--headless=new")
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument("--window-size=1920,1080")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-gpu")
    opt.add_experimental_option('excludeSwitches', ['enable-logging'])
    opt.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=opt)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

# ========== EXTRACTION RÉGION ==========
def extract_region_from_url(url):
    try:
        parts = url.split('/')
        if 'item' in parts:
            idx = parts.index('item')
            if len(parts) > idx + 2:
                region = parts[idx + 2]
                return clean_text(region.replace('-', ' ').title())
    except:
        pass
    return "Non spécifiée"

# ========== EXTRACTION DÉTAILS ==========
def safe_find(driver, xpath, timeout=2):
    try:
        elem = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return clean_text(elem.text.strip())
    except:
        return ""

def find_price(driver):
    try:
        elem = driver.find_element(By.XPATH, "//data[@value]")
        val = elem.get_attribute("value")
        if val and val.replace(" ", "").isdigit():
            return f"{int(val):,} DT".replace(",", " ")
    except:
        pass
    try:
        elem = driver.find_element(By.XPATH, "//*[contains(text(),'DT')]")
        return clean_text(elem.text.strip())
    except:
        return ""

def find_title(driver):
    try:
        return clean_text(driver.find_element(By.XPATH, "//h1").text.strip())
    except:
        return ""

def find_description(driver):
    try:
        desc = driver.find_element(By.XPATH, "//p[@dir='auto']").text.strip()
        if len(desc) > 50:
            return clean_text(desc)
    except:
        pass
    try:
        paragraphs = driver.find_elements(By.TAG_NAME, "p")[:10]
        longest = max((p.text.strip() for p in paragraphs), key=len, default="")
        if len(longest) > 50:
            return clean_text(longest)
    except:
        pass
    return ""

def find_phone(driver):
    time.sleep(0.5)
    
    # Méthode 1: lien tel:
    try:
        elem = driver.find_element(By.XPATH, "//a[starts-with(@href, 'tel:')]")
        phone = elem.text.strip() or elem.get_attribute("href").replace("tel:", "").strip()
        if phone and len(phone) >= 8:
            return clean_text(phone)
    except:
        pass
    
    # Méthode 2: tous les liens tel:
    try:
        elems = driver.find_elements(By.XPATH, "//*[starts-with(@href, 'tel:')]")
        for elem in elems:
            phone = elem.text.strip() or elem.get_attribute("href").replace("tel:", "").strip()
            if phone and len(phone) >= 8:
                return clean_text(phone)
    except:
        pass
    
    # Méthode 3: regex source
    try:
        match = re.search(r'href="tel:(\+?216\d{8})"', driver.page_source)
        if match:
            return clean_text(match.group(1))
    except:
        pass
    
    # Méthode 4: regex body
    try:
        text = driver.find_element(By.TAG_NAME, "body").text
        patterns = [
            r'\+216\s?\d{2}\s?\d{3}\s?\d{3}',
            r'\+216\d{8}',
            r'216\d{8}',
            r'\b([2579]\d{7})\b'
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                phone = match.group(0)
                if len(re.sub(r'\D', '', phone)) >= 8:
                    return clean_text(phone)
    except:
        pass
    
    return ""

def find_images(driver):
    try:
        pattern = r'https://www\.tayara\.tn/mediaGateway/resize-image\?img=[^"&\s]+'
        matches = re.findall(pattern, driver.page_source)
        seen = set()
        images = []
        for url in matches:
            if url not in seen:
                seen.add(url)
                images.append(url)
        return "|".join(images) if images else ""
    except:
        return ""

def find_category(driver):
    try:
        return clean_text(driver.find_element(By.XPATH, "//a[contains(@href,'/c/')]").text.strip())
    except:
        return ""

def find_location(driver):
    try:
        return clean_text(driver.find_element(By.XPATH, "//span[contains(text(),',')]").text.strip())
    except:
        return ""

def extract_criteria(driver):
    data = {}
    try:
        rows = driver.find_elements(By.XPATH, "//li")[:20]
        for r in rows:
            lines = r.text.strip().split("\n")
            if len(lines) == 2:
                data[clean_text(lines[0].strip())] = clean_text(lines[1].strip())
    except:
        pass
    return data

def scrape_detail(driver, url, region):
    try:
        driver.get(url)
        
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1.5)
        except:
            time.sleep(2)
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(SCROLL_WAIT)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(SCROLL_WAIT)

        title = find_title(driver)
        price = find_price(driver)
        description = find_description(driver)
        phone = find_phone(driver)
        images = find_images(driver)
        category = find_category(driver)
        location = find_location(driver)
        date = safe_find(driver, "//span[contains(text(),'ago') or contains(text(),'hours') or contains(text(),'days')]", 1)
        data = extract_criteria(driver)

        return {
            "region": region,
            "title": title,
            "url": url,
            "price": price,
            "location": location,
            "category": category,
            "type_de_transaction": data.get("Type de transaction", ""),
            "chambres": data.get("Chambres", ""),
            "salles_de_bains": data.get("Salles de bains", ""),
            "superficie": data.get("Superficie", ""),
            "date": date,
            "description": description,
            "phone": phone,
            "images": images,
            "status": "ok",
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        print(f"      Erreur détail: {str(e)[:50]}")
        return {
            "region": region, "title": "", "url": url, "price": "", "location": "",
            "category": "", "type_de_transaction": "", "chambres": "", "salles_de_bains": "",
            "superficie": "", "date": "", "description": "", "phone": "", "images": "",
            "status": "error", "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

# ========== MAIN ==========
def main():
    print("=" * 70)
    print("🚀 SCRAPER TAYARA COMPLET")
    print("=" * 70)
    
    state = load_state()
    driver = create_driver()
    
    # Charger DataFrame
    if os.path.exists(OUT):
        df = pd.read_csv(OUT, dtype=str).fillna("")
        print(f"📂 {len(df)} annonces dans le fichier")
    else:
        df = pd.DataFrame(columns=["region", "title", "url", "price", "location", "category",
                                   "type_de_transaction", "chambres", "salles_de_bains", "superficie",
                                   "date", "description", "phone", "images", "status", "scraped_at"])
        print("📂 Nouveau fichier")
    
    existing_urls = set(df["url"].tolist())
    list_page = state["list_page"]
    detail_index = state["detail_index"]
    list_done = state["list_done"]
    
    start_time = time.time()
    
    print(f"\n📊 État:")
    print(f"   • URLs: {len(existing_urls)}")
    print(f"   • OK: {(df['status']=='ok').sum()}")
    print(f"   • Page liste: {list_page}")
    print(f"   • Liste terminée: {'Oui' if list_done else 'Non'}")
    print("=" * 70)
    
    try:
        # ========== PHASE 1: COLLECTE ==========
        if not list_done:
            print("\n📋 PHASE 1: COLLECTE URLs")
            print("-" * 70)
            
            consecutive_empty = 0
            
            while list_page <= MAX_PAGES:
                url = f"{BASE_URL}/c/immobilier?page={list_page}"
                print(f"\n📄 Page {list_page}", end=" ")
                
                try:
                    driver.get(url)
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'/item/')]"))
                    )
                    time.sleep(random.uniform(2, 4))
                    
                    cards = driver.find_elements(By.XPATH, "//a[contains(@href,'/item/')]")
                    
                    if not cards:
                        print("→ Aucune carte")
                        break
                    
                    new_rows = []
                    for card in cards:
                        link = card.get_attribute("href")
                        if not link or link in existing_urls:
                            continue
                        
                        try:
                            text = card.text.strip()
                            lines = [l.strip() for l in text.split("\n") if l.strip()]
                            title = lines[0] if lines else "Sans titre"
                            region = extract_region_from_url(link)
                            
                            new_rows.append({
                                "region": region, "title": title, "url": link,
                                "price": "", "location": "", "category": "", "type_de_transaction": "",
                                "chambres": "", "salles_de_bains": "", "superficie": "", "date": "",
                                "description": "", "phone": "", "images": "", "status": "pending", "scraped_at": ""
                            })
                            existing_urls.add(link)
                        except:
                            continue
                    
                    print(f"→ +{len(new_rows)} URLs")
                    
                    if new_rows:
                        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
                        df.to_csv(OUT, index=False, encoding='utf-8')
                        consecutive_empty = 0
                    else:
                        consecutive_empty += 1
                        if consecutive_empty >= MAX_NO_NEW_PAGES:
                            print(f"\n🛑 {MAX_NO_NEW_PAGES} pages vides")
                            break
                    
                    save_state(list_page, detail_index, False)
                    list_page += 1
                    time.sleep(random.uniform(MIN_WAIT_LIST, MAX_WAIT_LIST))
                    
                except Exception as e:
                    print(f"→ Erreur: {str(e)[:40]}")
                    time.sleep(5)
            
            list_done = True
            save_state(list_page, detail_index, True)
            print(f"\n✅ Phase 1 terminée → {len(df)} URLs au total")
        
        # ========== PHASE 2: DÉTAILS ==========
        print("\n" + "=" * 70)
        print("📝 PHASE 2: EXTRACTION DÉTAILS")
        print("-" * 70)
        
        # Recréer le driver
        try:
            driver.current_url
        except:
            driver = create_driver()
        
        # Filtrer
        to_scrape = df[(df['status']=='pending') | (df['status']=='error') | (df['status']=='')].copy()
        total_to_scrape = len(to_scrape)
        
        print(f"\n📊 {total_to_scrape} annonces à scraper")
        
        if total_to_scrape == 0:
            print("✅ Tout est déjà fait!")
        else:
            count = 0
            for idx, row in to_scrape.iterrows():
                real_idx = df[df['url']==row['url']].index[0]
                
                if real_idx <= detail_index:
                    continue
                
                url = row['url']
                region = extract_region_from_url(url) or row['region'] or "Non spécifiée"
                
                print(f"\n   [{real_idx+1}/{len(df)}] {region[:20]}")
                
                try:
                    driver.current_url
                except:
                    driver = create_driver()
                
                try:
                    details = scrape_detail(driver, url, region)
                    count += 1
                except Exception as e:
                    print(f"      ❌ {str(e)[:40]}")
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = create_driver()
                    details = {
                        "region": region, "title": "", "url": url, "price": "", "location": "",
                        "category": "", "type_de_transaction": "", "chambres": "", "salles_de_bains": "",
                        "superficie": "", "date": "", "description": "", "phone": "", "images": "",
                        "status": "error", "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                
                for key, val in details.items():
                    df.at[real_idx, key] = str(val) if val else ""
                
                nb_img = len(details['images'].split('|')) if details['images'] else 0
                icon = "✅" if details['status']=='ok' else "❌"
                print(f"      {icon} {details['price'][:15] if details['price'] else 'N/A'} | {details['phone'][:15] if details['phone'] else 'N/A'} | {nb_img}📷")
                
                df.to_csv(OUT, index=False, encoding='utf-8')
                save_state(list_page, real_idx, list_done)
                
                time.sleep(random.uniform(MIN_WAIT_DETAIL, MAX_WAIT_DETAIL))
                
                if count % 10 == 0:
                    done = (df['status']=='ok').sum()
                    print(f"\n      📊 {done}/{len(df)} complètes")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  INTERRUPTION")
        df.to_csv(OUT, index=False, encoding='utf-8')
    
    finally:
        try:
            driver.quit()
        except:
            pass
        df.to_csv(OUT, index=False, encoding='utf-8')
        
        elapsed = time.time() - start_time
        ok = (df['status']=='ok').sum()
        err = (df['status']=='error').sum()
        pend = (df['status']=='pending').sum()
        
        print("\n" + "=" * 70)
        print("✅ FIN")
        print("=" * 70)
        print(f"📊 Stats:")
        print(f"   • Total: {len(df)}")
        print(f"   • OK: {ok}")
        print(f"   • Erreurs: {err}")
        print(f"   • En attente: {pend}")
        print(f"   • Temps: {elapsed/60:.1f} min")
        print(f"   • Fichier: {OUT}")
        print("=" * 70)
        
        if pend == 0 and err == 0:
            print("\n🎉 100% TERMINÉ")
            clear_state()

if __name__ == "__main__":
    main()