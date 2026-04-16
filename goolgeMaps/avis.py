"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   SCRAPER GOOGLE MAPS — PLAYWRIGHT — 100% GRATUIT — SANS API               ║
║   Agent Prédictif Tendances Immobilières Tunisie                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  INSTALLATION (une seule fois) :                                             ║
║    pip install playwright pandas openpyxl numpy                              ║
║    playwright install chromium                                               ║
║                                                                              ║
║  LANCEMENT :                                                                 ║
║    python scraper_playwright_tunisie.py                                      ║
║                                                                              ║
║  RÉSULTAT :                                                                  ║
║    signaux_immobilier_tunisie.xlsx  (8 onglets analytiques)                  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import random
import re
import time
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ══════════════════════════════════════════════════════════════════════════════
# ⚙️  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

FICHIER_SORTIE  = "signaux_immobilier_tunisie.xlsx"
FICHIER_BACKUP  = "backup_signaux.xlsx"
FICHIER_JSON    = "raw_data.json"

MAX_LIEUX_PAR_REQUETE = 15    # Lieux max scrapés par requête Google Maps
DELAI_MIN             = 3.0   # Secondes entre actions (ne pas réduire)
DELAI_MAX             = 6.0   # Anti-détection
BACKUP_TOUS_N         = 10    # Sauvegarde Excel toutes les N requêtes
MODE_HEADLESS         = True  # False = voir le navigateur (debug)

# ══════════════════════════════════════════════════════════════════════════════
# 🗺️  24 GOUVERNORATS
# ══════════════════════════════════════════════════════════════════════════════

GOUVERNORATS = {
    "Tunis":       {"region": "Grand Tunis",    "code": "11", "lat": 36.8190, "lng": 10.1658, "profil": "métropole",             "pop": 1056247},
    "Ariana":      {"region": "Grand Tunis",    "code": "12", "lat": 36.8625, "lng": 10.1956, "profil": "banlieue_nord",         "pop": 587200},
    "Ben Arous":   {"region": "Grand Tunis",    "code": "13", "lat": 36.7533, "lng": 10.2282, "profil": "banlieue_sud",          "pop": 703022},
    "Manouba":     {"region": "Grand Tunis",    "code": "14", "lat": 36.8100, "lng": 10.0970, "profil": "périurbain",            "pop": 391136},
    "Nabeul":      {"region": "Nord-Est",       "code": "21", "lat": 36.4561, "lng": 10.7376, "profil": "littoral_touristique",  "pop": 787920},
    "Zaghouan":    {"region": "Nord-Est",       "code": "22", "lat": 36.4029, "lng": 10.1426, "profil": "rural",                 "pop": 185888},
    "Bizerte":     {"region": "Nord-Est",       "code": "23", "lat": 37.2744, "lng":  9.8739, "profil": "industriel_littoral",   "pop": 568219},
    "Béja":        {"region": "Nord-Ouest",     "code": "31", "lat": 36.7256, "lng":  9.1817, "profil": "agricole",              "pop": 308553},
    "Jendouba":    {"region": "Nord-Ouest",     "code": "32", "lat": 36.5011, "lng":  8.7757, "profil": "frontalier",            "pop": 422717},
    "Kef":         {"region": "Nord-Ouest",     "code": "33", "lat": 36.1823, "lng":  8.7147, "profil": "intérieur",             "pop": 258591},
    "Siliana":     {"region": "Nord-Ouest",     "code": "34", "lat": 36.0847, "lng":  9.3714, "profil": "rural",                 "pop": 237217},
    "Sousse":      {"region": "Sahel",          "code": "41", "lat": 35.8245, "lng": 10.6346, "profil": "métropole_côtière",     "pop": 674535},
    "Monastir":    {"region": "Sahel",          "code": "42", "lat": 35.7643, "lng": 10.8113, "profil": "touristique",           "pop": 560605},
    "Mahdia":      {"region": "Sahel",          "code": "43", "lat": 35.5047, "lng": 11.0622, "profil": "littoral_émergent",     "pop": 414412},
    "Kasserine":   {"region": "Centre-Ouest",   "code": "51", "lat": 35.1671, "lng":  8.8365, "profil": "intérieur_industriel",  "pop": 436130},
    "Sidi Bouzid": {"region": "Centre-Ouest",   "code": "52", "lat": 35.0382, "lng":  9.4858, "profil": "agricole",              "pop": 438628},
    "Kairouan":    {"region": "Centre",         "code": "53", "lat": 35.6781, "lng": 10.0963, "profil": "religieux_artisanal",   "pop": 570559},
    "Sfax":        {"region": "Sud-Est",        "code": "61", "lat": 34.7398, "lng": 10.7600, "profil": "métropole_économique",  "pop": 955421},
    "Gabès":       {"region": "Sud-Est",        "code": "71", "lat": 33.8814, "lng": 10.0982, "profil": "industriel_chimique",   "pop": 374300},
    "Médenine":    {"region": "Sud-Est",        "code": "82", "lat": 33.3547, "lng": 10.4956, "profil": "frontalier_touristique","pop": 490931},
    "Tataouine":   {"region": "Sud-Est",        "code": "83", "lat": 32.9297, "lng": 10.4516, "profil": "frontalier_touristique","pop": 157447},
    "Gafsa":       {"region": "Sud-Ouest",      "code": "72", "lat": 34.4250, "lng":  8.7842, "profil": "minier",                "pop": 383914},
    "Tozeur":      {"region": "Sud-Ouest",      "code": "73", "lat": 33.9197, "lng":  8.1335, "profil": "oasien_touristique",    "pop": 108852},
    "Kébili":      {"region": "Sud-Ouest",      "code": "74", "lat": 33.7042, "lng":  8.9651, "profil": "saharien_touristique",  "pop": 165509},
}

# ══════════════════════════════════════════════════════════════════════════════
# 📡  12 SIGNAUX PRÉDICTIFS
# ══════════════════════════════════════════════════════════════════════════════

SIGNAUX = {
    "INFRA":          {"desc": "Infrastructure & développement",   "poids": 0.20, "requetes": ["zone industrielle", "zone d'activités économiques", "projet urbain"]},
    "MOBILITE":       {"desc": "Transport & connectivité",         "poids": 0.15, "requetes": ["gare routière", "station bus", "station service"]},
    "EMPLOI":         {"desc": "Emploi & activité économique",     "poids": 0.15, "requetes": ["usine", "centre d appels", "grande surface"]},
    "IMMO_DIRECT":    {"desc": "Marché immobilier direct",         "poids": 0.12, "requetes": ["agence immobilière", "promoteur immobilier", "notaire"]},
    "PROJETS_NEUFS":  {"desc": "Pipeline offre future",            "poids": 0.10, "requetes": ["résidence neuve", "appartement neuf", "lotissement"]},
    "CREDIT":         {"desc": "Crédit & capacité achat",          "poids": 0.08, "requetes": ["banque", "Banque de l Habitat", "agence bancaire"]},
    "EDUCATION":      {"desc": "Services éducatifs",               "poids": 0.07, "requetes": ["école", "lycée", "université"]},
    "SANTE":          {"desc": "Services de santé",                "poids": 0.05, "requetes": ["hôpital", "clinique", "pharmacie"]},
    "TOURISME":       {"desc": "Tourisme & locatif saisonnier",    "poids": 0.04, "requetes": ["hôtel", "maison d hôtes", "agence voyage"]},
    "QUALITE_VIE":    {"desc": "Qualité de vie & premium",         "poids": 0.02, "requetes": ["parc", "plage", "marina"]},
    "CONSTRUCTION":   {"desc": "BTP & futur stock logements",      "poids": 0.01, "requetes": ["matériaux construction", "quincaillerie bâtiment"]},
    "COMMERCE":       {"desc": "Vitalité commerciale locale",      "poids": 0.01, "requetes": ["supermarché", "centre commercial", "marché municipal"]},
}

# Aplatir la liste des requêtes
TOUTES_REQUETES = [
    (req, sig_id, sig["desc"])
    for sig_id, sig in SIGNAUX.items()
    for req in sig["requetes"]
]

POIDS = {k: v["poids"] for k, v in SIGNAUX.items()}


# ══════════════════════════════════════════════════════════════════════════════
# 🕷️  SCRAPER PLAYWRIGHT
# ══════════════════════════════════════════════════════════════════════════════

async def attendre(min_s=None, max_s=None):
    """Pause aléatoire anti-détection."""
    min_s = min_s or DELAI_MIN
    max_s = max_s or DELAI_MAX
    await asyncio.sleep(random.uniform(min_s, max_s))


def extraire_note(texte: str) -> float:
    """Extrait la note numérique depuis le texte Google Maps."""
    if not texte:
        return 0.0
    match = re.search(r"(\d[,\.]\d)", texte)
    if match:
        return float(match.group(1).replace(",", "."))
    match = re.search(r"(\d)\s+étoile", texte)
    if match:
        return float(match.group(1))
    return 0.0


def extraire_nb_avis(texte: str) -> int:
    """Extrait le nombre d'avis depuis le texte."""
    if not texte:
        return 0
    # Formats : "1 234 avis" / "1,234 reviews" / "(245)"
    texte_clean = texte.replace("\u202f", "").replace(" ", "").replace(",", "")
    match = re.search(r"(\d+)", texte_clean)
    return int(match.group(1)) if match else 0


async def scraper_requete(page, requete: str, gouvernorat: str,
                           signal_id: str, signal_desc: str,
                           infos: dict) -> list[dict]:
    """
    Scrape Google Maps pour une requête dans un gouvernorat.
    Retourne une liste de dicts (un dict = un lieu).
    """
    query  = f"{requete} {gouvernorat} Tunisie"
    url    = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    lieux  = []
    now    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await attendre(2, 4)

        # ── Accepter cookies si présents
        try:
            btn = page.locator("button:has-text('Tout accepter'), button:has-text('Accept all')")
            if await btn.count() > 0:
                await btn.first.click()
                await attendre(1, 2)
        except Exception:
            pass

        # ── Attendre les résultats
        try:
            await page.wait_for_selector(
                "[role='feed'] .Nv2PK, [jsaction*='mouseover'] .Nv2PK",
                timeout=15000
            )
        except PWTimeout:
            # Pas de résultats pour cette requête / gouvernorat
            return []

        # ── Scroll pour charger plus de résultats
        nb_scroll = min(3, MAX_LIEUX_PAR_REQUETE // 5)
        for _ in range(nb_scroll):
            try:
                panneau = page.locator("[role='feed']").first
                await panneau.evaluate("el => el.scrollBy(0, 800)")
                await attendre(1.5, 2.5)
            except Exception:
                break

        # ── Récupérer les cartes de lieux
        cartes = await page.locator("[role='feed'] .Nv2PK").all()
        cartes = cartes[:MAX_LIEUX_PAR_REQUETE]

        for carte in cartes:
            try:
                # Nom
                nom_el = carte.locator(".qBF1Pd, .fontHeadlineSmall")
                nom    = (await nom_el.first.inner_text()).strip() if await nom_el.count() > 0 else ""

                if not nom:
                    continue

                # Note et avis (format : "4,3 (127 avis)")
                note_el  = carte.locator(".MW4etd")
                note_txt = (await note_el.first.inner_text()).strip() if await note_el.count() > 0 else ""
                note     = extraire_note(note_txt)

                avis_el  = carte.locator(".UY7F9")
                avis_txt = (await avis_el.first.inner_text()).strip() if await avis_el.count() > 0 else ""
                nb_avis  = extraire_nb_avis(avis_txt)

                # Adresse / type
                infos_el   = carte.locator(".W4Efsd span")
                infos_list = []
                for el in await infos_el.all():
                    t = (await el.inner_text()).strip()
                    if t and t not in ["·", "•", ""]:
                        infos_list.append(t)

                type_lieu = infos_list[0] if len(infos_list) > 0 else ""
                adresse   = " ".join(infos_list[1:]) if len(infos_list) > 1 else ""

                # URL du lieu
                lien_el  = carte.locator("a.hfpxzc")
                url_lieu = await lien_el.first.get_attribute("href") if await lien_el.count() > 0 else ""

                # Statut (ouvert/fermé)
                statut_el  = carte.locator(".eXlnHd, .o0rrWb")
                statut_txt = (await statut_el.first.inner_text()).strip() if await statut_el.count() > 0 else ""
                est_ouvert = "fermé" not in statut_txt.lower() and "closed" not in statut_txt.lower()

                # Score d'activité (0-100)
                score = min(100, round(
                    (min(nb_avis, 500) / 500 * 60)
                    + (note / 5 * 30)
                    + (10 if est_ouvert else 0)
                ))

                lieux.append({
                    # ── LIEU
                    "nom_lieu":           nom,
                    "adresse_lieu":       adresse,
                    "type_lieu":          type_lieu,
                    "statut_ouvert":      "Ouvert" if est_ouvert else "Fermé/Inconnu",
                    "url_google_maps":    url_lieu or "",
                    "latitude":           infos["lat"],
                    "longitude":          infos["lng"],

                    # ── MÉTRIQUES
                    "note_google":        note,
                    "total_avis":         nb_avis,
                    "score_activite":     score,

                    # ── GÉOGRAPHIE
                    "gouvernorat":        gouvernorat,
                    "region":             infos["region"],
                    "code_gouv":          infos["code"],
                    "profil":             infos["profil"],
                    "population":         infos["pop"],

                    # ── SIGNAL
                    "signal_id":          signal_id,
                    "signal_description": signal_desc,
                    "requete":            requete,

                    # ── METADATA
                    "source":             "Google Maps (Playwright)",
                    "date_extraction":    now,
                })

            except Exception:
                continue

    except Exception as e:
        print(f"\n  ⚠️  Erreur '{requete}' / {gouvernorat}: {str(e)[:60]}")

    return lieux


async def scraper_detail_lieu(page, url: str) -> dict:
    """
    (Optionnel) Scrape la page détail d'un lieu pour enrichir les données.
    Récupère : téléphone, site web, horaires.
    """
    details = {"telephone": "", "site_web": "", "horaires": ""}

    if not url or not url.startswith("https://"):
        return details

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await attendre(1.5, 3)

        # Téléphone
        tel_el = page.locator("button[data-item-id*='phone'] .Io6YTe")
        if await tel_el.count() > 0:
            details["telephone"] = (await tel_el.first.inner_text()).strip()

        # Site web
        web_el = page.locator("a[data-item-id='authority'] .Io6YTe")
        if await web_el.count() > 0:
            details["site_web"] = (await web_el.first.inner_text()).strip()

        # Statut horaires
        hor_el = page.locator(".o0rrWb span")
        if await hor_el.count() > 0:
            details["horaires"] = (await hor_el.first.inner_text()).strip()

    except Exception:
        pass

    return details


# ══════════════════════════════════════════════════════════════════════════════
# 🚀  ORCHESTRATEUR PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

async def lancer_scraping(gouvernorats_cibles=None):
    """
    Lance le scraping complet.
    gouvernorats_cibles : liste de noms pour ne scraper qu'une partie (test)
    """
    gouvernorats = {k: v for k, v in GOUVERNORATS.items()
                    if gouvernorats_cibles is None or k in gouvernorats_cibles}

    total_req    = len(gouvernorats) * len(TOUTES_REQUETES)
    tous_lieux   = []
    lieux_vus    = set()
    compteur     = 0
    raw_data     = {}

    print(f"\n{'═'*68}")
    print(f"  🇹🇳  SCRAPING GOOGLE MAPS — PLAYWRIGHT — TUNISIE IMMOBILIER")
    print(f"{'═'*68}")
    print(f"  Gouvernorats  : {len(gouvernorats)}")
    print(f"  Requêtes/gouv : {len(TOUTES_REQUETES)}")
    print(f"  Total requêtes: {total_req:,}")
    print(f"  Durée estimée : {total_req * ((DELAI_MIN+DELAI_MAX)/2 + 3) / 60:.0f} min")
    print(f"{'═'*68}\n")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=MODE_HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="fr-TN",
            timezone_id="Africa/Tunis",
            viewport={"width": 1366, "height": 768},
        )
        page = await context.new_page()

        # Bloquer images/fonts → plus rapide
        await context.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot}",
            lambda route: route.abort()
        )

        for gouvernorat, infos in gouvernorats.items():
            print(f"\n{'─'*68}")
            print(f"  📍 {gouvernorat.upper()} — {infos['region']} | {infos['profil']}")
            print(f"{'─'*68}")
            raw_data[gouvernorat] = []

            for requete, signal_id, signal_desc in TOUTES_REQUETES:
                compteur += 1
                print(
                    f"  [{compteur:4d}/{total_req}] "
                    f"{signal_id:16s} | {requete[:30]:30s}",
                    end=" ", flush=True
                )

                lieux = await scraper_requete(
                    page, requete, gouvernorat,
                    signal_id, signal_desc, infos
                )

                # Dédupliquer (nom + gouvernorat)
                nouveaux = 0
                for l in lieux:
                    cle = (l["nom_lieu"].lower().strip(), l["gouvernorat"])
                    if cle[0] and cle not in lieux_vus:
                        lieux_vus.add(cle)
                        tous_lieux.append(l)
                        raw_data[gouvernorat].append(l)
                        nouveaux += 1

                print(f"→ {nouveaux:3d} lieux | Total : {len(tous_lieux):,}")

                # Backup régulier
                if compteur % BACKUP_TOUS_N == 0 and tous_lieux:
                    pd.DataFrame(tous_lieux).to_excel(FICHIER_BACKUP, index=False)
                    with open(FICHIER_JSON, "w", encoding="utf-8") as f:
                        json.dump(raw_data, f, ensure_ascii=False,
                                  default=str, indent=2)
                    print(f"  💾 Backup sauvegardé ({len(tous_lieux):,} lieux)")

                await attendre()

        await browser.close()

    return tous_lieux


# ══════════════════════════════════════════════════════════════════════════════
# 📊  CALCUL SCORE ATTRACTIVITÉ
# ══════════════════════════════════════════════════════════════════════════════

def calculer_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score d'Attractivité Immobilière (0-100) par gouvernorat.
    Formule pondérée basée sur 12 signaux.
    """
    rows = []
    for gouv, grp in df.groupby("gouvernorat"):
        g = GOUVERNORATS.get(gouv, {})
        score = 0.0
        nb_par_signal = {}
        note_par_signal = {}

        for sig_id, poids in POIDS.items():
            sg = grp[grp["signal_id"] == sig_id]
            nb  = len(sg)
            note = sg["note_google"][sg["note_google"] > 0].mean() if nb > 0 else 0.0
            note = note or 0.0

            nb_max  = MAX_LIEUX_PAR_REQUETE * len(SIGNAUX[sig_id]["requetes"])
            nb_norm = min(1.0, nb / max(1, nb_max))

            score += poids * nb_norm * (note / 5.0) * 100
            nb_par_signal[sig_id]   = nb
            note_par_signal[sig_id] = round(note, 2)

        score = round(min(100, score), 2)

        cat = (
            "🔥 Très attractif" if score >= 70 else
            "📈 Attractif"      if score >= 50 else
            "➡️  Neutre"        if score >= 30 else
            "📉 Peu attractif"  if score >= 15 else
            "❄️  Faible"
        )

        rows.append({
            "gouvernorat":           gouv,
            "region":                g.get("region", ""),
            "profil":                g.get("profil", ""),
            "population":            g.get("pop", 0),
            "score_attractivite":    score,
            "categorie":             cat,
            "nb_lieux_total":        len(grp),
            "note_globale_moy":      round(grp["note_google"][grp["note_google"]>0].mean() or 0, 2),
            # Nb lieux par signal
            **{f"nb_{s.lower()}":   nb_par_signal.get(s, 0)  for s in POIDS},
            # Note moyenne par signal
            **{f"note_{s.lower()}": note_par_signal.get(s, 0) for s in POIDS},
        })

    df_sc = pd.DataFrame(rows).sort_values("score_attractivite", ascending=False)
    df_sc.insert(0, "rang", range(1, len(df_sc)+1))
    return df_sc


# ══════════════════════════════════════════════════════════════════════════════
# 💾  SAUVEGARDE EXCEL (8 onglets)
# ══════════════════════════════════════════════════════════════════════════════

def sauvegarder_excel(tous_lieux: list):
    if not tous_lieux:
        print("⚠️  Aucune donnée collectée.")
        return

    df = pd.DataFrame(tous_lieux)
    df["note_google"] = pd.to_numeric(df["note_google"], errors="coerce").fillna(0)
    df["total_avis"]  = pd.to_numeric(df["total_avis"],  errors="coerce").fillna(0)
    df = df.drop_duplicates(subset=["nom_lieu", "gouvernorat"])

    df_scores = calculer_scores(df)

    print(f"\n{'═'*68}")
    print(f"  📊 RÉSULTATS FINAUX")
    print(f"{'═'*68}")
    print(f"  Lieux collectés     : {len(df):,}")
    print(f"  Gouvernorats        : {df['gouvernorat'].nunique()}/24")
    print(f"  Note moyenne        : {df.loc[df['note_google']>0,'note_google'].mean():.2f}/5")
    print(f"\n  🏆 Top 5 Gouvernorats :")
    for _, r in df_scores.head(5).iterrows():
        print(f"    {int(r['rang']):2d}. {r['gouvernorat']:15s} "
              f"→ {r['score_attractivite']:5.1f}/100  {r['categorie']}")

    with pd.ExcelWriter(FICHIER_SORTIE, engine="openpyxl") as writer:

        # Onglet 1 — Données brutes
        df.to_excel(writer, sheet_name="📋 Données Brutes", index=False)

        # Onglet 2 — Score attractivité (classement)
        df_scores.to_excel(writer, sheet_name="🏆 Score Attractivité", index=False)

        # Onglet 3 — Par région
        reg = df_scores.groupby("region").agg(
            nb_gouvernorats  = ("gouvernorat", "count"),
            score_moyen      = ("score_attractivite", "mean"),
            nb_lieux_total   = ("nb_lieux_total", "sum"),
            note_globale_moy = ("note_globale_moy", "mean"),
        ).round(2).reset_index().sort_values("score_moyen", ascending=False)
        reg.to_excel(writer, sheet_name="🌍 Par Région", index=False)

        # Onglet 4 — Analyse par signal
        sig_stats = []
        for sig_id, sig in SIGNAUX.items():
            sous = df[df["signal_id"] == sig_id]
            sig_stats.append({
                "signal_id":       sig_id,
                "description":     sig["desc"],
                "poids_agent":     sig["poids"],
                "nb_lieux":        len(sous),
                "nb_gouvernorats": sous["gouvernorat"].nunique(),
                "note_moyenne":    round(sous.loc[sous["note_google"]>0,"note_google"].mean() or 0, 2),
                "nb_actifs":       (sous["statut_ouvert"] == "Ouvert").sum(),
                "total_avis":      int(sous["total_avis"].sum()),
            })
        pd.DataFrame(sig_stats).sort_values("nb_lieux", ascending=False)\
          .to_excel(writer, sheet_name="📡 Analyse Signaux", index=False)

        # Onglet 5 — Top lieux (note ≥ 4 + avis ≥ 5)
        top = df[(df["note_google"] >= 4) & (df["total_avis"] >= 5)]\
            .sort_values("total_avis", ascending=False)\
            .head(200)[[
                "signal_id","gouvernorat","region","nom_lieu","adresse_lieu",
                "note_google","total_avis","score_activite",
                "statut_ouvert","url_google_maps"
            ]]
        top.to_excel(writer, sheet_name="⭐ Top Lieux", index=False)

        # Onglet 6 — Lieux à surveiller (peu d'avis ou mal notés)
        alerte = df[
            ((df["note_google"] > 0) & (df["note_google"] < 3)) |
            ((df["total_avis"] >= 20) & (df["note_google"] < 3.5))
        ].sort_values("note_google")[[
            "gouvernorat","region","signal_id","nom_lieu","adresse_lieu",
            "note_google","total_avis","statut_ouvert","url_google_maps"
        ]]
        alerte.to_excel(writer, sheet_name="⚠️ Alertes Qualité", index=False)

        # Onglet 7 — Activité construction & projets neufs
        constr = df[df["signal_id"].isin(["CONSTRUCTION","PROJETS_NEUFS"])]\
            .sort_values(["gouvernorat","total_avis"], ascending=[True,False])[[
                "gouvernorat","region","signal_id","nom_lieu","adresse_lieu",
                "note_google","total_avis","score_activite","url_google_maps",
                "latitude","longitude"
            ]]
        constr.to_excel(writer, sheet_name="🏗️ Construction & Projets", index=False)

        # Onglet 8 — Dictionnaire signaux
        dico = pd.DataFrame([
            {
                "signal_id":    sid,
                "description":  s["desc"],
                "poids":        s["poids"],
                "nb_requetes":  len(s["requetes"]),
                "requetes":     " | ".join(s["requetes"]),
                "logique": {
                    "INFRA":         "Zones industrielles/ZAE → hausse valeur foncière future",
                    "MOBILITE":      "Densité transport → prime localisation +15–30%",
                    "EMPLOI":        "Bassin emploi dense → demande résidentielle stable",
                    "IMMO_DIRECT":   "Nb agences actives → proxy liquidité du marché",
                    "PROJETS_NEUFS": "Nb projets neufs → offre disponible dans 2–5 ans",
                    "CREDIT":        "Densité bancaire → capacité d'achat réelle",
                    "EDUCATION":     "Équipements scolaires → attractivité des familles",
                    "SANTE":         "Équipements santé → niveau de vie et densité",
                    "TOURISME":      "Hôtels/agences → demande locative saisonnière",
                    "QUALITE_VIE":   "Parcs/plages/golf → premium de localisation",
                    "CONSTRUCTION":  "BTP actif → futur stock, possible pression sur prix",
                    "COMMERCE":      "Commerces actifs → vitalité économique locale",
                }.get(sid, "")
            }
            for sid, s in SIGNAUX.items()
        ])
        dico.to_excel(writer, sheet_name="📖 Dictionnaire Signaux", index=False)

    print(f"\n{'═'*68}")
    print(f"  ✅ FICHIER SAUVEGARDÉ : {FICHIER_SORTIE}")
    print(f"{'═'*68}")
    print(f"  Onglet 1 → 📋 Données brutes     ({len(df):,} lieux)")
    print(f"  Onglet 2 → 🏆 Score Attractivité  (classement 24 gouvernorats)")
    print(f"  Onglet 3 → 🌍 Synthèse par région")
    print(f"  Onglet 4 → 📡 Analyse par signal")
    print(f"  Onglet 5 → ⭐ Top 200 lieux actifs")
    print(f"  Onglet 6 → ⚠️  Alertes qualité")
    print(f"  Onglet 7 → 🏗️  Construction & projets neufs")
    print(f"  Onglet 8 → 📖 Dictionnaire signaux")
    print(f"{'═'*68}")


# ══════════════════════════════════════════════════════════════════════════════
# 🚀  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║  SCRAPER GOOGLE MAPS — PLAYWRIGHT — TUNISIE IMMOBILIER               ║
║  100% Gratuit | Aucune API | 12 signaux prédictifs                   ║
╚══════════════════════════════════════════════════════════════════════╝
    """)

    print("Choisir le mode de lancement :")
    print("  1 → Scraping complet (24 gouvernorats, ~2h)")
    print("  2 → Scraping rapide  (Grand Tunis + Sousse + Sfax, ~15 min)")
    print("  3 → Test 1 gouvernorat (Tunis, ~3 min)")
    print()

    choix = input("Choix [1/2/3] : ").strip() or "3"

    if choix == "1":
        gouv_cibles = None   # tous
    elif choix == "2":
        gouv_cibles = ["Tunis", "Ariana", "Ben Arous", "Manouba",
                       "Sousse", "Sfax", "Nabeul", "Monastir"]
    else:
        gouv_cibles = ["Tunis"]

    print(f"\n🚀 Démarrage...")
    print(f"   Fichier sortie : {FICHIER_SORTIE}")
    print(f"   Backup auto    : {FICHIER_BACKUP}\n")

    tous_lieux = asyncio.run(lancer_scraping(gouv_cibles))

    sauvegarder_excel(tous_lieux)

    print(f"\n🎉 TERMINÉ !")
    print(f"   → Ouvre {FICHIER_SORTIE}")


if __name__ == "__main__":
    main()