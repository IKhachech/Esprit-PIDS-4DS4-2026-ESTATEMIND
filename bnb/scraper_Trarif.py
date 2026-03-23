import requests
from bs4 import BeautifulSoup
import pandas as pd

URL = "https://www.bnb.tn/les-tarifs-des-agences-immobilieres-en-tunisie-tout-ce-que-vous-devez-savoir/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
}

def clean(txt):
    return " ".join(txt.split()).strip()

# 1) Télécharger la page
r = requests.get(URL, headers=HEADERS, timeout=20)
r.raise_for_status()

soup = BeautifulSoup(r.text, "html.parser")

# 2) Récupérer le bloc exact
content = soup.select_one("div.content")
if not content:
    raise Exception("❌ div.content introuvable")

data = []

# 3) Parcourir les sections
for h3 in content.find_all("h3"):
    titre = clean(h3.get_text())

    bloc = []
    for el in h3.find_next_siblings():
        if el.name == "h3":
            break
        if el.name == "p":
            bloc.append(clean(el.get_text()))
        elif el.name == "ul":
            for li in el.find_all("li"):
                bloc.append("• " + clean(li.get_text()))

    description = " ".join(bloc)

    data.append({
        "Categorie": titre,
        "Description": description
    })

# 4) Export
df = pd.DataFrame(data)
df.to_csv("tarifs_agences_tunisie.csv", index=False, encoding="utf-8-sig")

print("✅ Fichier créé : tarifs_agences_tunisie.csv")
print(df.head())
