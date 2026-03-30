"""
autoencoder_tunisia.py — Autoencoder + KMeans pour la Tunisie
==============================================================
Réseau de neurones autoencodeur suivi de KMeans pour la segmentation.
Version corrigée avec gestion des cas où KMeans ne trouve pas assez de clusters.
"""

import os
import pickle
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.cluster import KMeans
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import silhouette_score

# Mapping des gouvernorats
GOV_NAMES = {
    1: 'Ariana', 2: 'Béja', 3: 'Ben Arous', 4: 'Bizerte', 5: 'Gabès', 6: 'Gafsa',
    7: 'Jendouba', 8: 'Kairouan', 9: 'Kasserine', 10: 'Kébili', 11: 'Le Kef', 12: 'Mahdia',
    13: 'Manouba', 14: 'Médenine', 15: 'Monastir', 16: 'Nabeul', 17: 'Sfax', 18: 'Sidi Bouzid',
    19: 'Siliana', 20: 'Sousse', 21: 'Tataouine', 22: 'Tozeur', 23: 'Tunis', 24: 'Zaghouan'
}

CLUSTER_NAMES = {
    'Residentiel': {0: 'Grand Tunis et Grandes Villes', 1: 'Zones Côtières Dynamiques', 2: 'Zones Intérieures et Rurales'},
    'Foncier': {0: 'Grand Tunis et Pôles Urbains', 1: 'Zones Côtières', 2: 'Zones Intérieures'},
    'Commercial': {0: 'Pôles Commerciaux Majeurs', 1: 'Marchés Commerciaux Secondaires', 2: 'Zones à Faible Activité Commerciale'},
    'Divers': {0: 'Cœur Métropolitain', 1: 'Zones Côtières', 2: 'Zones Intérieures'}
}

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODELS_DIR, exist_ok=True)

FEATURES_BY_SEGMENT = {
    'Residentiel': ['score_attractivite', 'nb_infra', 'nb_commerce', 'indice_prix_m2_regional'],
    'Foncier': ['indice_prix_m2_regional'],
    'Commercial': ['score_attractivite', 'nb_infra', 'nb_commerce', 'indice_prix_m2_regional'],
    'Divers': ['score_attractivite', 'nb_infra', 'nb_commerce', 'indice_prix_m2_regional'],
}

K = 3


class Autoencoder(nn.Module):
    """Autoencodeur simple pour réduction de dimension."""
    def __init__(self, input_dim, encoding_dim=8):
        super(Autoencoder, self).__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 16),
            nn.ReLU(),
            nn.Linear(16, encoding_dim),
            nn.ReLU()
        )
        self.decoder = nn.Sequential(
            nn.Linear(encoding_dim, 16),
            nn.ReLU(),
            nn.Linear(16, input_dim),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return encoded, decoded


def load_data():
    """Charge les données."""
    data_dir = None
    for path in [os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'BO3'),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'BO3'),
                 os.path.dirname(os.path.abspath(__file__))]:
        if os.path.exists(path):
            data_dir = path
            break
    
    if not data_dir:
        print("❌ Dossier BO3 introuvable")
        return {}
    
    print(f"📁 Données : {data_dir}")
    
    groupes = {}
    for nom in ['Residentiel', 'Foncier', 'Commercial', 'Divers']:
        f = os.path.join(data_dir, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            df = pd.read_excel(f)
            groupes[nom] = df
            print(f"  ✔ {nom} : {len(df):,} lignes")
    
    return groupes


def train_autoencoder(X, input_dim, epochs=300, lr=0.001):
    """Entraîne l'autoencodeur."""
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = Autoencoder(input_dim, encoding_dim=min(8, input_dim)).to(device)
    optimizer = optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()
    
    X_tensor = torch.tensor(X, dtype=torch.float32).to(device)
    
    for epoch in range(epochs):
        optimizer.zero_grad()
        encoded, decoded = model(X_tensor)
        loss = criterion(decoded, X_tensor)
        loss.backward()
        optimizer.step()
        
        if (epoch + 1) % 100 == 0:
            print(f"      Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}")
    
    model.eval()
    with torch.no_grad():
        encoded, _ = model(X_tensor)
    
    return model, encoded.cpu().numpy()


def run_autoencoder_kmeans(groupes):
    """Autoencoder + KMeans avec gestion des erreurs."""
    print("\n" + "="*60)
    print("  AUTOENCODER + KMEANS (k=3) - TUNISIE")
    print("="*60)
    
    resultats = {}
    
    for nom, df in groupes.items():
        print(f"\n  --- {nom} ---")
        
        features = FEATURES_BY_SEGMENT[nom]
        
        # Agrégation par gouvernorat
        gov = df.groupby('gouvernorat')[features].mean().reset_index()
        
        # Normalisation
        scaler = RobustScaler()
        X = scaler.fit_transform(gov[features].values)
        
        # Autoencoder
        print(f"    Entraînement de l'autoencodeur...")
        input_dim = X.shape[1]
        model, encoded = train_autoencoder(X, input_dim, epochs=300)
        
        # KMeans sur les features encodées
        # Vérifier si on peut faire du clustering
        n_samples = len(encoded)
        k_actual = min(K, n_samples - 1)
        
        if n_samples < 3 or k_actual < 2:
            print(f"    ⚠️ Pas assez de données pour le clustering ({n_samples} samples)")
            # Si pas assez de données, tous dans un seul cluster
            labels = np.zeros(n_samples, dtype=int)
            sil = -1
        else:
            kmeans = KMeans(n_clusters=k_actual, random_state=42, n_init=10)
            labels = kmeans.fit_predict(encoded)
            
            # Vérifier le nombre de clusters distincts
            n_distinct = len(set(labels))
            if n_distinct < 2:
                print(f"    ⚠️ KMeans n'a trouvé qu'un seul cluster, passage à k=2")
                kmeans = KMeans(n_clusters=min(2, n_samples-1), random_state=42, n_init=10)
                labels = kmeans.fit_predict(encoded)
            
            # Calculer silhouette si possible
            if len(set(labels)) >= 2:
                sil = silhouette_score(encoded, labels)
            else:
                sil = -1
        
        gov['cluster'] = labels
        n_clusters = len(set(labels))
        
        print(f"    k={n_clusters} | Silhouette={sil:.3f}" if sil > 0 else f"    k={n_clusters}")
        
        # Affichage des clusters
        for c in sorted(set(labels)):
            govs = gov[gov['cluster'] == c]['gouvernorat'].map(GOV_NAMES).tolist()
            prix = gov[gov['cluster'] == c]['indice_prix_m2_regional'].mean()
            name = CLUSTER_NAMES[nom].get(c, f'Cluster {c}')
            print(f"    C{c} - {name} ({len(govs)} govs | {prix:.0f} TND): {', '.join(govs)}")
        
        resultats[nom] = {
            'model': (model, kmeans if 'kmeans' in locals() else None),
            'scaler': scaler,
            'feats': features,
            'k': n_clusters,
            'silhouette': sil if sil > 0 else None,
            'cluster_map': gov.set_index('gouvernorat')['cluster'].to_dict(),
            'cluster_names': CLUSTER_NAMES[nom],
        }
    
    return resultats


def save_results(resultats, groupes):
    """Sauvegarde."""
    with open(os.path.join(MODELS_DIR, 'autoencoder_kmeans.pkl'), 'wb') as f:
        pickle.dump(resultats, f)
    print(f"\n✅ Modèle: {MODELS_DIR}/autoencoder_kmeans.pkl")
    
    dfs = []
    for nom, df in groupes.items():
        d = df.copy()
        d['segment'] = nom
        if nom in resultats:
            d['cluster'] = d['gouvernorat'].map(resultats[nom]['cluster_map'])
            d['cluster_name'] = d['cluster'].map(resultats[nom]['cluster_names'])
        dfs.append(d)
    
    df_all = pd.concat(dfs, ignore_index=True)
    df_all.to_csv(os.path.join(MODELS_DIR, 'BO3_consolidated_clustered.csv'), index=False)
    print(f"✅ Dataset: {MODELS_DIR}/BO3_consolidated_clustered.csv")


def print_summary(resultats):
    """Résumé final."""
    print("\n" + "="*60)
    print("  RÉSUMÉ FINAL - CLUSTERS TUNISIE (AUTOENCODER + KMEANS)")
    print("="*60)
    
    for nom, res in resultats.items():
        sil_display = f"{res['silhouette']:.3f}" if res['silhouette'] else "N/A"
        print(f"\n📊 {nom} (k={res['k']} | Silhouette={sil_display})")
        
        clusters = {}
        for gov, c in res['cluster_map'].items():
            clusters.setdefault(c, []).append(GOV_NAMES.get(gov, str(gov)))
        
        for c in sorted(clusters.keys()):
            name = res['cluster_names'].get(c, f'Cluster {c}')
            govs = clusters[c]
            print(f"   C{c} - {name}: {', '.join(govs)}")


if __name__ == '__main__':
    groupes = load_data()
    
    if not groupes:
        print("\n❌ Aucune donnée chargée")
        exit(1)
    
    resultats = run_autoencoder_kmeans(groupes)
    save_results(resultats, groupes)
    print_summary(resultats)
    
    print("\n" + "="*60)
    print("✅ Autoencoder + KMeans Segmentation terminée !")
    print("="*60)