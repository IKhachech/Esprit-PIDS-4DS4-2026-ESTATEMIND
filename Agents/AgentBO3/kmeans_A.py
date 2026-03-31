"""
kmeans_A.py — K-means Segmentation Géographique
"""
import os, pickle, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings('ignore')

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (silhouette_score,
                              davies_bouldin_score,
                              calinski_harabasz_score)

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODELS_DIR, exist_ok=True)

# Features validées par tests exhaustifs — maximisent le Silhouette Score
FEATURES_PAR_GROUPE = {
    'Residentiel': ['score_attractivite', 'nb_infra', 'nb_commerce',
                    'indice_prix_m2_regional',
                    'inflation_glissement_annuel', 'croissance_pib_trim',
                    'glissement_immo_trim'],
    'Foncier':     ['indice_prix_m2_regional',
                    'glissement_immo_trim'],
    'Commercial':  ['indice_prix_m2_regional',
                    'glissement_immo_trim'],
    'Divers':      ['indice_prix_m2_regional'],
}
FEATURES_DEFAULT = ['score_attractivite', 'nb_infra', 'nb_commerce',
                    'indice_prix_m2_regional']


def run_kmeans(groupes):
    print("\n" + "="*58)
    print("  K-MEANS — Segmentation géographique")
    print("="*58)

    resultats   = {}
    silhouettes = []

    for groupe, df in groupes.items():
        t    = df['indice_prix_m2_regional']
        p5   = t.quantile(0.05)
        p95  = t.quantile(0.95)
        df_c = df[(t >= p5) & (t <= p95)].copy()

        features = FEATURES_PAR_GROUPE.get(groupe, FEATURES_DEFAULT)
        feats    = [f for f in features if f in df_c.columns]
        gov_df   = df_c.groupby('gouvernorat')[feats].mean().reset_index()
        n_gov    = len(gov_df)

        if n_gov < 3:
            print(f"  {groupe} : {n_gov} gouvernorats → skip")
            continue

        scaler = StandardScaler()
        X_s    = scaler.fit_transform(gov_df[feats].values)

        best_k, best_sil = 2, -1
        for k in range(2, min(9, n_gov)):
            lbl = KMeans(n_clusters=k, random_state=42, n_init=15).fit_predict(X_s)
            sil = silhouette_score(X_s, lbl)
            if sil > best_sil:
                best_sil, best_k = sil, k

        km = KMeans(n_clusters=best_k, random_state=42, n_init=15)
        gov_df['cluster'] = km.fit_predict(X_s)

        db = davies_bouldin_score(X_s, gov_df['cluster'])
        ch = calinski_harabasz_score(X_s, gov_df['cluster'])

        print(f"\n  {groupe} ({len(feats)} features) :")
        print(f"    k={best_k} | Silhouette={best_sil:.4f} | "
              f"Davies-Bouldin={db:.4f}↓ | Calinski={ch:.1f}↑")
        for c in range(best_k):
            m    = gov_df['cluster'] == c
            govs = gov_df[m]['gouvernorat'].tolist()
            prix = gov_df[m]['indice_prix_m2_regional'].mean()
            print(f"    Cluster {c} ({len(govs)} govs | {prix:.0f} TND/m²) : {govs}")

        silhouettes.append(best_sil)
        resultats[groupe] = {
            'model':             km,
            'scaler':            scaler,
            'feats':             feats,
            'k':                 best_k,
            'silhouette':        best_sil,
            'davies_bouldin':    db,
            'calinski_harabasz': ch,
            'clusters':          gov_df[['gouvernorat','cluster']].to_dict('records'),
        }

    score  = float(np.mean(silhouettes)) if silhouettes else 0.0
    chemin = os.path.join(MODELS_DIR, 'kmeans.pkl')
    with open(chemin, 'wb') as f:
        pickle.dump(resultats, f)

    print(f"\n  → Silhouette moyen : {score:.4f}")
    print(f"  → Sauvegardé      : {chemin}")
    return {'score': score, 'metric': 'silhouette_score', 'pkl': chemin}


if __name__ == '__main__':
    # Chemin corrigé et plus visible
    dossier = os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', '..', 'BO3'))
    
    print(f" Dossier recherché : {dossier}")
    print(f" Chemin absolu du script : {os.path.abspath(__file__)}\n")

    groupes = {}
    for nom in ['Residentiel', 'Foncier', 'Commercial', 'Divers']:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        print(f" Vérification : {f}")
        
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f" {nom} chargé → {len(groupes[nom]):,} lignes")
        else:
            print(f" {nom} manquant → {f}")

    print(f"\n Total groupes trouvés : {len(groupes)}")

    if groupes:
        run_kmeans(groupes)
    else:
        print("\n ERREUR : Aucun fichier trouvé !")
        print("Vérifie que tes fichiers s'appellent exactement :")
        print("   - residentiel_BO3.xlsx")
        print("   - foncier_BO3.xlsx")
        print("   - commercial_BO3.xlsx")
        print("   - divers_BO3.xlsx")