"""
pelt_A.py — Change Point Detection avec PELT
Version finale améliorée
"""

import os
import pickle
import warnings
import numpy as np
import pandas as pd
import ruptures as rpt

warnings.filterwarnings('ignore')

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODELS_DIR, exist_ok=True)

# ==================== PARAMÈTRES ====================
MIN_ANNONCES = 3      # Minimum d'annonces par mois/gouvernorat
MIN_SIGNAL   = 4      # Minimum de points pour appliquer PELT
MIN_SIZE     = 3      # Taille minimale d'un segment


def calculer_seuil_amplitude(df):
    """Calcule un seuil dynamique d'amplitude basé sur les variations réelles."""
    grp = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg = grp['indice_prix_m2_regional'].mean().reset_index()
    
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                         .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    
    agg['variation'] = (agg.groupby('gouvernorat')['prix_lisse'].pct_change().abs())
    variations = agg['variation'].dropna()
    
    seuil = float(variations.quantile(0.25)) if len(variations) > 0 else 0.08
    return round(max(0.05, min(0.15, seuil)), 3)


def preparer_serie(df):
    """Prépare et lisse les séries temporelles par gouvernorat."""
    cols = ['indice_prix_m2_regional', 'glissement_immo_trim']
    cols_ok = [c for c in cols if c in df.columns]
    
    grp = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg = grp[cols_ok].mean().join(grp.size().rename('n')).reset_index()
    
    agg = agg[agg['n'] >= MIN_ANNONCES].sort_values(['gouvernorat', 'annee', 'mois'])
    
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                         .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    
    return agg


def construire_signal(sub):
    """Construit le signal pour PELT (prix + glissement)."""
    prix = sub['prix_lisse'].values
    prix_n = (prix - prix.mean()) / (prix.std() + 1e-6)
    
    gliss_n = np.zeros_like(prix_n)
    if 'glissement_immo_trim' in sub.columns:
        gliss = sub['glissement_immo_trim'].fillna(0).values
        gliss_n = (gliss - gliss.mean()) / (gliss.std() + 1e-6)
    
    # Signal composite : prix dominant + glissement en soutien
    signal = np.column_stack([prix_n, gliss_n * 0.35])
    return signal, prix


def filtrer_ruptures(bps, prix, seuil=0.10):
    """Filtre les ruptures selon leur amplitude réelle."""
    bps_valides = []
    for bp in bps:
        if bp == 0 or bp >= len(prix):
            continue
        avant = prix[:bp].mean()
        apres = prix[bp:].mean()
        amplitude = abs(apres - avant) / (avant + 1e-6)
        if amplitude >= seuil:
            bps_valides.append(bp)
    return bps_valides


def detecter_ruptures(sub, seuil_amp=0.10):
    """Détection des ruptures avec PELT."""
    signal, prix = construire_signal(sub)
    n = len(prix)
    pen = np.log(n) * signal.std()
    
    algo = rpt.Pelt(model='rbf', min_size=MIN_SIZE, jump=1)
    algo.fit(signal)
    bps_raw = [b for b in algo.predict(pen=max(pen, 0.5)) if b < n]
    
    return filtrer_ruptures(bps_raw, prix, seuil_amp), prix


def run_pelt(groupes):
    print("\n" + "="*65)
    print("  PELT — Change Point Detection (Zones Émergentes)")
    print("="*65)

    resultats = {}
    nb_total = 0

    for groupe, df in groupes.items():
        print(f"\n→ Traitement du groupe : {groupe}")
        
        agg = preparer_serie(df)
        seuil_amp = calculer_seuil_amplitude(df)
        print(f"    Seuil amplitude dynamique : {seuil_amp*100:.1f}%")

        ruptures = {}
        govs_ok = agg['gouvernorat'].unique()

        for gov in govs_ok:
            sub = agg[agg['gouvernorat'] == gov].sort_values(['annee','mois']).reset_index(drop=True)
            
            if len(sub) < MIN_SIGNAL:
                continue

            try:
                bps, prix = detecter_ruptures(sub, seuil_amp)
                
                if bps:
                    dates_rup = []
                    for bp in bps:
                        if bp < len(sub):
                            row = sub.iloc[bp]
                            dates_rup.append(f"{int(row['annee'])}-{int(row['mois']):02d}")
                    
                    ruptures[int(gov)] = {
                        'breakpoints': bps,
                        'dates': dates_rup,
                        'n': len(bps),
                        'prix_avant': round(float(prix[:bps[0]].mean()), 2) if bps else None,
                        'prix_apres': round(float(prix[bps[-1]:].mean()), 2) if bps else None,
                    }
                    
                    delta = abs(ruptures[int(gov)]['prix_apres'] - ruptures[int(gov)]['prix_avant'])
                    print(f"    ✓ {gov} : {len(bps)} rupture(s) | Δ ≈ {delta:.0f} TND/m²")
            except Exception as e:
                print(f"    ✗ {gov} : erreur ({e})")

        n_rup = sum(r.get('n', 0) for r in ruptures.values())
        nb_total += n_rup
        resultats[groupe] = {'ruptures': ruptures, 'n_ruptures': n_rup}
        
        print(f"    Total {groupe} : {n_rup} ruptures détectées")

    # Sauvegarde
    chemin = os.path.join(MODELS_DIR, 'pelt.pkl')
    with open(chemin, 'wb') as f:
        pickle.dump(resultats, f)

    print(f"\n{'='*65}")
    print(f"✓ PELT terminé | Total ruptures validées : {nb_total}")
    print(f"✓ Modèle sauvegardé → {chemin}")
    print("="*65)

    return {'score': float(nb_total), 'metric': 'nb_ruptures', 'pkl': chemin}


# ===================== EXECUTION =====================
if __name__ == '__main__':
    dossier = os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', '..', 'BO3'))
    
    print(f"Dossier source : {dossier}\n")
    
    groupes = {}
    for nom in ['Residentiel', 'Foncier', 'Commercial', 'Divers']:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f"✓ {nom} chargé : {len(groupes[nom]):,} lignes")
        else:
            print(f"✗ {nom} manquant")

    if groupes:
        run_pelt(groupes)