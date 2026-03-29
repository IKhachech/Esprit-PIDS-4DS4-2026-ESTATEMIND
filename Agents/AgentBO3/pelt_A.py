"""
pelt_A.py — PELT Change Point Detection
Usage : python pelt_A.py
"""
import os, pickle, warnings
import numpy as np
import pandas as pd
import ruptures as rpt
warnings.filterwarnings('ignore')

MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODELS_DIR, exist_ok=True)


def preparer_serie(df, min_annonces=3):
    """
    Agrège par gouvernorat × annee × mois.
    Applique lissage 3 mois pour réduire le bruit.
    Garde seulement les points avec >= min_annonces annonces.
    """
    agg = (df.groupby(['gouvernorat', 'annee', 'mois'])
             ['indice_prix_m2_regional']
             .agg(['mean', 'count'])
             .reset_index())
    agg.columns = ['gouvernorat', 'annee', 'mois', 'prix', 'n']
    agg = agg[agg['n'] >= min_annonces].copy()
    agg = agg.sort_values(['gouvernorat', 'annee', 'mois'])

    # Lissage 3 mois — réduit le bruit, révèle la tendance
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['prix']
                            .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    return agg.reset_index(drop=True)


def run_pelt(groupes):
    print("\n" + "="*55)
    print("  PELT — Change Point Detection")
    print("="*55)

    resultats = {}
    nb_total  = 0

    for groupe, df in groupes.items():
        agg = preparer_serie(df)

        # Seuil dynamique = médiane des points par gouvernorat
        pts_par_gov = agg.groupby('gouvernorat').size()
        seuil       = float(pts_par_gov.median())
        govs_ok     = pts_par_gov[pts_par_gov >= seuil].index.tolist()

        print(f"\n  {groupe} : {len(govs_ok)}/{df['gouvernorat'].nunique()} "
              f"gouvernorats (seuil={seuil:.0f} pts)")

        ruptures = {}

        if govs_ok:
            for gov in govs_ok:
                sub = (agg[agg['gouvernorat'] == gov]
                       .sort_values(['annee', 'mois'])
                       .reset_index(drop=True))
                signal = sub['prix_lisse'].values

                if len(signal) < 4:
                    continue

                # Log transform avant PELT pour stabiliser la variance
                signal_log = np.log1p(signal)

                try:
                    algo = rpt.Pelt(model='rbf', min_size=2, jump=1)
                    algo.fit(signal_log.reshape(-1, 1))
                    # Pénalité adaptative selon variabilité de la série
                    pen  = np.log(len(signal_log)) * signal_log.std()
                    bps  = algo.predict(pen=max(pen, 0.5))
                    # PELT retourne toujours n en dernier → on l'enlève
                    bps  = [b for b in bps if b < len(signal_log)]

                    if bps:
                        print(f"    gov={gov} : {len(bps)} rupture(s) "
                              f"| prix avant={signal[:bps[0]].mean():.0f} "
                              f"| prix après={signal[bps[-1]:].mean():.0f} TND/m²")

                    ruptures[int(gov)] = {
                        'breakpoints': bps,
                        'n':           len(bps),
                        'prix_avant':  round(float(signal[:bps[0]].mean()), 2) if bps else None,
                        'prix_apres':  round(float(signal[bps[-1]:].mean()), 2) if bps else None,
                    }
                except Exception as e:
                    print(f"    gov={gov} : erreur → {e}")

        else:
            # Trop peu de données → série nationale agrégée
            print(f"    → Série nationale agrégée")
            signal = agg.groupby(['annee', 'mois'])['prix_lisse'].mean().values
            if len(signal) >= 4:
                try:
                    algo = rpt.Pelt(model='rbf', min_size=2, jump=1)
                    algo.fit(np.log1p(signal).reshape(-1, 1))
                    pen  = np.log(len(signal)) * np.log1p(signal).std()
                    bps  = [b for b in algo.predict(pen=max(pen, 0.5))
                            if b < len(signal)]
                    ruptures['national'] = {'breakpoints': bps, 'n': len(bps)}
                    print(f"    national : {len(bps)} rupture(s)")
                except Exception:
                    pass

        n_rup      = sum(r['n'] for r in ruptures.values() if isinstance(r, dict))
        nb_total  += n_rup
        resultats[groupe] = {'ruptures': ruptures, 'n_ruptures': n_rup}
        print(f"    Total {groupe} : {n_rup} ruptures")

    chemin = os.path.join(MODELS_DIR, 'pelt.pkl')
    with open(chemin, 'wb') as f:
        pickle.dump(resultats, f)

    print(f"\n  → Total ruptures toutes catégories : {nb_total}")
    print(f"  → Sauvegardé : {chemin}")
    return {'score': float(nb_total), 'metric': 'nb_ruptures', 'pkl': chemin}


if __name__ == '__main__':
    dossier = os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', '..', 'BO3'))
    print(f"Dossier : {dossier}")
    groupes = {}
    for nom in ['Residentiel', 'Foncier', 'Commercial', 'Divers']:
        f = os.path.join(dossier, f'{nom.lower()}_BO3.xlsx')
        if os.path.exists(f):
            groupes[nom] = pd.read_excel(f)
            print(f"  ✔ {nom} : {len(groupes[nom]):,} lignes")
        else:
            print(f"  ✗ {nom} manquant")
    if groupes:
        run_pelt(groupes)
