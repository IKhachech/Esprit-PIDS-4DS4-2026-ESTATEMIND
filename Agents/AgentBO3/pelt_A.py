"""
Change Point Detection
=========================================
PELT = Pruned Exact Linear Time
"""
import os, pickle, warnings
import numpy as np
import pandas as pd
import ruptures as rpt
warnings.filterwarnings('ignore')

MODELS_DIR       = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
os.makedirs(MODELS_DIR, exist_ok=True)

MIN_ANNONCES     = 3     # annonces minimales par point de série
MIN_SIGNAL       = 4     # points minimaux pour appliquer PELT
MIN_SIZE         = 3     # segment minimal entre 2 ruptures


def calculer_seuil_amplitude(df, min_ann=3):
    grp = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg = grp['indice_prix_m2_regional'].mean().reset_index()
    agg.columns = ['gouvernorat', 'annee', 'mois', 'prix']
    agg = agg.sort_values(['gouvernorat', 'annee', 'mois']).reset_index(drop=True)

    # Lissage 3 mois AVANT calcul des variations
    # → les variations brutes sont trop volatiles (médiane ~50%)
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['prix']
                            .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    agg['variation']  = (agg.groupby('gouvernorat')['prix_lisse']
                            .pct_change().abs())

    variations = agg['variation'].dropna()
    if len(variations) == 0:
        return 0.08

    # Q1 des variations lissées : une rupture doit dépasser 75% des variations normales
    seuil = float(variations.quantile(0.25))
    return round(max(0.05, min(0.15, seuil)), 3)


def preparer_serie(df):
    """Agrège + lissage 3 mois."""
    cols    = ['indice_prix_m2_regional', 'glissement_immo_trim']
    cols_ok = [c for c in cols if c in df.columns]
    grp     = df.groupby(['gouvernorat', 'annee', 'mois'])
    agg     = grp[cols_ok].mean().join(grp.size().rename('n')).reset_index()
    agg     = agg[agg['n'] >= MIN_ANNONCES].sort_values(
                ['gouvernorat','annee','mois']).reset_index(drop=True)
    agg['prix_lisse'] = (agg.groupby('gouvernorat')['indice_prix_m2_regional']
                            .transform(lambda x: x.rolling(3, min_periods=1).mean()))
    return agg


def construire_signal(sub):
    """
    Construit le signal d'entrée pour PELT.
    Si glissement_immo_trim disponible → signal composite 2D
    Sinon → prix seul 1D
    """
    prix = sub['prix_lisse'].values

    if 'glissement_immo_trim' in sub.columns:
        gliss   = sub['glissement_immo_trim'].fillna(0).values
        prix_n  = (prix  - prix.mean())  / (prix.std()  + 1e-6)
        gliss_n = (gliss - gliss.mean()) / (gliss.std() + 1e-6)
        # Prix = composante principale, glissement = signal secondaire (poids 0.3)
        return np.column_stack([prix_n, gliss_n * 0.3]), prix
    else:
        return np.log1p(prix).reshape(-1, 1), prix


def filtrer_ruptures(bps, prix, seuil=0.10):
    """
    Filtre les ruptures dont l'amplitude est inférieure au seuil dynamique.
    seuil = calculé depuis les données du groupe (médiane des variations).
    """ 
    bps_valides = []
    for bp in bps:
        avant     = prix[:bp].mean()
        apres     = prix[bp:].mean()
        amplitude = abs(apres - avant) / (avant + 1e-6)
        if amplitude >= seuil:
            bps_valides.append(bp)
    return bps_valides


def detecter_ruptures(sub, seuil=0.10):
    """Détection PELT avec signal composite + filtre amplitude dynamique."""
    signal, prix = construire_signal(sub)
    n   = len(prix)
    pen = np.log(n) * signal.std()

    algo = rpt.Pelt(model='rbf', min_size=MIN_SIZE, jump=1)
    algo.fit(signal)
    bps_raw = [b for b in algo.predict(pen=max(pen, 0.5)) if b < n]

    return filtrer_ruptures(bps_raw, prix, seuil), prix


def run_pelt(groupes):
    print("\n" + "=" * 58)
    print("  PELT — Change Point Detection (zones émergentes)")
    print("=" * 58)

    resultats = {}
    nb_total  = 0

    for groupe, df in groupes.items():
        agg         = preparer_serie(df)
        pts_par_gov = agg.groupby('gouvernorat').size()
        seuil       = float(pts_par_gov.median())
        govs_ok     = pts_par_gov[pts_par_gov >= seuil].index.tolist()

        print(f"\n  {groupe} : {len(govs_ok)}/{df['gouvernorat'].nunique()} "
              f"gouvernorats éligibles (seuil={seuil:.0f} pts)")

        ruptures = {}

        # Seuil dynamique calculé depuis les données réelles du groupe
        seuil_amp = calculer_seuil_amplitude(df)
        print(f"    Seuil amplitude dynamique : {seuil_amp*100:.1f}%")

        if govs_ok:
            for gov in govs_ok:
                sub = (agg[agg['gouvernorat'] == gov]
                       .sort_values(['annee','mois']).reset_index(drop=True))

                if len(sub) < MIN_SIGNAL:
                    continue

                try:
                    bps, prix = detecter_ruptures(sub, seuil_amp)

                    if bps:
                        amplitude = abs(prix[bps[-1]:].mean() - prix[:bps[0]].mean())
                        print(f"    gov={gov} : {len(bps)} rupture(s) "
                              f"| avant={prix[:bps[0]].mean():.0f} "
                              f"| après={prix[bps[-1]:].mean():.0f} TND/m² "
                              f"(Δ={amplitude:.0f})")

                    # Convertir indices → dates réelles
                    dates_rup = []
                    for bp in bps:
                        if bp < len(sub):
                            row = sub.iloc[bp]
                            dates_rup.append(f"{int(row['annee'])}-{int(row['mois']):02d}")

                    ruptures[int(gov)] = {
                        'breakpoints': bps,
                        'dates':       dates_rup,
                        'n':           len(bps),
                        'prix_avant':  round(float(prix[:bps[0]].mean()), 2) if bps else None,
                        'prix_apres':  round(float(prix[bps[-1]:].mean()), 2) if bps else None,
                    }
                except Exception as e:
                    print(f"    gov={gov} : erreur → {e}")

        else:
            # Série nationale agrégée
            print(f"    → Mode série nationale agrégée")
            signal_nat = agg.groupby(['annee','mois'])['prix_lisse'].mean().values
            if len(signal_nat) >= MIN_SIGNAL:
                try:
                    pen = np.log(len(signal_nat)) * np.log1p(signal_nat).std()
                    algo = rpt.Pelt(model='rbf', min_size=MIN_SIZE, jump=1)
                    algo.fit(np.log1p(signal_nat).reshape(-1,1))
                    bps = [b for b in algo.predict(pen=max(pen,0.5))
                           if b < len(signal_nat)]
                    bps = filtrer_ruptures(bps, signal_nat)
                    ruptures['national'] = {'breakpoints': bps, 'n': len(bps)}
                    if bps:
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

    print(f"\n  → Total ruptures validées : {nb_total}")
    print(f"  → Sauvegardé             : {chemin}")
    # Top 5 ruptures les plus importantes
    print("\n  Top ruptures (delta le plus grand) :")
    top = []
    for grp, res in resultats.items():
        for gov, r in res['ruptures'].items():
            if isinstance(r, dict) and r['n'] > 0 and r['prix_avant'] and r['prix_apres']:
                delta = abs(r['prix_apres'] - r['prix_avant'])
                dates = r.get('dates', [])
                top.append((delta, grp, gov, r['prix_avant'], r['prix_apres'], dates))
    for delta, grp, gov, avant, apres, dates in sorted(top, reverse=True)[:5]:
        sens = 'hausse' if apres > avant else 'baisse'
        d = dates[0] if dates else '?'
        print(f"    {grp} gov={gov}: {avant:.0f}->{apres:.0f} TND/m2 ({sens} +{delta:.0f}) depuis {d}")

    return {'score': float(nb_total), 'metric': 'nb_ruptures', 'pkl': chemin}


def visualiser_ruptures(groupe, gov, df, resultats, save_dir=None):
    """
    Affiche le graphique des ruptures pour un gouvernorat.
    Utile pour la soutenance.

    Usage :
      visualiser_ruptures('Residentiel', 23, df_residentiel, resultats)
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("matplotlib non installé : pip install matplotlib")
        return

    agg = preparer_serie(df)
    sub = (agg[agg['gouvernorat'] == gov]
           .sort_values(['annee','mois']).reset_index(drop=True))

    if len(sub) < MIN_SIGNAL:
        print(f"Trop peu de données pour gov={gov}")
        return

    prix  = sub['prix_lisse'].values
    dates = pd.to_datetime(
        sub['annee'].astype(str) + '-' + sub['mois'].astype(str).str.zfill(2) + '-01')

    bps = resultats.get(groupe, {}).get('ruptures', {}).get(gov, {}).get('breakpoints', [])

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dates, prix, color='steelblue', linewidth=2, label='Prix lissé')
    ax.fill_between(dates, prix, alpha=0.1, color='steelblue')

    for bp in bps:
        if bp < len(dates):
            ax.axvline(x=dates.iloc[bp], color='red', linestyle='--',
                       linewidth=2, alpha=0.8)
            ax.annotate(f'Rupture\n{dates.iloc[bp].strftime("%m/%Y")}',
                        xy=(dates.iloc[bp], prix[bp]),
                        xytext=(10, 20), textcoords='offset points',
                        color='red', fontsize=9,
                        arrowprops=dict(arrowstyle='->', color='red'))

    ax.set_title(f'PELT — {groupe} | Gouvernorat {gov} | {len(bps)} rupture(s)',
                 fontsize=13, fontweight='bold')
    ax.set_xlabel('Date')
    ax.set_ylabel('Prix m² (TND)')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%Y'))
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()

    if save_dir:
        path = os.path.join(save_dir, f'pelt_{groupe}_gov{gov}.png')
        plt.savefig(path, dpi=150, bbox_inches='tight')
        print(f"Graphique sauvegardé : {path}")
    else:
        plt.show()


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
        resultats = run_pelt(groupes)
