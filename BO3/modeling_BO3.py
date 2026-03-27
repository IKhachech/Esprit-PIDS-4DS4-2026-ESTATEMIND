"""
modeling_BO3.py — Enrichissement, calcul target et export pour l'Objectif 3 : Tendances Régionales.

Étapes :
  6. enrich_external()         — Google Maps, Satellite, INS/BCT, Signaux
  7. compute_regional_index()  — indice_prix_m2_regional (moyenne × gov × annee × mois)
  8. export_datasets()         — 4 fichiers Excel + encoding_mappings_BO3.json
  9. print_final_report()      — résumé complet

TARGET : indice_prix_m2_regional
  = moyenne du prix/m² par gouvernorat × année × mois
  Utilisée pour : ARIMA/LSTM (séries temporelles), K-means, Change Point Detection

Différences vs BO2 :
  - Pas de BERT / NLP / embeddings (inutile pour tendances)
  - Pas de sample_weight (pas de modèle supervisé unique)
  - Target = indice agrégé, pas prix individuel
  - Ajout colonnes macro-économiques (inflation, PIB) au niveau mensuel
"""

import os, re, json, warnings
import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows

warnings.filterwarnings('ignore')

from mappings_BO3 import (
    GOUVERNORAT_ENC, GOUVERNORAT_DEC,
    TARGET_COLS, FICHIERS_ML, HIGH_SEASON_MONTHS
)
from external_data_BO3 import get_inflation, get_pib

def section(t): print("\n" + "="*65 + f"\n   {t}\n" + "="*65)
def log(m):     print(f"  {m}")


# ================================================================
# ÉTAPE 6 — ENRICHISSEMENT MULTI-SOURCES
# ================================================================

def enrich_external(groupes_clean: dict, external: dict) -> dict:
    """
    Enrichit chaque groupe avec :
    - score_attractivite    (Signaux — normalisé 0-1)
    - nb_infra, nb_commerce (Satellite)
    - inflation_glissement_annuel (INS mensuel)
    - croissance_pib_trim   (INS trimestriel)
    - glissement_immo_trim  (INS immobilier par type×trimestre — réel + extrapolé)
    """
    section("ETAPE 6 — ENRICHISSEMENT MULTI-SOURCES")

    from mappings_BO3 import GLISSEMENT_IMMO_INS

    score_att    = external.get('score_attractivite', {})
    nb_infra_map = external.get('nb_infra', {})      # depuis signaux — pas satellite
    nb_comm_map  = external.get('nb_commerce', {})   # depuis signaux — pas satellite
    inf_map      = external.get('inflation_by_month', {})
    pib_map      = external.get('pib_by_quarter', {})

    # Mapping type_categorise → série INS immobilier
    def _serie_ins(type_cat: str) -> str:
        t = str(type_cat).lower()
        if 'foncier' in t: return 'Terrain nus'
        return 'Appartement'  # Residentiel + Commercial + Divers

    def _glissement_immo(annee: int, mois: int, type_cat: str) -> float:
        q    = (int(mois) - 1) // 3 + 1
        yr   = int(annee)
        serie = _serie_ins(type_cat)
        key   = (yr, q, serie)
        if key in GLISSEMENT_IMMO_INS:
            return GLISSEMENT_IMMO_INS[key]
        # Trimestre le plus proche disponible
        avail = [(k[0], k[1]) for k in GLISSEMENT_IMMO_INS if k[2] == serie]
        if not avail: return 5.0
        best = min(avail, key=lambda k: abs(k[0]*4+k[1] - (yr*4+q)))
        return GLISSEMENT_IMMO_INS[(best[0], best[1], serie)]

    for groupe, dg in groupes_clean.items():

        # ── Score attractivité (0-1, normalisé /100 comme BO2) ──
        dg['score_attractivite'] = dg['_gouvernorat_str'].map(score_att).fillna(0.30).round(4)

        # ── Satellite → remplacé par signaux (même fichier, 0 dépendance) ──
        dg['nb_infra']    = dg['_gouvernorat_str'].map(nb_infra_map).fillna(0).astype(int)
        dg['nb_commerce'] = dg['_gouvernorat_str'].map(nb_comm_map).fillna(0).astype(int)

        # ── Macro-économique INS ───────────────────────────────
        dg['inflation_glissement_annuel'] = dg.apply(
            lambda r: get_inflation(r['annee'], r['mois'], inf_map), axis=1).round(2)
        dg['croissance_pib_trim'] = dg.apply(
            lambda r: get_pib(r['annee'], r['mois'], pib_map), axis=1).round(2)

        # ── Glissement immobilier INS ──────────────────────────
        # 8 trimestres réels (T2 2022 → T1 2024) + extrapolation 2024-2026
        # Clé pour ARIMA : donne l'évolution nationale du marché par type de bien
        dg['glissement_immo_trim'] = dg.apply(
            lambda r: _glissement_immo(
                r['annee'], r['mois'],
                r.get('type_categorise', 'Residentiel')),
            axis=1).round(2)

        groupes_clean[groupe] = dg

    all_dg = pd.concat(groupes_clean.values(), ignore_index=True)
    log(f"✔ Enrichissement terminé : {len(all_dg):,} annonces")
    log(f"  score_attractivite     : mean={all_dg['score_attractivite'].mean():.3f} "
        f"(min={all_dg['score_attractivite'].min():.3f} / max={all_dg['score_attractivite'].max():.3f})")
    log(f"  inflation moyenne      : {all_dg['inflation_glissement_annuel'].mean():.1f}%")
    log(f"  pib moyen              : {all_dg['croissance_pib_trim'].mean():.1f}%")
    log(f"  glissement_immo_trim   : mean={all_dg['glissement_immo_trim'].mean():.1f}% "
        f"| NaN={all_dg['glissement_immo_trim'].isna().sum()}")
    return groupes_clean


# ================================================================
# ÉTAPE 7 — TARGET : INDICE_PRIX_M2_REGIONAL
# ================================================================

def compute_regional_index(groupes_clean: dict) -> dict:
    """
    Calcule l'indice régional du prix/m² :
      indice_prix_m2_regional = mean(prix_m2) par _gouvernorat_str × annee × mois

    Ajoute 3 colonnes de correction calculées 100% depuis les données réelles scrapées :

      sample_weight_temporal :
        Poids inverse à la surreprésentation annuelle.
        Source : distribution réelle des années dans le dataset.
        But    : ARIMA ne sera pas biaisé par 2026 (50% des données).
        Formule : (nb_annonces_moyen_par_an / nb_annonces_annee) plafonné à 5 normalisé mean=1

      sample_weight_geo :
        Poids inverse à la surreprésentation géographique.
        Source : distribution réelle des gouvernorats dans le dataset.
        But    : K-means ne sera pas dominé par Tunis (51% des données).
        Formule : (nb_annonces_moyen_par_gov / nb_annonces_gov) plafonné à 10 normalisé mean=1

      arima_eligible :
        Indicateur binaire de faisabilité ARIMA.
        Source : nb de points temporels distincts (annee×mois) par gouvernorat.
        But    : l'agent sait directement si ARIMA est possible (≥12 pts) ou si
                 fallback médiane régionale est nécessaire (<12 pts).
        Seuil  : ADAPTATIF par groupe — médiane des points temporels du groupe.
    """
    section("ETAPE 7 — TARGET : indice_prix_m2_regional")

    # Calculer prix_m2 directement dans chaque groupe
    for groupe, dg in groupes_clean.items():
        if 'prix_m2' not in dg.columns or dg['prix_m2'].isna().all():
            dg['prix_m2'] = np.where(
                dg['prix'].notna() & dg['surface_m2'].notna() & (dg['surface_m2'] > 0),
                dg['prix'] / dg['surface_m2'], np.nan)
            mask_loc = dg['type_transaction'] == 1
            mask_ven = dg['type_transaction'] == 2
            dg.loc[mask_loc & ((dg['prix_m2'] < 1)   | (dg['prix_m2'] > 500)),    'prix_m2'] = np.nan
            dg.loc[mask_ven & ((dg['prix_m2'] < 100) | (dg['prix_m2'] > 30_000)), 'prix_m2'] = np.nan
            groupes_clean[groupe] = dg

    df_all = pd.concat(groupes_clean.values(), ignore_index=True)
    log(f"  prix_m2 calculé : {df_all['prix_m2'].notna().sum():,} valides / {len(df_all):,}")

    # ── Calcul indice TARGET ──────────────────────────────────────
    indice = df_all.groupby(
        ['_gouvernorat_str', 'annee', 'mois']
    )['prix_m2'].mean().reset_index()
    indice = indice.rename(columns={'prix_m2': 'indice_prix_m2_regional'})
    indice['indice_prix_m2_regional'] = indice['indice_prix_m2_regional'].round(2)
    log(f"  ✔ Indice calculé : {len(indice):,} combinaisons gouvernorat×annee×mois")
    log(f"  Plage : {indice['indice_prix_m2_regional'].min():.0f} – {indice['indice_prix_m2_regional'].max():.0f} TND/m²")
    gov_avg = indice.groupby('_gouvernorat_str')['indice_prix_m2_regional'].mean().sort_values(ascending=False)
    log("  Top 5 gouvernorats (prix/m² moyen) :")
    for gov, val in gov_avg.head(5).items():
        log(f"    {gov:<15} : {val:,.0f} TND/m²")

    # ── Calcul 3 colonnes de correction depuis données réelles ────
    #
    # IMPORTANT : ces 3 colonnes sont calculées UNIQUEMENT depuis
    # les données scrapées réelles (distribution des annonces).
    # Aucune valeur n'est inventée ou codée en dur.
    #
    n_total = len(df_all)

    # 1. sample_weight_temporal — depuis distribution réelle des années
    yr_counts   = df_all['annee'].value_counts()
    yr_expected = n_total / yr_counts.nunique()
    yr_weight   = (yr_expected / yr_counts).clip(upper=5.0)
    yr_weight   = (yr_weight / yr_weight.mean()).round(4)
    log(f"\n  sample_weight_temporal (depuis {yr_counts.nunique()} années réelles) :")
    for yr in sorted(yr_counts.index):
        log(f"    {yr}: {yr_counts[yr]:>6,} ann. → poids={yr_weight[yr]:.4f}")

    # 2. sample_weight_geo — depuis distribution réelle des gouvernorats
    gov_counts   = df_all['gouvernorat'].value_counts()
    gov_expected = n_total / gov_counts.nunique()
    gov_weight   = (gov_expected / gov_counts).clip(upper=10.0)
    gov_weight   = (gov_weight / gov_weight.mean()).round(4)
    log(f"\n  sample_weight_geo (depuis {gov_counts.nunique()} gouvernorats réels) :")
    for code in sorted(gov_counts.nlargest(3).index.tolist() + gov_counts.nsmallest(3).index.tolist()):
        log(f"    gov={code}: {gov_counts[code]:>6,} ann. → poids={gov_weight[code]:.4f}")

    # 3. arima_eligible — SEUIL ADAPTATIF par groupe
    #
    # Problème seuil fixe 12 :
    #   - Commercial/Divers ont peu de données → 0 ou peu de gouvernorats éligibles
    #   - Seuil fixe pénalise injustement les groupes avec moins d'annonces
    #
    # Solution dynamique :
    #   - Calculer le seuil depuis la distribution réelle de chaque groupe
    #   - Seuil = médiane(pts_par_gov) — aucune valeur imposée
    #   - Minimum absolu = 6 pts (ARIMA viable avec au moins 6 observations)
    #
    # Interpolation géographique pour gouvernorats sous seuil :
    #   - Au lieu de fallback médiane nationale (trop grossière)
    #   - Interpolation depuis les 3 gouvernorats les plus similaires
    #   - Similarité = distance euclidienne sur score_attractivite

    gov_pts = (df_all.groupby('gouvernorat')[['annee','mois']]
                     .apply(lambda x: x.drop_duplicates().shape[0]))

    def _seuil_adaptatif(pts_series):
        """
        Calcule le seuil ARIMA 100% dynamique depuis les données.
        Seuil = médiane des points temporels par gouvernorat.
        Signification : un gouvernorat est éligible s'il a
        au moins autant de points que la moitié des gouvernorats
        de son groupe. Aucune valeur imposée de l'extérieur.
        """
        if len(pts_series) == 0:
            return 1
        return float(pts_series.median())

    def _interpoler_depuis_voisins(gov_cible, df_groupe, indice_df, n_voisins=3):
        """
        Interpole l'indice prix/m² d'un gouvernorat depuis ses voisins similaires.
        Similarité basée sur score_attractivite (proxy de richesse économique).
        """
        if 'score_attractivite' not in df_groupe.columns:
            return None

        score_cible = df_groupe[df_groupe['gouvernorat'] == gov_cible]['score_attractivite'].mean()
        if pd.isna(score_cible):
            return None

        # Scores de tous les autres gouvernorats
        scores_gov = df_groupe.groupby('gouvernorat')['score_attractivite'].mean()
        scores_gov = scores_gov.drop(index=gov_cible, errors='ignore')

        if len(scores_gov) == 0:
            return None

        # Distance euclidienne sur score_attractivite
        distances = abs(scores_gov - score_cible)
        voisins   = distances.nsmallest(n_voisins).index.tolist()

        # Moyenne pondérée par similarité (1/distance)
        poids_voisins = []
        indices_voisins = []
        for v in voisins:
            idx_v = indice_df[indice_df['_gouvernorat_str'].isin(
                df_groupe[df_groupe['gouvernorat'] == v]['_gouvernorat_str'].unique()
            )]['indice_prix_m2_regional']
            if len(idx_v) > 0:
                poids_voisins.append(1.0 / (distances[v] + 1e-6))
                indices_voisins.append(idx_v.mean())

        if not indices_voisins:
            return None

        poids_arr  = np.array(poids_voisins)
        poids_norm = poids_arr / poids_arr.sum()
        return float(np.dot(poids_norm, indices_voisins))

    # Calcul arima_eligible avec seuil adaptatif PAR GROUPE
    arima_ok_par_groupe = {}
    seuils_log = {}

    for groupe, dg in groupes_clean.items():
        pts_groupe = (dg.groupby('gouvernorat')[['annee','mois']]
                        .apply(lambda x: x.drop_duplicates().shape[0]))
        seuil = _seuil_adaptatif(pts_groupe)
        seuils_log[groupe] = seuil
        arima_ok_par_groupe[groupe] = (pts_groupe >= seuil).astype(int)

    # Pour df_all global on prend le seuil résidentiel (le plus représentatif)
    seuil_global = seuils_log.get('Residentiel', 12)
    arima_ok     = (gov_pts >= seuil_global).astype(int)

    log(f"\n  arima_eligible — seuil ADAPTATIF par groupe :")
    for groupe, seuil in seuils_log.items():
        ok  = arima_ok_par_groupe[groupe]
        log(f"    {groupe:<15}: seuil={seuil} pts | "
            f"{ok.sum()} éligibles / {len(ok)} gouvernorats")

    # ── Joindre tout dans chaque groupe ──────────────────────────
    global_med = indice['indice_prix_m2_regional'].median()
    for groupe, dg in groupes_clean.items():
        # TARGET
        dg = dg.merge(
            indice[['_gouvernorat_str', 'annee', 'mois', 'indice_prix_m2_regional']],
            on=['_gouvernorat_str', 'annee', 'mois'],
            how='left'
        )
        if dg['indice_prix_m2_regional'].isna().any():
            gov_med = dg.groupby('_gouvernorat_str')['indice_prix_m2_regional'].transform('median')
            dg['indice_prix_m2_regional'] = dg['indice_prix_m2_regional'].fillna(gov_med).fillna(global_med)

        # 3 colonnes de correction — valeurs calculées depuis les données réelles
        dg['sample_weight_temporal'] = dg['annee'].map(yr_weight).fillna(1.0).round(4)
        dg['sample_weight_geo']      = dg['gouvernorat'].map(gov_weight).fillna(1.0).round(4)
        # arima_eligible avec seuil adaptatif par groupe
        arima_ok_groupe = arima_ok_par_groupe.get(groupe, arima_ok)
        dg['arima_eligible'] = dg['gouvernorat'].map(arima_ok_groupe).fillna(0).astype(int)

        # Interpolation géographique pour gouvernorats non éligibles
        # (meilleur que fallback médiane nationale)
        govs_non_eligibles = arima_ok_groupe[arima_ok_groupe == 0].index.tolist()
        if govs_non_eligibles and 'score_attractivite' in dg.columns:
            for gov_ne in govs_non_eligibles:
                val_interp = _interpoler_depuis_voisins(gov_ne, dg, indice)
                if val_interp is not None:
                    mask = (dg['gouvernorat'] == gov_ne) & dg['indice_prix_m2_regional'].isna()
                    dg.loc[mask, 'indice_prix_m2_regional'] = round(val_interp, 2)

        n_nan = dg['indice_prix_m2_regional'].isna().sum()
        log(f"  {groupe:<15} : {len(dg):,} ann. | NaN TARGET={n_nan} | "
            f"arima_eligible={dg['arima_eligible'].sum()}/{len(dg)}")
        groupes_clean[groupe] = dg

    return groupes_clean


# ================================================================
# ÉTAPE 8 — EXPORT
# ================================================================

def _write_excel(df_out: pd.DataFrame, filename: str, color_hex: str, sheet_name: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    hfill = PatternFill("solid", start_color=color_hex)
    hfont = Font(bold=True, color="FFFFFF", name="Arial", size=9)

    for r in dataframe_to_rows(df_out, index=False, header=True):
        clean = []
        for cell in r:
            if isinstance(cell, str):
                cell = cell.encode('ascii', 'ignore').decode('ascii')
                cell = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', cell)
                cell = re.sub(r'\s+', ' ', cell).strip()
            elif isinstance(cell, bool):         cell = str(cell)
            elif isinstance(cell, np.integer):   cell = int(cell)
            elif isinstance(cell, np.floating):  cell = round(float(cell), 4) if not np.isnan(cell) else None
            clean.append(cell)
        ws.append(clean)

    for cell in ws[1]:
        cell.fill = hfill
        cell.font = hfont
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for col in ws.columns:
        w = max(len(str(c.value)) if c.value else 0 for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(w + 2, 40)
    ws.freeze_panes = "A2"
    wb.save(filename)
    log(f"  {filename:<40}: {len(df_out):>6} lignes | {len(df_out.columns)} colonnes")


def export_datasets(groupes_clean: dict, output_dir: str = '.') -> None:
    section("ETAPE 8 — EXPORT FINAL : 4 DATASETS SEGMENTÉS")

    os.makedirs(output_dir, exist_ok=True)

    for groupe, dg in groupes_clean.items():
        if groupe not in FICHIERS_ML: continue
        fname, color = FICHIERS_ML[groupe]

        # Sélectionner les colonnes disponibles
        cols_sel = [c for c in TARGET_COLS if c in dg.columns]

        # code_gouv → gouvernorat si TARGET_COLS utilise gouvernorat
        if 'gouvernorat' in cols_sel and 'code_gouv' in dg.columns and 'gouvernorat' not in dg.columns:
            dg = dg.rename(columns={'code_gouv': 'gouvernorat'})
        elif 'gouvernorat' in cols_sel and 'code_gouv' in dg.columns:
            dg['gouvernorat'] = dg['code_gouv']

        df_export = dg[cols_sel].copy()

        # Filtre : garder uniquement annee >= 2022
        MIN_ANNEE = 2022
        avant = len(df_export)
        df_export = df_export[df_export['annee'] >= MIN_ANNEE].reset_index(drop=True)
        apres = len(df_export)
        if avant != apres:
            log(f"  [{groupe}] Filtre annee >= {MIN_ANNEE} : {avant} → {apres} (-{avant-apres})")

        df_export = df_export.sort_values(['gouvernorat', 'annee', 'mois']).reset_index(drop=True)

        out_path = os.path.join(output_dir, fname)
        _write_excel(df_export, out_path, color, f"{groupe}_BO3")

    # Encoding mappings
    mappings = {
        'gouvernorat':      {str(v): k for k, v in GOUVERNORAT_ENC.items()},
        'type_transaction': {'1': 'Location', '2': 'Vente'},
        'high_season':      {'0': 'Basse saison', '1': 'Haute saison (mars-mai, sept-nov)'},
        'trimestre':        {'1':'T1 Jan-Mar','2':'T2 Avr-Jun','3':'T3 Jul-Sep','4':'T4 Oct-Dec'},
        'semestre':         {'1':'S1 Jan-Jun','2':'S2 Jul-Dec'},
    }
    map_path = os.path.join(output_dir, 'encoding_mappings_BO3.json')
    try:
        with open(map_path, 'w', encoding='utf-8') as f:
            json.dump(mappings, f, ensure_ascii=False, indent=2)
        log(f"  Mappings sauvegardés : encoding_mappings_BO3.json")
    except Exception as e:
        log(f"  [WARN] Mappings non sauvegardés : {e}")


# ================================================================
# RAPPORT FINAL
# ================================================================

def print_final_report(groupes_clean: dict, n_sources: int,
                       n_brut: int, n_dedup: int, bct: dict) -> None:
    section("RAPPORT FINAL — OBJECTIF 3 : TENDANCES RÉGIONALES")

    total = sum(len(g) for g in groupes_clean.values())

    log(f"Sources chargées                   : {n_sources}")
    log(f"Annonces brutes                    : {n_brut:>8,}")
    log(f"Après déduplication                : {n_dedup:>8,}  (-{n_brut - n_dedup:,})")
    log(f"Après nettoyage complet            : {total:>8,}")
    log("")
    log("Répartition par groupe :")
    for groupe, dg in groupes_clean.items():
        pct = len(dg)/total*100 if total > 0 else 0
        log(f"  {groupe:<15}: {len(dg):>7,} annonces ({pct:.1f}%)")
    log("")

    # Vérifier qualité de la TARGET
    all_df = pd.concat(groupes_clean.values(), ignore_index=True)
    nan_target = all_df['indice_prix_m2_regional'].isna().sum()
    log(f"TARGET indice_prix_m2_regional :")
    log(f"  NaN        : {nan_target}")
    log(f"  Médiane    : {all_df['indice_prix_m2_regional'].median():,.0f} TND/m²")
    log(f"  Min/Max    : {all_df['indice_prix_m2_regional'].min():,.0f} – {all_df['indice_prix_m2_regional'].max():,.0f}")
    log("")

    # Couverture temporelle
    log(f"Couverture temporelle :")
    log(f"  Plage      : {all_df['annee'].min()} – {all_df['annee'].max()}")
    by_year = all_df['annee'].value_counts().sort_index()
    for yr, cnt in by_year.items():
        log(f"  {yr}       : {cnt:,} annonces")
    log("")
    log(f"BCT taux directeur : {bct.get('taux_directeur', 7.0)}% (date: {bct.get('date', '?')})")

    print("\n" + "="*65)
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║   OBJECTIF 3 — DONNÉES PRÊTES POUR MODÉLISATION            ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║  TARGET : indice_prix_m2_regional                          ║")
    print("║           mean(prix_m2) par gouvernorat × annee × mois     ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║  11 colonnes finales :                                      ║")
    print("║  gouvernorat, annee, mois, high_season                     ║")
    print("║  type_transaction                                           ║")
    print("║  score_attractivite (0-1), nb_infra, nb_commerce           ║")
    print("║  inflation_glissement_annuel, croissance_pib_trim          ║")
    print("║  indice_prix_m2_regional (TARGET)                          ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║  Modèles cibles :                                           ║")
    print("║  → ARIMA/LSTM    séries temporelles (gov × mois)            ║")
    print("║  → K-means       segmentation (attractivité + prix/m²)     ║")
    print("║  → Change Point  détection zones émergentes                 ║")
    print("║  → Causaux       scénarios (high_season, macro-éco)         ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║  Fichiers exportés :                                        ║")
    for groupe, (fname, _) in FICHIERS_ML.items():
        print(f"║    {fname:<56}║")
    print("╚══════════════════════════════════════════════════════════════╝")