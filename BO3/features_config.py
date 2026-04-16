"""
features_config.py — Feature Registry centralisé · Version Production L99+
===========================================================================

PRINCIPE : UN SEUL dataset propre (les Excel BO3) → chaque modèle choisit ses features.
           Ce fichier est LE contrat ML du pipeline. Toute modification de features
           se fait ICI, pas dans les fichiers modèles.

ARCHITECTURE :
  pipeline_BO3.py  →  df_clean (Excel BO3 : 22 colonnes)
                            │
                     features_config.py   ← CE FICHIER (contrat ML)
                            │
             ┌──────────────┼──────────────┬─────────────┐
         kmeans_A.py    pelt_A.py      lstm_A.py    tft_dataset_builder.py
         (clustering)   (ruptures)     (prédiction) (dataset TFT structuré)

POURQUOI C'EST MIEUX :
  ✔ Zéro duplication de listes de features entre fichiers
  ✔ Modification en un seul endroit → cohérence garantie
  ✔ Défendable académiquement : "Model-specific feature projection from a unified dataset"
  ✔ Pas de datasets dérivés — un seul df_clean, sélection par modèle

COLUMNS DISPONIBLES DANS df_clean (TARGET_COLS — 22 colonnes) :
  Géographie  : gouvernorat, ville_encoded, zone_geographique
  Temporel    : annee, mois, high_season
  Transaction : type_transaction
  Marché      : score_attractivite, nb_infra, nb_commerce,
                indice_liquidite, volatilite_prix_trim, potentiel_emergent
  Macro       : inflation_glissement_annuel, croissance_pib_trim, glissement_immo_trim
  Poids       : sample_weight_temporal, sample_weight_geo, arima_eligible
  Target      : indice_prix_m2_regional, indice_loc_lisse, indice_ven_lisse
"""

# ================================================================
# FEATURE SETS PAR MODÈLE
# ================================================================

FEATURE_SETS = {

    # ── KMeans : segmentation géographique ──────────────────────────────────
    # Objectif : regrouper les gouvernorats par comportement de marché.
    # → Features statiques (caractéristiques intrinsèques de chaque gouvernorat)
    # → NE PAS inclure les features temporelles (annee, mois) : on veut le profil moyen
    "kmeans": [
        # Géographie
        "gouvernorat",
        "zone_geographique",          # ancre spatiale (littoral vs intérieur)
        # Prix (agrégés par gouvernorat dans kmeans_A.build_features())
        "indice_prix_m2_regional",    # niveau de prix (agrégé → prix_mean)
        "indice_loc_lisse",           # prix location lissé
        "indice_ven_lisse",           # prix vente lissé
        # Marché local
        "score_attractivite",         # potentiel d'attractivité 0-1
        "nb_infra",                   # dotation en infrastructure
        "nb_commerce",                # activité commerciale
        "indice_liquidite",           # liquidité du marché
        "volatilite_prix_trim",       # stabilité des prix
        "potentiel_emergent",         # zones à surveiller
        # Composition du marché
        "type_transaction",           # ratio loc/ven (agrégé dans build_features())
        # Macro (signal de contexte)
        "inflation_glissement_annuel",
        "glissement_immo_trim",
    ],

    # ── PELT : détection des ruptures de régime ──────────────────────────────
    # Objectif : détecter les changements structurels du marché dans le temps.
    # → Features temporelles obligatoires (annee, mois)
    # → Signal prix + composition + liquidité (influence le seuil PELT)
    "pelt": [
        # Identification
        "gouvernorat",
        "annee",
        "mois",
        # Signal prix (SOURCE du signal de rupture)
        "indice_prix_m2_regional",    # signal global (séquence temporelle)
        "indice_loc_lisse",           # sous-signal location (cohérence OR 50%)
        "indice_ven_lisse",           # sous-signal vente (cohérence OR 50%)
        # Contexte marché (influence pénalité PELT)
        "type_transaction",           # composition loc/ven
        "indice_liquidite",           # marchés illiquides → pénalité ×2.5
        "volatilite_prix_trim",       # marchés volatils → ruptures attendues
        "zone_geographique",          # ancre spatiale L102 (dim 6 du signal 7D)
        # Macro (signal de rupture macroéconomique)
        "glissement_immo_trim",
        "inflation_glissement_annuel",
        "croissance_pib_trim",
        "high_season",
        # Poids (fiabilité de la série)
        "sample_weight_temporal",
    ],

    # ── LSTM : prédiction des tendances ──────────────────────────────────────
    # Objectif : prévoir indice_prix_m2_regional sur HORIZON=3 mois.
    # → Toutes les features temporelles + pipeline (cluster_id, ruptures PELT)
    # → Les features pipeline (cluster_id, post_rupture, etc.) sont ajoutées
    #   dans lstm_A.enrichir_dataframe() depuis cluster_map + pelt_rapport.
    # → NE PAS inclure ville_encoded (trop granulaire pour une série temporelle)
    "lstm": [
        # Géographie + clustering (pipeline kmeans)
        "gouvernorat",
        # Signal prix stratifié (CORRECTION type_transaction)
        "indice_prix_m2_regional",    # TARGET
        "indice_loc_lisse",           # → renommé indice_prix_m2_loc dans lstm_A
        "indice_ven_lisse",           # → renommé indice_prix_m2_ven dans lstm_A
        # Composition marché (mensuel)
        "type_transaction",           # → ratio_location_vente dans lstm_A
        # Temporel
        "annee",
        "mois",
        "high_season",                # saisonnalité immobilière
        # Macro-économique
        "glissement_immo_trim",
        "inflation_glissement_annuel",
        "croissance_pib_trim",
        # Marché local
        "score_attractivite",
        "nb_infra",
        "nb_commerce",
        # Poids (sample_weight_temporal utilisé dans le fit())
        "sample_weight_temporal",
        # NOTE : cluster_id, post_rupture, score_rupture_norm,
        #        rupture_intensity, zscore_prix sont ajoutés par
        #        lstm_A.enrichir_dataframe() depuis cluster_map + pelt_rapport.
        # Ils ne sont PAS dans TARGET_COLS car calculés en post-pipeline.
    ],

    # ── TFT : dataset structuré multi-séries ────────────────────────────────
    # Objectif : préparer le dataset pour PyTorch Forecasting TimeSeriesDataSet.
    # → Structure 4 groupes de features (static / known / unknown / target)
    # → rupture_flag calculé depuis pelt_A_rapport.csv (pas dans TARGET_COLS)
    # → cluster_size calculé depuis cluster_map.pkl (pas dans TARGET_COLS)
    "tft": {
        # TARGET (ce qu'on prédit)
        "target": "indice_prix_m2_regional",

        # Identifiants de série (obligatoires pour TimeSeriesDataSet)
        "group_ids": ["gouvernorat", "groupe"],

        # 1. Static features (constantes par série)
        "static_cat": [
            "gouvernorat",            # ID entité (catégoriel)
            "zone_geographique",      # zone 0-4 (catégoriel)
            "groupe",                 # Residentiel/Foncier/Commercial
        ],
        "static_real": [
            # cluster et cluster_size viennent de cluster_map.pkl (post-kmeans)
            # ils sont ajoutés par tft_dataset_builder.py
            "score_attractivite",     # profil attractivité (stable)
            "potentiel_emergent",     # zone émergente (quasi-statique)
        ],

        # 2. Time-varying KNOWN (connues à l'avance)
        "known_real": [
            "annee",                  # tendance long terme
            "mois",                   # saisonnalité
            "high_season",            # haute saison immobilière
            # mois_sin, mois_cos, saison, annee_norm ajoutés par tft_dataset_builder
            "inflation_glissement_annuel",  # prévision INS disponible
            "croissance_pib_trim",    # prévision PIB disponible
        ],

        # 3. Time-varying OBSERVED (pas connues à l'avance)
        "unknown_real": [
            "indice_prix_m2_regional",  # TARGET
            "indice_loc_lisse",        # sous-signal location
            "indice_ven_lisse",        # sous-signal vente
            "indice_liquidite",        # liquidité du marché
            "volatilite_prix_trim",    # instabilité
            "glissement_immo_trim",    # glissement prix immo
            "nb_infra",                # dotation infra
            "nb_commerce",             # activité commerciale
            # rupture_flag, score_rupture, post_rupture ajoutés par tft_dataset_builder
            # ratio_loc_ven, nb_annonces_proxy, volatilite_rolling idem
        ],
    },
}

# ================================================================
# FEATURE METADATA (documentation et validation)
# ================================================================

FEATURE_METADATA = {
    # Nom → {source, dtype, range, description, models}
    "gouvernorat":                 {"source": "pipeline", "dtype": "int",   "range": "1-24",  "models": ["kmeans","pelt","lstm","tft"]},
    "ville_encoded":               {"source": "pipeline", "dtype": "int",   "range": "1000+", "models": []},  # non utilisé dans les modèles
    "zone_geographique":           {"source": "modeling", "dtype": "int",   "range": "0-4",   "models": ["kmeans","pelt","tft"]},
    "annee":                       {"source": "pipeline", "dtype": "int",   "range": "2020+", "models": ["pelt","lstm","tft"]},
    "mois":                        {"source": "pipeline", "dtype": "int",   "range": "1-12",  "models": ["pelt","lstm","tft"]},
    "high_season":                 {"source": "pipeline", "dtype": "int",   "range": "0-1",   "models": ["lstm","tft"]},
    "type_transaction":            {"source": "pipeline", "dtype": "int",   "range": "1-2",   "models": ["kmeans","pelt","lstm"]},
    "score_attractivite":          {"source": "signaux",  "dtype": "float", "range": "0-1",   "models": ["kmeans","lstm","tft"]},
    "nb_infra":                    {"source": "signaux",  "dtype": "int",   "range": "0+",    "models": ["kmeans","lstm","tft"]},
    "nb_commerce":                 {"source": "signaux",  "dtype": "int",   "range": "0+",    "models": ["kmeans","lstm","tft"]},
    "indice_liquidite":            {"source": "modeling", "dtype": "float", "range": "0+",    "models": ["kmeans","pelt"]},
    "volatilite_prix_trim":        {"source": "modeling", "dtype": "float", "range": "0+",    "models": ["kmeans","pelt","tft"]},
    "potentiel_emergent":          {"source": "modeling", "dtype": "float", "range": "0-1",   "models": ["kmeans","tft"]},
    "inflation_glissement_annuel": {"source": "ins",      "dtype": "float", "range": "0-20",  "models": ["kmeans","pelt","lstm","tft"]},
    "croissance_pib_trim":         {"source": "ins",      "dtype": "float", "range": "-5-10", "models": ["lstm","tft"]},
    "glissement_immo_trim":        {"source": "ins",      "dtype": "float", "range": "0-20",  "models": ["pelt","lstm","tft"]},
    "sample_weight_temporal":      {"source": "modeling", "dtype": "float", "range": "0-5",   "models": ["lstm"]},
    "sample_weight_geo":           {"source": "modeling", "dtype": "float", "range": "0-10",  "models": []},  # kmeans_A utilise RobustScaler, pas ce poids
    "arima_eligible":              {"source": "modeling", "dtype": "int",   "range": "0-1",   "models": []},  # info pipeline, pas feature modèle
    "indice_prix_m2_regional":     {"source": "modeling", "dtype": "float", "range": "100+",  "models": ["kmeans","pelt","lstm","tft"], "is_target": True},
    "indice_loc_lisse":            {"source": "modeling", "dtype": "float", "range": "100+",  "models": ["kmeans","pelt","lstm","tft"]},
    "indice_ven_lisse":            {"source": "modeling", "dtype": "float", "range": "100+",  "models": ["kmeans","pelt","lstm","tft"]},
}

# ================================================================
# FEATURES CALCULÉES POST-PIPELINE (pas dans TARGET_COLS)
# ================================================================
# Ces features sont calculées APRÈS le pipeline BO3, dans les modèles.
# Elles ne font PAS partie du dataset Excel → générées à la volée.

POST_PIPELINE_FEATURES = {
    # Ajoutées par kmeans_A.py → cluster_map.pkl
    "cluster":              "kmeans_A.run_kmeans()    → cluster_map.pkl",
    "cluster_size":         "kmeans_A.run_kmeans()    → cluster_map.pkl  [NOUVEAU]",
    "zscore_prix":          "kmeans_A.build_features() → cluster_map.pkl",
    "ratio_location_vente": "kmeans_A.build_features() → cluster_map.pkl",
    "prix_mean_loc":        "kmeans_A.build_features() → cluster_map.pkl",
    "prix_mean_ven":        "kmeans_A.build_features() → cluster_map.pkl",

    # Ajoutées par pelt_A.py → pelt_A_rapport.csv
    "rupture_flag":         "pelt_A.run_pelt()        → pelt_A_rapport.csv  [NOUVEAU]",
    "score_norm":           "pelt_A.run_pelt()        → pelt_A_rapport.csv",
    "fiabilite_signal":     "pelt_A.run_pelt()        → pelt_A_rapport.csv",
    "zone_geographique_pelt":"pelt_A.run_pelt()       → pelt_A_rapport.csv (L102)",
    "indice_liquidite_pelt":"pelt_A.run_pelt()        → pelt_A_rapport.csv (L102)",

    # Ajoutées par lstm_A.enrichir_dataframe()
    "cluster_id":           "lstm_A.enrichir_dataframe() depuis cluster_map",
    "post_rupture":         "lstm_A.enrichir_dataframe() depuis pelt_rapport",
    "score_rupture_norm":   "lstm_A.enrichir_dataframe() depuis pelt_rapport",
    "rupture_intensity":    "lstm_A.enrichir_dataframe() depuis pelt_rapport",

    # Ajoutées par tft_dataset_builder.py
    "mois_sin":             "tft_dataset_builder._agreger_mensuel()",
    "mois_cos":             "tft_dataset_builder._agreger_mensuel()",
    "saison":               "tft_dataset_builder._agreger_mensuel()",
    "annee_norm":           "tft_dataset_builder._agreger_mensuel()",
    "nb_annonces_proxy":    "tft_dataset_builder._agreger_mensuel()",
    "volatilite_rolling":   "tft_dataset_builder._agreger_mensuel()",
    "ratio_loc_ven":        "tft_dataset_builder._agreger_mensuel()",
}

# ================================================================
# HELPERS
# ================================================================

def get_features(model: str, df_columns: list = None) -> list:
    """
    Retourne la liste de features pour un modèle donné.

    Parameters
    ----------
    model      : "kmeans" | "pelt" | "lstm" | "tft"
    df_columns : si fourni, filtre les features disponibles dans df

    Returns
    -------
    list de colonnes (ou dict pour "tft")
    """
    if model not in FEATURE_SETS:
        raise ValueError(f"Modèle '{model}' inconnu. Choix : {list(FEATURE_SETS.keys())}")

    feats = FEATURE_SETS[model]

    # Pour TFT, le format est un dict (pas une liste)
    if isinstance(feats, dict):
        if df_columns is None:
            return feats
        cols = set(df_columns)
        filtered = {}
        for k, v in feats.items():
            if isinstance(v, list):
                filtered[k] = [f for f in v if f in cols]
            else:
                filtered[k] = v
        return filtered

    # Pour les autres modèles, liste simple
    if df_columns is None:
        return feats
    return [f for f in feats if f in df_columns]


def validate_features(df, model: str, strict: bool = False) -> tuple:
    """
    Vérifie que les features requises par un modèle sont présentes dans df.
    Affiche un rapport détaillé : présentes / manquantes / post-pipeline.

    Returns
    -------
    (present: list, missing_real: list)
    Si strict=True et des features manquent (hors post-pipeline), lève une ValueError.
    """
    feats = FEATURE_SETS[model]
    if isinstance(feats, dict):
        all_feats = []
        for v in feats.values():
            if isinstance(v, list):
                all_feats.extend(v)
            elif isinstance(v, str) and v in df.columns:
                all_feats.append(v)
    else:
        all_feats = list(feats)

    # Séparer features post-pipeline (normalement absentes du df_clean)
    post_pipeline = set(POST_PIPELINE_FEATURES.keys())
    feats_source     = [f for f in all_feats if f not in post_pipeline]
    feats_post       = [f for f in all_feats if f in post_pipeline]

    present      = [f for f in feats_source if f in df.columns]
    missing_real = [f for f in feats_source if f not in df.columns]

    # Rapport
    ok = len(missing_real) == 0
    status = "✔" if ok else "✗"
    print(f"  [{model.upper()}] {status} {len(present)}/{len(feats_source)} features présentes")
    if missing_real:
        print(f"    MANQUANTES (df_clean) : {missing_real}")
    if feats_post:
        print(f"    Post-pipeline (calculées par les modèles, pas dans df_clean) : {feats_post}")

    if missing_real and strict:
        raise ValueError(f"[{model}] Features manquantes : {missing_real}")

    return present, missing_real


def print_feature_registry() -> None:
    """Affiche le registre complet des features par modèle."""
    print("\n" + "=" * 72)
    print("  FEATURE REGISTRY — PIPELINE BO3 (features_config.py)")
    print("=" * 72)
    print(f"  Dataset source  : TARGET_COLS ({sum(1 for _ in FEATURE_METADATA)} colonnes)")
    print(f"  Features post-pipeline : {len(POST_PIPELINE_FEATURES)} (cluster_map + pelt_rapport)")
    print()
    for model, feats in FEATURE_SETS.items():
        if isinstance(feats, dict):
            n = sum(len(v) for v in feats.values() if isinstance(v, list))
            print(f"  [{model.upper():<8}] {n} features (structuré TFT)")
            for group, cols in feats.items():
                if isinstance(cols, list):
                    print(f"    {group:<16}: {cols}")
                else:
                    print(f"    {group:<16}: '{cols}'")
        else:
            print(f"  [{model.upper():<8}] {len(feats)} features : {feats}")
        print()
    print("  Post-pipeline (calculées dans les modèles, pas dans df_clean) :")
    for feat, origine in POST_PIPELINE_FEATURES.items():
        print(f"    {feat:<30} ← {origine}")
    print("=" * 72)


# ── Point d'entrée : affiche le registre ──────────────────────────────────────
if __name__ == '__main__':
    print_feature_registry()