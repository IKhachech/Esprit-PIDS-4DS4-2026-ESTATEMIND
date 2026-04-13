"""
confidence.py — Calcul fourchette de prix et niveau de confiance
"""
import math


def compute_interval(prix: float, r2: float,
                     type_transaction: int = 2,
                     type_bien: int = 1,
                     gouvernorat: int = 23,
                     cycle_marche: int = 2) -> dict:
    """
    Calcule fourchette [min, max] et niveau de confiance.
    Marge basée sur R² et type de bien.
    """
    # Marge de base selon R²
    if r2 >= 0.80:
        marge = 0.12
        confiance = "élevée"
    elif r2 >= 0.65:
        marge = 0.20
        confiance = "moyenne"
    elif r2 >= 0.50:
        marge = 0.28
        confiance = "faible"
    else:
        marge = 0.35
        confiance = "faible"

    # Ajustement type de bien
    if type_bien in [5, 8]:    # terrain/ferme → plus incertain
        marge *= 1.3
    elif type_bien == 9:       # villa → plus incertain
        marge *= 1.15

    # Ajustement cycle marché
    if cycle_marche == 3:      # peak → fourchette plus large
        marge *= 1.1

    fourchette_min = round(prix * (1 - marge))
    fourchette_max = round(prix * (1 + marge))

    return {
        "fourchette_min": max(fourchette_min, 1),
        "fourchette_max": fourchette_max,
        "confiance":      confiance,
        "marge":          round(marge, 3),
    }
