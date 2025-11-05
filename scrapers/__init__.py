"""
Module scrapers régionaux
"""
import importlib
from typing import Optional

AVAILABLE_REGIONS = {
    "auvergne-rhone-alpes": "auvergne_rhone_alpes",
    "bourgogne-franche-comte": "bourgogne_franche_comte",
    "normandie": "normandie",
    "grand-est": "grand_est", 
    "hauts-de-france": "hauts_de_france",
    "ile-de-france": "ile_de_france",
    "bretagne": "bretagne",
}


def get_scraper(region: str):
    """
    Charge dynamiquement le scraper pour une région
    
    Args:
        region: Slug de la région (ex: 'auvergne-rhone-alpes')
    
    Returns:
        Module du scraper régional
    
    Raises:
        ImportError: Si le scraper n'existe pas
    """
    region_slug = region.lower().replace(" ", "-").replace("'", "-")
    
    if region_slug not in AVAILABLE_REGIONS:
        raise ImportError(
            f"Région '{region}' non supportée. "
            f"Régions disponibles: {list(AVAILABLE_REGIONS.keys())}"
        )
    
    module_name = AVAILABLE_REGIONS[region_slug]
    module = importlib.import_module(f"scrapers.{module_name}")
    
    return module
