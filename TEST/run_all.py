#!/usr/bin/env python3
"""
Runner de scraping pour TOUTES les DREAL (Sortie CSV unique)

Version 3.1 - Architecture "Bibliothèque" (Corrigée)

- N'UTILISE PLUS SUBPROCESS.
- Importe la logique directement depuis 'scrape_side_lib.py'.
- Appelle discover_projects() en boucle.
- Agrège tous les résultats dans une seule liste.
- Écrit un unique fichier CSV à la fin.
- Correction bug 'year' vs 'args.year' dans le logger.
"""

import argparse
import logging
from pathlib import Path
from typing import List

# Importe la logique et les modèles de données depuis notre bibliothèque
try:
    from scrape_side_lib import discover_projects, _write_to_csv, Project
except ImportError:
    print("ERREUR FATALE: Le fichier 'scrape_side_lib.py' est introuvable.")
    print("Veuillez vous assurer que 'scrape_side_lib.py' est dans le même dossier que ce script.")
    exit(1)


# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration des régions (V2.1 - Labels de l'image)
REGIONS_CONFIG = [
    {"label": "Occitanie", "slug": "occitanie"},
    {"label": "Nouvelle Aquitaine", "slug": "nouvelle_aquitaine"},
    {"label": "Auvergne-Rhône-Alpes", "slug": "ara"},
    {"label": "Hauts de France", "slug": "hdf"},
    {"label": "Pays-de-Loire", "slug": "pays_de_loire"},
    {"label": "Bourgogne-Franche-Compté", "slug": "bfc"},
    {"label": "Normandie", "slug": "normandie"},
    {"label": "Grand Est", "slug": "grand_est"},
    {"label": "Centre-Val de Loire", "slug": "centre_val_de_loire"},
    {"label": "BRETAGNE", "slug": "bretagne"}
]

def main():
    parser = argparse.ArgumentParser(
        description="Lance le scraping BESS pour TOUTES les régions sur une année donnée."
    )
    parser.add_argument(
        "-y", "--year", 
        type=str, 
        required=True, 
        help="L'année cible (YYYY). Ex: '2023'"
    )
    args = parser.parse_args()

    logger.info(f"Démarrage du scraping de masse (sortie unique) pour l'année {args.year}...")

    # La liste qui contiendra TOUS les résultats
    all_projects: List[Project] = []

    for config in REGIONS_CONFIG:
        label = config["label"]
        slug = config["slug"]
        
        # === CORRECTION APPLIQUÉE ICI ===
        logger.info("\n%s\nLancement pour : %s (%s)\n%s", "="*50, label, args.year, "="*50)
        
        try:
            # Appel direct de la fonction importée
            projects_for_region = discover_projects(
                year=args.year,
                region_label=label,
                region_slug=slug
            )
            
            # Ajout des résultats à la liste principale
            all_projects.extend(projects_for_region)
            logger.info(f"Terminé pour {label}: {len(projects_for_region)} projets trouvés.")

        except Exception as e:
            logger.error(
                f"ERREUR FATALE (non-subprocess) pour {label}: {e}", 
                exc_info=True
            )

    logger.info("\n" + "="*50 + "\nScraping de masse terminé." + "\n" + "="*50)

    # Définition du fichier de sortie unique
    output_file = Path(f"projets_bess_TOUTES_REGIONS_{args.year}.csv")

    # Appel de la fonction d'écriture CSV importée, une seule fois
    _write_to_csv(all_projects, output_file)
    
    logger.info(f"TOTAL: {len(all_projects)} projets agrégés.")


if __name__ == "__main__":
    main()