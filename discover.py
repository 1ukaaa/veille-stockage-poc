#!/usr/bin/env python3
"""
CLI de découverte de projets BESS par région
Version optimisée avec benchmark et statistiques

Usage: 
    python discover.py --region auvergne-rhone-alpes --year 2024
    python discover.py --region auvergne-rhone-alpes --dept 01 --year 2024 --benchmark
"""
import sys
import time
import argparse
import logging
from pathlib import Path

import pandas as pd

from config import settings
from utils import HTTPClient
from scrapers import AVAILABLE_REGIONS, get_scraper

logger = logging.getLogger(__name__)


def format_duration(seconds: float) -> str:
    """Formate une durée en format lisible"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}min {secs:.0f}s"


def configure_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Découverte de projets BESS sur les sites DREAL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  # Tous les départements d'une région
  python discover.py --region auvergne-rhone-alpes --year 2024
  
  # Département spécifique
  python discover.py --region auvergne-rhone-alpes --dept 01 --year 2024
  
  # Avec benchmark de performance
  python discover.py --region auvergne-rhone-alpes --dept 01 --year 2024 --benchmark
  
  # Output personnalisé
  python discover.py --region bourgogne-franche-comte --year 2024 --output mes_projets.csv
        """
    )
    
    parser.add_argument(
        "--region",
        required=True,
        help="Région à scraper (ex: auvergne-rhone-alpes, bourgogne-franche-comte)"
    )
    parser.add_argument(
        "--year",
        required=True,
        help="Année à rechercher (ex: 2024, 2023)"
    )
    parser.add_argument(
        "--dept",
        help="Code département optionnel (ex: 01, 38). Si omis, tous les départements"
    )
    parser.add_argument(
        "--output",
        help="Fichier CSV de sortie (défaut: out/projects/region_year.csv)"
    )
    parser.add_argument(
        "--seed-url",
        help="URL de départ personnalisée (optionnel, ignoré pour version optimisée)"
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Afficher statistiques de performance détaillées"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mode verbose (logs DEBUG)"
    )
    
    args = parser.parse_args()
    
    configure_logging(args.verbose)
    
    # Validation année
    if not args.year.isdigit() or len(args.year) != 4:
        logger.error(f"Année invalide: {args.year} (format attendu: YYYY)")
        sys.exit(1)
    
    year_int = int(args.year)
    if year_int < 2020 or year_int > 2030:
        logger.warning(f"Année inhabituelle: {args.year}")
    
    # Génération nom fichier sortie
    if args.output:
        output_path = Path(args.output)
    else:
        region_slug = args.region.lower().replace(" ", "-").replace("'", "-")
        dept_suffix = f"_dep{args.dept}" if args.dept else ""
        filename = f"projects_{region_slug}{dept_suffix}_{args.year}.csv"
        output_path = settings.OUTPUT_DIR / "projects" / filename
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Chargement du scraper régional
    try:
        scraper_module = get_scraper(args.region)
    except ImportError as e:
        logger.error(str(e))
        available = ", ".join(sorted(AVAILABLE_REGIONS.keys()))
        logger.info(f"Régions disponibles: {available}")
        sys.exit(1)
    
    # Affichage configuration
    print("\n" + "="*70)
    print("DÉCOUVERTE PROJETS BESS")
    print("="*70)
    print(f"Région:        {args.region}")
    print(f"Année:         {args.year}")
    print(f"Département:   {args.dept if args.dept else 'Tous'}")
    print(f"Output:        {output_path}")
    print(f"Benchmark:     {'Activé' if args.benchmark else 'Désactivé'}")
    print("="*70)
    print()
    
    # Démarrage timer
    start_time = time.time()
    request_start_count = 0
    
    # Découverte projets
    with HTTPClient() as client:
        if args.benchmark:
            request_start_count = client._request_count
        
        try:
            projects = scraper_module.discover_projects(
                year=args.year,
                client=client,
                dept=args.dept,
                seed_url=args.seed_url
            )
        except KeyboardInterrupt:
            logger.warning("\nInterruption utilisateur (Ctrl+C)")
            sys.exit(130)
        except Exception as e:
            logger.error(f"Erreur lors de la découverte: {e}", exc_info=args.verbose)
            sys.exit(1)
        
        # Statistiques HTTP
        total_requests = client._request_count - request_start_count
    
    # Durée totale
    duration = time.time() - start_time
    
    # Vérification résultats
    if not projects:
        logger.warning("⚠️  Aucun projet BESS trouvé")
        print("\nPossibles raisons:")
        print("  - Aucun projet pour cette année/département")
        print("  - Site DREAL temporairement indisponible")
        print("  - Structure du site modifiée")
        sys.exit(0)
    
    # Export CSV
    try:
        df = pd.DataFrame([p.to_dict() for p in projects])
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(f"✓ {len(projects)} projets sauvegardés dans {output_path}")
    except Exception as e:
        logger.error(f"Erreur lors de l'export CSV: {e}")
        sys.exit(1)
    
    # Affichage résumé
    print("\n" + "="*70)
    print("RÉSUMÉ DÉCOUVERTE")
    print("="*70)
    print(f"Région:          {args.region}")
    print(f"Année:           {args.year}")
    print(f"Département:     {args.dept or 'Tous'}")
    print(f"Projets BESS:    {len(projects)}")
    print(f"Durée:           {format_duration(duration)}")
    print(f"Fichier:         {output_path}")
    print("="*70)
    
    # Benchmark détaillé
    if args.benchmark:
        print("\n" + "="*70)
        print("⏱️  BENCHMARK PERFORMANCE")
        print("="*70)
        print(f"Durée totale:          {format_duration(duration)}")
        print(f"Requêtes HTTP:         {total_requests}")
        print(f"Projets trouvés:       {len(projects)}")
        print(f"Projets/seconde:       {len(projects)/duration:.2f}")
        print(f"Temps moyen/projet:    {duration/max(1, len(projects)):.2f}s")
        print(f"Temps moyen/requête:   {duration/max(1, total_requests):.2f}s")
        print("="*70)
        
        # Estimation gain vs version non-optimisée
        estimated_old_duration = total_requests * 1.0  # Ancien délai 1s
        speedup = estimated_old_duration / duration if duration > 0 else 1
        
        print("\n📊 ESTIMATION vs VERSION NON-OPTIMISÉE")
        print(f"   Durée estimée (ancien):  {format_duration(estimated_old_duration)}")
        print(f"   Durée réelle (optimisé): {format_duration(duration)}")
        print(f"   Accélération:            {speedup:.1f}x plus rapide")
    
    # Aperçu projets découverts
    if len(projects) > 0:
        print("\n" + "="*70)
        print("APERÇU PROJETS DÉCOUVERTS")
        print("="*70)
        
        # Grouper par département
        depts = {}
        for project in projects:
            dept = project.dept
            if dept not in depts:
                depts[dept] = []
            depts[dept].append(project)
        
        # Afficher premiers projets
        shown = 0
        max_show = 5
        
        for dept in sorted(depts.keys()):
            dept_projects = depts[dept]
            print(f"\nDépartement {dept} ({len(dept_projects)} projets):")
            
            for project in dept_projects[:2]:  # Max 2 par département
                if shown >= max_show:
                    break
                
                print(f"\n  • {project.project_title[:70]}")
                if len(project.project_title) > 70:
                    print(f"    {project.project_title[70:][:70]}")
                print(f"    URL: {project.project_url}")
                shown += 1
            
            if shown >= max_show:
                remaining = len(projects) - shown
                if remaining > 0:
                    print(f"\n  ... et {remaining} autres projets")
                break
        
        print("="*70)
    
    # Message succès final
    print(f"\n✅ Découverte terminée avec succès en {format_duration(duration)}")
    print(f"   Fichier disponible: {output_path}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption utilisateur")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}", exc_info=True)
        sys.exit(1)
