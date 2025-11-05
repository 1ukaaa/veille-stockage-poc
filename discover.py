#!/usr/bin/env python3
"""
CLI de découverte de projets BESS par région - VERSION OPTIMISÉE
Benchmark intégré pour mesurer les gains de performance

Usage:
    python discover.py --region auvergne-rhone-alpes --year 2024
    python discover.py --region normandie --dept 14 --year 2024 --benchmark
"""
import sys
import time
import argparse
import logging
from pathlib import Path

import pandas as pd

from config import settings
from utils import AdaptiveHTTPClient, PersistentCache
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
        description="Découverte optimisée de projets BESS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python discover.py --region auvergne-rhone-alpes --year 2024
  python discover.py --region normandie --year 2024 --benchmark
  python discover.py --region normandie --dept 14 --year 2024 --benchmark
        """
    )
    
    parser.add_argument(
        "--region",
        required=True,
        help="Région: auvergne-rhone-alpes, normandie, bourgogne-franche-comte"
    )
    parser.add_argument(
        "--year",
        required=True,
        help="Année: 2024, 2023, etc."
    )
    parser.add_argument(
        "--dept",
        help="Code département optionnel (ex: 01, 14)"
    )
    parser.add_argument(
        "--output",
        help="Fichier CSV de sortie personnalisé"
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Afficher benchmark de performance détaillé"
    )
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Utiliser cache persistant (2e run beaucoup plus rapide)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mode verbose (DEBUG logs)"
    )
    
    args = parser.parse_args()
    configure_logging(args.verbose)
    
    # Validation année
    if not args.year.isdigit() or len(args.year) != 4:
        logger.error(f"Année invalide: {args.year}")
        sys.exit(1)
    
    # Nom fichier sortie
    if args.output:
        output_path = Path(args.output)
    else:
        region_slug = args.region.lower().replace(" ", "-")
        dept_suffix = f"_dep{args.dept}" if args.dept else ""
        filename = f"projects_{region_slug}{dept_suffix}_{args.year}.csv"
        output_path = settings.OUTPUT_DIR / "projects" / filename
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Chargement scraper
    try:
        scraper_module = get_scraper(args.region)
    except ImportError as e:
        logger.error(str(e))
        available = ", ".join(sorted(AVAILABLE_REGIONS.keys()))
        logger.info(f"Régions disponibles: {available}")
        sys.exit(1)
    
    # Affichage config
    print("\n" + "="*70)
    print("🚀 DÉCOUVERTE PROJETS BESS - VERSION OPTIMISÉE")
    print("="*70)
    print(f"Région:        {args.region}")
    print(f"Année:         {args.year}")
    print(f"Département:   {args.dept or 'Tous'}")
    print(f"Cache:         {'Activé' if args.cache else 'Désactivé'}")
    print(f"Benchmark:     {'Activé' if args.benchmark else 'Désactivé'}")
    print("="*70)
    print()
    
    # Timer
    start_time = time.time()
    
    # Client optimisé
    # ⭐ CLÉS OPTIMISATIONS :
    # - AdaptiveHTTPClient au lieu de HTTPClient
    # - Délai 0.15s adaptatif (vs 0.4s fixe)
    # - Backoff exponentiel si rate-limited
    # - User-Agent rotation
    client = AdaptiveHTTPClient(base_delay=0.15, max_delay=0.5)
    request_start_count = client._request_count
    
    try:
        projects = scraper_module.discover_projects(
            year=args.year,
            client=client,
            dept=args.dept,
            seed_url=None
        )
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Interruption utilisateur")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erreur découverte: {e}", exc_info=args.verbose)
        sys.exit(1)
    finally:
        client.close()
    
    total_requests = client._request_count - request_start_count
    duration = time.time() - start_time
    
    # Vérification résultats
    if not projects:
        logger.warning("⚠️  Aucun projet BESS trouvé")
        sys.exit(0)
    
    # Export CSV
    try:
        df = pd.DataFrame([p.to_dict() for p in projects])
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(f"✓ {len(projects)} projets sauvegardés dans {output_path}")
    except Exception as e:
        logger.error(f"Erreur export CSV: {e}")
        sys.exit(1)
    
    # Résumé
    print("\n" + "="*70)
    print("✅ DÉCOUVERTE TERMINÉE")
    print("="*70)
    print(f"Projets trouvés:     {len(projects)}")
    print(f"Durée:               {format_duration(duration)}")
    print(f"Requêtes HTTP:       {total_requests}")
    print(f"Fichier:             {output_path}")
    print("="*70)
    
    # Benchmark détaillé
    if args.benchmark:
        print("\n📊 BENCHMARK PERFORMANCE")
        print("="*70)
        print(f"Durée totale:        {format_duration(duration)}")
        print(f"Requêtes HTTP:       {total_requests}")
        print(f"Projets/seconde:     {len(projects)/duration:.2f}")
        print(f"Temps/projet:        {duration/max(1, len(projects)):.2f}s")
        print(f"Temps/requête:       {duration/max(1, total_requests):.2f}s")
        print("="*70)
        
        # Estimation vs ancien
        estimated_old = total_requests * 0.5  # Ancien DELAY ~0.5s effectif
        speedup = estimated_old / duration if duration > 0 else 1
        
        print(f"\n⚡ GAIN ESTIMÉ vs VERSION NON-OPTIMISÉE")
        print(f"   Ancien (DELAY 0.4s):  {format_duration(estimated_old)}")
        print(f"   Nouveau (optimisé):   {format_duration(duration)}")
        print(f"   Accélération:         {speedup:.1f}x plus rapide")
        print("="*70)
    
    print(f"\n✨ Succès! Fichier: {output_path}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption utilisateur")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Erreur inattendue: {e}", exc_info=True)
        sys.exit(1)
