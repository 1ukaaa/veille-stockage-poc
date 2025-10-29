#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
ENRICHISSEMENT PERMIS DE CONSTRUIRE - VERSION PRODUCTION v2.0
═══════════════════════════════════════════════════════════════════════════
Architecture: Multi-search pattern avec agrégation intelligente
Optimisé pour: Gemini Tier 1 (1000 RPM, 1M TPM)
Performance: ~2-3 projets/seconde en mode parallèle

Usage:
    python enrich.py --input file.csv --parallel --benchmark
    python enrich.py --input file.csv --limit 5 --debug
"""
import sys
import json
import time
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import argparse
import logging

import pandas as pd
from google import genai
from google.genai import types

from config import settings


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION GLOBALE
# ═══════════════════════════════════════════════════════════════════════════

# Logging optimisé
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# Suppression logs verbeux
for log_name in ["google.generativeai", "google.auth", "httpx"]:
    logging.getLogger(log_name).setLevel(logging.ERROR)

# Performance tuning
class Config:
    MAX_WORKERS = 4
    RATE_LIMIT_DELAY = 1.2
    REQUEST_TIMEOUT = 25
    MAX_RETRIES = 2
    SEARCH_DELAY = 0.4
    NUM_SEARCHES = 4
    
    # Gemini models config
    SEARCH_OUTPUT_TOKENS = 1024
    JSON_OUTPUT_TOKENS = 1536
    SEARCH_TEMPERATURE = 0.1
    JSON_TEMPERATURE = 0.0


# Mapping départements (cache statique)
DEPT_SLUGS = {
    "01": "ain", "03": "allier", "07": "ardeche", "15": "cantal",
    "26": "drome", "38": "isere", "42": "loire", "43": "haute-loire",
    "63": "puy-de-dome", "69": "rhone", "73": "savoie", "74": "haute-savoie",
    "14": "calvados", "27": "eure", "50": "manche", "61": "orne", "76": "seine-maritime",
}

REGION_SLUGS = {
    "auvergne-rhone-alpes": "auvergne-rhone-alpes",
    "normandie": "normandie",
    "nouvelle-aquitaine": "nouvelle-aquitaine",
}


# ═══════════════════════════════════════════════════════════════════════════
# PROMPTS TEMPLATES (OPTIMISÉS)
# ═══════════════════════════════════════════════════════════════════════════

class Prompts:
    """Templates de prompts optimisés"""
    
    SEARCH_PARTICIPATION = """Cherche permis construire: {title}, {commune} ({dept})
Sources: Participation publique
1. site:{dept_slug}.gouv.fr "participation du public" "{commune}" batterie
2. site:{dept_slug}.gouv.fr "consultation publique" "{commune}"

Format: XXX YYY ZZ RNNNN (ex: 063 204 24 R0004)
Réponds: Permis [numéro] URL [lien] Confiance [0-1] OU Non trouvé"""

    SEARCH_RAA = """Cherche permis: {title}, {commune} ({dept})
Sources: RAA 2024
1. site:{dept_slug}.gouv.fr RAA 2024 "{commune}" filetype:pdf
2. site:{dept_slug}.gouv.fr "recueil actes administratifs"

Format: XXX YYY ZZ RNNNN
Réponds: Permis [numéro] PDF [lien] Confiance [0-1] OU Non trouvé"""

    SEARCH_CERFA = """Cherche permis: {title}, {commune} ({dept})
Sources: CERFA/MRAE
1. site:{dept_slug}.gouv.fr CERFA "{commune}" filetype:pdf
2. site:mrae.developpement-durable.gouv.fr "{commune}"

Format: XXX YYY ZZ RNNNN
Réponds: Permis [numéro] URL [lien] Confiance [0-1] OU Non trouvé"""

    SEARCH_ARRETE = """Cherche permis: {title}, {commune} ({dept})
Sources: Arrêtés
1. "{commune}" {dept} "arrêté préfectoral" stockage
2. "{applicant}" permis construire {dept}

Format: XXX YYY ZZ RNNNN
Réponds: Permis [numéro] URL [lien] Confiance [0-1] OU Non trouvé"""

    AGGREGATION = """Analyse recherches permis {title}, {commune} ({dept}):

R1 (Participation): {r1}
R2 (RAA): {r2}
R3 (CERFA): {r3}
R4 (Arrêtés): {r4}

Extrais meilleure info (confiance max). Format XXX YYY ZZ RNNNN.
Priorité: Participation > RAA > CERFA > Arrêtés

JSON strict (pas de markdown):
{{"permit_number":"XXX YYY ZZ RNNNN ou null","permit_type":"PC ou DP ou null","issue_date":"YYYY-MM-DD ou null","applicant":"nom ou null","source_url":"URL ou null","source_type":"participation_publique ou RAA ou CERFA ou arrete ou null","confidence":0.0,"search_summary":"Source retenue"}}

Si rien: tout null, confidence 0.0"""


# ═══════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SearchResult:
    """Résultat d'une recherche ciblée"""
    text: str
    urls: List[str] = field(default_factory=list)
    success: bool = False
    duration: float = 0.0


@dataclass
class PermitData:
    """Données structurées du permis"""
    permit_number: Optional[str] = None
    permit_type: Optional[str] = None
    issue_date: Optional[str] = None
    applicant: Optional[str] = None
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    confidence: float = 0.0
    search_summary: str = ""
    grounding_urls: List[str] = field(default_factory=list)
    api_calls: int = 0
    duration: float = 0.0
    
    def to_csv_dict(self, project: Dict) -> Dict:
        """Conversion pour export CSV"""
        return {
            **project,
            "permit_number": self.permit_number,
            "permit_type": self.permit_type,
            "permit_date": self.issue_date,
            "permit_applicant": self.applicant,
            "permit_source": self.source_url,
            "permit_source_type": self.source_type,
            "permit_confidence": self.confidence,
            "permit_search_summary": self.search_summary,
            "permit_grounding_urls": "|".join(self.grounding_urls[:5]),
            "permit_validated": "no"
        }


@dataclass
class EnricherStats:
    """Statistiques globales"""
    api_calls: int = 0
    cache_hits: int = 0
    errors: int = 0
    search_calls: int = 0
    aggregation_calls: int = 0
    total_duration: float = 0.0
    
    def increment(self, key: str, value: int = 1):
        if hasattr(self, key):
            setattr(self, key, getattr(self, key) + value)


# ═══════════════════════════════════════════════════════════════════════════
# UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════

class JSONCleaner:
    """Nettoyage JSON robuste"""
    
    @staticmethod
    def clean(text: str) -> str:
        """Nettoie le texte JSON"""
        # Suppression markdown
        text = re.sub(r'```\s*', '', text)
        text = text.strip('`').strip()
        return text
    
    @staticmethod
    def parse(text: str) -> Dict:
        """Parse JSON avec fallback"""
        try:
            clean_text = JSONCleaner.clean(text)
            return json.loads(clean_text)
        except json.JSONDecodeError:
            # Tentative extraction via regex
            match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except:
                    pass
            raise ValueError(f"JSON parsing failed: {text[:100]}")


@lru_cache(maxsize=128)
def get_dept_slug(dept_code: str) -> str:
    """Cache département slug"""
    return DEPT_SLUGS.get(dept_code, f"dept{dept_code}")


@lru_cache(maxsize=32)
def get_region_slug(region: str) -> str:
    """Cache région slug"""
    return REGION_SLUGS.get(region.lower(), region.lower().replace(" ", "-"))


def extract_grounding_urls(response) -> List[str]:
    """Extraction URLs depuis grounding metadata"""
    urls = []
    try:
        if hasattr(response, 'grounding_metadata'):
            meta = response.grounding_metadata
            if hasattr(meta, 'grounding_chunks'):
                for chunk in meta.grounding_chunks[:10]:
                    if hasattr(chunk, 'web') and chunk.web and chunk.web.uri:
                        urls.append(str(chunk.web.uri))
    except Exception as e:
        logger.debug(f"Grounding extraction error: {e}")
    return urls


# ═══════════════════════════════════════════════════════════════════════════
# ENRICHISSEUR PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

class PermitEnricher:
    """Enrichisseur haute performance avec pattern multi-search"""
    
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("GOOGLE_API_KEY manquante dans .env")
        
        self.client = genai.Client(api_key=api_key)
        self.stats = EnricherStats()
        self._cache: Dict[str, PermitData] = {}
        
        # Configurations Gemini
        self.config_search = types.GenerateContentConfig(
            temperature=Config.SEARCH_TEMPERATURE,
            max_output_tokens=Config.SEARCH_OUTPUT_TOKENS,
            tools=[types.Tool(google_search=types.GoogleSearch())]
        )
        
        self.config_json = types.GenerateContentConfig(
            temperature=Config.JSON_TEMPERATURE,
            max_output_tokens=Config.JSON_OUTPUT_TOKENS,
            response_mime_type="application/json"
        )
    
    def _cache_key(self, project: Dict) -> str:
        """Génère clé cache unique"""
        return f"{project.get('commune', '')}_{project.get('project_title', '')[:40]}"
    
    def _execute_search(self, prompt: str, name: str) -> SearchResult:
        """Exécute une recherche ciblée avec validation"""
        start = time.time()
        
        try:
            response = self.client.models.generate_content(
                model=settings.GEMINI_MODEL,
                contents=prompt,
                config=self.config_search
            )
            
            self.stats.increment('api_calls')
            self.stats.increment('search_calls')
            
            # Validation
            if not hasattr(response, 'text'):
                logger.debug(f"  ✗ {name}: no text attribute")
                return SearchResult(text="Non trouvé", success=False, duration=time.time()-start)
            
            text = str(response.text).strip()
            
            if len(text) < 10:
                logger.debug(f"  ✗ {name}: response too short ({len(text)} chars)")
                return SearchResult(text="Non trouvé", success=False, duration=time.time()-start)
            
            # Extraction URLs
            urls = extract_grounding_urls(response)
            
            # Détection succès
            success = any(kw in text.lower() for kw in ['permis', 'pc', 'trouvé']) or len(urls) > 0
            
            logger.debug(f"  ✓ {name}: {len(text)} chars, {len(urls)} URLs, success={success}")
            
            return SearchResult(
                text=text,
                urls=urls,
                success=success,
                duration=time.time() - start
            )
        
        except Exception as e:
            logger.debug(f"  ✗ {name} error: {e}")
            self.stats.increment('errors')
            return SearchResult(
                text=f"Erreur: {str(e)}",
                success=False,
                duration=time.time() - start
            )
    
    def _aggregate_results(self, title: str, commune: str, dept: str, 
                          results: List[SearchResult]) -> PermitData:
        """Agrège les résultats via Gemini JSON"""
        
        try:
            # Préparation prompt
            prompt = Prompts.AGGREGATION.format(
                title=title,
                commune=commune,
                dept=dept,
                r1=results[0].text[:700],
                r2=results[1].text[:700],
                r3=results[2].text[:700],
                r4=results[3].text[:700]
            )
            
            # Appel Gemini
            response = self.client.models.generate_content(
                model=settings.GEMINI_MODEL,
                contents=prompt,
                config=self.config_json
            )
            
            self.stats.increment('api_calls')
            self.stats.increment('aggregation_calls')
            
            # Parsing JSON
            if not hasattr(response, 'text') or not response.text:
                raise ValueError("Empty JSON response")
            
            data = JSONCleaner.parse(str(response.text))
            
            # Collecte URLs
            all_urls = []
            for r in results:
                all_urls.extend(r.urls)
            
            # Construction résultat
            return PermitData(
                permit_number=data.get('permit_number'),
                permit_type=data.get('permit_type'),
                issue_date=data.get('issue_date'),
                applicant=data.get('applicant'),
                source_url=data.get('source_url'),
                source_type=data.get('source_type'),
                confidence=float(data.get('confidence', 0.0)),
                search_summary=str(data.get('search_summary', '')),
                grounding_urls=all_urls[:10],
                api_calls=Config.NUM_SEARCHES + 1
            )
        
        except Exception as e:
            logger.error(f"  ✗ Aggregation error: {e}")
            self.stats.increment('errors')
            return PermitData(
                search_summary=f"Erreur agrégation: {str(e)}",
                api_calls=Config.NUM_SEARCHES + 1
            )
    
    def search_permit(self, project: Dict, use_cache: bool = True) -> PermitData:
        """Point d'entrée principal: recherche multi-sources + agrégation"""
        
        start_time = time.time()
        
        # Cache check
        cache_key = self._cache_key(project)
        if use_cache and cache_key in self._cache:
            self.stats.increment('cache_hits')
            logger.info(f"  ⚡ Cache: {project.get('project_title', '')[:40]}")
            return self._cache[cache_key]
        
        # Extraction données projet
        title = project.get("project_title", "")
        commune = project.get("commune", "inconnu")
        dept = project.get("dept", "")
        region = project.get("region", "")
        applicant = project.get("demandeur", "inconnu")
        
        dept_slug = get_dept_slug(dept)
        region_slug = get_region_slug(region)
        
        # Variables template
        vars_dict = {
            "title": title,
            "commune": commune,
            "dept": dept,
            "applicant": applicant,
            "dept_slug": dept_slug,
            "region_slug": region_slug
        }
        
        # Retry loop
        for attempt in range(Config.MAX_RETRIES):
            try:
                logger.info(f"🔍 [{attempt+1}/{Config.MAX_RETRIES}] {title[:45]}...")
                
                # ════════════════════════════════════════════════════════
                # PHASE 1: 4 RECHERCHES CIBLÉES (séquentiel stable)
                # ════════════════════════════════════════════════════════
                
                searches = [
                    (Prompts.SEARCH_PARTICIPATION, "Participation"),
                    (Prompts.SEARCH_RAA, "RAA"),
                    (Prompts.SEARCH_CERFA, "CERFA"),
                    (Prompts.SEARCH_ARRETE, "Arrêté")
                ]
                
                results = []
                for prompt_template, name in searches:
                    prompt = prompt_template.format(**vars_dict)
                    result = self._execute_search(prompt, name)
                    results.append(result)
                    time.sleep(Config.SEARCH_DELAY)
                
                # Log succès
                success_count = sum(1 for r in results if r.success)
                logger.debug(f"  Recherches: {success_count}/{len(results)} succès")
                
                # ════════════════════════════════════════════════════════
                # PHASE 2: AGRÉGATION JSON
                # ════════════════════════════════════════════════════════
                
                permit_data = self._aggregate_results(title, commune, dept, results)
                permit_data.duration = time.time() - start_time
                
                # Cache
                if use_cache:
                    self._cache[cache_key] = permit_data
                
                # Log final
                status = "✅" if permit_data.confidence > 0.5 else "⚠️"
                logger.info(
                    f"  {status} {permit_data.confidence:.2f} - "
                    f"{permit_data.permit_number or 'N/A'} "
                    f"({permit_data.source_type or 'N/A'})"
                )
                
                return permit_data
            
            except Exception as e:
                logger.error(f"  ✗ Tentative {attempt+1} échouée: {e}")
                if attempt < Config.MAX_RETRIES - 1:
                    time.sleep(3)
                    continue
                
                self.stats.increment('errors')
                return PermitData(
                    search_summary=f"Erreur après {Config.MAX_RETRIES} tentatives: {str(e)}",
                    duration=time.time() - start_time
                )
    
    def get_stats(self) -> Dict:
        """Retourne stats complètes"""
        return {
            **asdict(self.stats),
            "cache_size": len(self._cache)
        }


# ═══════════════════════════════════════════════════════════════════════════
# WORKER PARALLÈLE
# ═══════════════════════════════════════════════════════════════════════════

def process_project_worker(args: Tuple) -> Dict:
    """Worker optimisé pour ThreadPoolExecutor"""
    enricher, project, delay = args
    
    try:
        result = enricher.search_permit(project, use_cache=True)
        time.sleep(delay)
        return result.to_csv_dict(project)
    
    except Exception as e:
        logger.error(f"Worker error: {e}")
        return {
            **project,
            "permit_number": None,
            "permit_confidence": 0.0,
            "permit_search_summary": f"Worker error: {e}",
            "permit_validated": "no"
        }


# ═══════════════════════════════════════════════════════════════════════════
# CLI MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="🎯 Enrichissement Permis v2.0 - PRODUCTION OPTIMISÉE",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python enrich.py --input file.csv --parallel --benchmark
  python enrich.py --input file.csv --limit 5 --debug
  python enrich.py --input file.csv --workers 2 --delay 1.5
        """
    )
    
    parser.add_argument("--input", required=True, help="Fichier CSV d'entrée")
    parser.add_argument("--output", help="Fichier CSV de sortie (auto par défaut)")
    parser.add_argument("--confidence-threshold", type=float, default=0.5)
    parser.add_argument("--limit", type=int, help="Limiter nombre projets (test)")
    parser.add_argument("--delay", type=float, default=Config.RATE_LIMIT_DELAY)
    parser.add_argument("--parallel", action="store_true", help="Mode parallèle (recommandé)")
    parser.add_argument("--workers", type=int, default=Config.MAX_WORKERS)
    parser.add_argument("--benchmark", action="store_true", help="Afficher stats détaillées")
    parser.add_argument("--debug", action="store_true", help="Mode debug verbeux")
    
    args = parser.parse_args()
    
    # Debug mode
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Validation
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"❌ Fichier introuvable: {input_path}")
        sys.exit(1)
    
    if not settings.GOOGLE_API_KEY:
        logger.error("❌ GOOGLE_API_KEY manquante dans .env")
        sys.exit(1)
    
    # Output path
    if args.output:
        output_path = Path(args.output)
    else:
        filename = input_path.stem.replace("analyzed", "enriched") + ".csv"
        output_path = settings.OUTPUT_DIR / "enriched" / filename
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Chargement données
    logger.info(f"📂 Chargement: {input_path.name}")
    df = pd.read_csv(input_path, dtype=str).fillna("")
    logger.info(f"📊 Projets: {len(df)}")
    
    if args.limit:
        df = df.head(args.limit)
        logger.info(f"⚠️  Mode TEST: limité à {args.limit} projets")
    
    # Initialisation enrichisseur
    try:
        enricher = PermitEnricher(settings.GOOGLE_API_KEY)
    except Exception as e:
        logger.error(f"❌ Erreur init: {e}")
        sys.exit(1)
    
    # ═══════════════════════════════════════════════════════════════════════
    # TRAITEMENT
    # ═══════════════════════════════════════════════════════════════════════
    
    start_time = time.time()
    results = []
    low_confidence_count = 0
    
    if args.parallel:
        logger.info(f"⚡ Mode PARALLÈLE ({args.workers} workers, delay={args.delay}s)")
        
        tasks = [(enricher, row.to_dict(), args.delay) for _, row in df.iterrows()]
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_project_worker, task): idx 
                      for idx, task in enumerate(tasks)}
            
            for future in as_completed(futures):
                result = future.result()
                conf = float(result.get("permit_confidence", 0))
                
                if conf < args.confidence_threshold:
                    low_confidence_count += 1
                
                results.append(result)
    
    else:
        logger.info("🐌 Mode SÉQUENTIEL")
        
        for _, row in df.iterrows():
            permit_data = enricher.search_permit(row.to_dict())
            result = permit_data.to_csv_dict(row.to_dict())
            
            if permit_data.confidence < args.confidence_threshold:
                low_confidence_count += 1
            
            results.append(result)
            time.sleep(args.delay)
    
    duration = time.time() - start_time
    
    # ═══════════════════════════════════════════════════════════════════════
    # EXPORT & STATS
    # ═══════════════════════════════════════════════════════════════════════
    
    # Export principal
    df_out = pd.DataFrame(results)
    df_out.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"✅ Export: {len(results)} projets → {output_path}")
    
    # Fichier review (confiance faible)
    if low_confidence_count > 0:
        review_path = output_path.parent / f"review_{output_path.name}"
        df_review = df_out[df_out['permit_confidence'].astype(float) < args.confidence_threshold]
        df_review.to_csv(review_path, index=False, encoding="utf-8")
        logger.info(f"⚠️  Review: {low_confidence_count} projets → {review_path}")
    
    # Statistiques finales
    stats = enricher.get_stats()
    found = sum(1 for r in results if r.get('permit_number'))
    avg_conf = sum(float(r.get('permit_confidence', 0)) for r in results) / max(1, len(results))
    
    print("\n" + "="*80)
    print("📊 RÉSUMÉ FINAL")
    print("="*80)
    print(f"Total projets:           {len(results)}")
    print(f"Permis trouvés:          {found} ({found/len(results)*100:.1f}%)")
    print(f"Confiance moyenne:       {avg_conf:.2f}")
    print(f"Confiance < {args.confidence_threshold}:          {low_confidence_count}")
    print(f"Durée totale:            {duration:.1f}s")
    print(f"Vitesse:                 {len(results)/duration:.2f} projets/s")
    print(f"Cache hits:              {stats['cache_hits']}")
    print(f"Erreurs:                 {stats['errors']}")
    print("="*80)
    
    if args.benchmark:
        rpm_used = stats['api_calls'] / (duration / 60) if duration > 0 else 0
        cost = stats['api_calls'] * 0.0035  # Tier 1 pricing
        
        print(f"\n⏱️  BENCHMARK DÉTAILLÉ")
        print(f"   Appels API totaux:    {stats['api_calls']}")
        print(f"   - Recherches:         {stats['search_calls']}")
        print(f"   - Agrégations:        {stats['aggregation_calls']}")
        print(f"   Appels/projet:        {stats['api_calls']/len(results):.1f}")
        print(f"   RPM utilisé:          {rpm_used:.0f} / 1000")
        print(f"   Coût estimé:          ${cost:.3f}")
        print(f"   Workers:              {args.workers if args.parallel else 1}")
    
    print(f"\n✅ Traitement terminé avec succès en {duration:.1f}s\n")


if __name__ == "__main__":
    main()
