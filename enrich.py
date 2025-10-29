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
import hashlib
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import argparse
import logging

import httpx
import pandas as pd

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
    NUM_SEARCHES = 5
    
    # Gemini models config
    SEARCH_OUTPUT_TOKENS = 1024
    JSON_OUTPUT_TOKENS = 1536
    SEARCH_TEMPERATURE = 0.1
    JSON_TEMPERATURE = 0.0


class GeminiClient:
    """Client HTTP minimal pour Gemini API avec support Google Search."""
    
    BASE_URL = "https://generativelanguage.googleapis.com"
    
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.model = model
        self.client = httpx.Client(
            base_url=self.BASE_URL,
            timeout=Config.REQUEST_TIMEOUT,
            headers={"Content-Type": "application/json"}
        )
    
    def close(self):
        self.client.close()
    
    def generate(
        self,
        prompt: str,
        *,
        generation_config: Optional[Dict[str, Any]] = None,
        response_mime_type: Optional[str] = None,
        use_google_search: bool = False
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "contents": [
                {"role": "user", "parts": [{"text": prompt}]}
            ]
        }
        
        config = dict(generation_config or {})
        if response_mime_type:
            config["responseMimeType"] = response_mime_type
        if config:
            body["generationConfig"] = config
        
        if use_google_search:
            body["tools"] = [{"googleSearch": {}}]
        
        response = self.client.post(
            f"/v1beta/models/{self.model}:generateContent",
            params={"key": self.api_key},
            json=body
        )
        response.raise_for_status()
        data = response.json()
        
        candidates = data.get("candidates", [])
        text_parts: List[str] = []
        grounding_metadata: Dict[str, Any] = {}
        
        if candidates:
            first = candidates[0]
            content = first.get("content", {})
            for part in content.get("parts", []):
                if "text" in part:
                    text_parts.append(part["text"])
            grounding_metadata = first.get("groundingMetadata", {}) or {}
        
        return {
            "text": "".join(text_parts).strip(),
            "grounding_metadata": grounding_metadata,
            "raw": data
        }


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
    
    SEARCH_PREF_GENERAL = """Tu es un expert des permis de construire français.
Identifie le permis de construire délivré par la {prefecture_hint} pour le projet "{title}" situé à {commune} ({dept}).
Utilise uniquement des requêtes Google ciblées sur la préfecture compétente :
1. prefecture "{commune}" "permis de construire" {dept}
2. "{prefecture_hint}" "permis de construire" "{commune}"
Ne retiens que les résultats qui mentionnent explicitement un permis de construire émis par la préfecture (PC). Ignore CERFA, avis MRAE, enquêtes publiques et dossiers de participation.

Format réponse :
Permis [numéro] Préfecture [nom] URL [lien] Confiance [0-1]
OU "Non trouvé" si aucun permis préfectoral n'apparaît."""

    SEARCH_PREF_ARRETE = """Recherche l'arrêté préfectoral accordant le permis de construire pour "{title}" à {commune} ({dept}).
Focus : communiqué ou arrêté signé par la {prefecture_hint}.
Requêtes suggérées :
1. "arrêté préfectoral" "permis de construire" "{commune}" {dept}
2. "{prefecture_hint}" "arrêté" "permis de construire" "{title}"
Ne renvoie que des permis signés par la préfecture. Écarte CERFA, études d'impact ou autres documents préparatoires.

Format : Permis [numéro] Préfecture [nom] URL [lien] Confiance [0-1] OU Non trouvé."""

    SEARCH_PREF_RAA = """Consulte le Recueil des Actes Administratifs (RAA) ou bulletins de la {prefecture_hint} pour trouver le permis de construire correspondant à "{title}" ({commune}, {dept}).
Requêtes :
1. "recueil actes administratifs" "{commune}" "permis de construire"
2. site:gouv.fr "{commune}" "permis de construire" "{prefecture_hint}"
Ne t'intéresse qu'aux permis de construire préfectoraux (PC). Ignore décisions CERFA, avis d'enquête, déclarations préalables.

Format : Permis [numéro] Préfecture [nom] URL [lien] Confiance [0-1] OU Non trouvé."""

    SEARCH_PREF_ARCHIVE = """Explore les archives ou bases documentaires de la préfecture compétente ({prefecture_hint}) pour le permis de construire du projet "{title}" ({commune}, {dept}).
Requêtes :
1. "{prefecture_hint}" "autorisation environnementale" "permis de construire"
2. "pc" "{commune}" "{prefecture_hint}" filetype:pdf
Ne remonte que les permis de construire préfectoraux. Exclue avis MRAE, CERFA, documents sans décision de la préfecture.

Format : Permis [numéro] Préfecture [nom] URL [lien] Confiance [0-1] OU Non trouvé."""

    SEARCH_PREF_COMMUNICATION = """Vérifie si la {prefecture_hint} a publié un communiqué ou une actualité mentionnant le permis de construire pour "{title}" ({commune}, {dept}).
Requêtes :
1. site:gouv.fr "{commune}" "autorisation" "préfecture"
2. "{prefecture_hint}" "permis de construire" "batterie" "{commune}"
Ne conserve que des informations confirmant un permis de construire préfectoral. Ignore tout autre document administratif.

Format : Permis [numéro] Préfecture [nom] URL [lien] Confiance [0-1] OU Non trouvé."""

    AGGREGATION = """Analyse des recherches pour le permis préfectoral du projet "{title}" à {commune} ({dept}).
Objectif : identifier uniquement le permis de construire émis par la préfecture compétente. Ignore CERFA, avis MRAE, consultations ou dossiers préparatoires.

R1 (Préfecture ciblée) : {r1}
R2 (Arrêté préfectoral) : {r2}
R3 (RAA / bulletin) : {r3}
R4 (Archives préfectorales) : {r4}
R5 (Communication officielle) : {r5}

Retourne un JSON STRICT (aucun texte hors JSON) avec ce schéma :
{{
  "permit_number": "XXX YYY ZZ RNNNN ou null",
  "permit_type": "PC ou null",
  "issue_date": "YYYY-MM-DD ou null",
  "applicant": "nom ou null",
  "source_url": "URL préfectorale ou null",
  "source_type": "prefecture_arrete | prefecture_raa | prefecture_communique | autre | null",
  "confidence": 0.0,
  "search_summary": "Phrase courte décrivant la source retenue"
}}

Contraintes :
- Résultat uniquement si la source confirme un permis de construire préfectoral.
- Si rien de concluant : toutes les valeurs à null et confidence 0.0.
"""


# ═══════════════════════════════════════════════════════════════════════════
# DATACLASSES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SearchResult:
    """Résultat d'une recherche ciblée"""
    label: str
    text: str
    urls: List[str] = field(default_factory=list)
    success: bool = False
    duration: float = 0.0
    api_calls: int = 1


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


@lru_cache(maxsize=128)
def get_prefecture_hint(dept_code: str) -> str:
    """Retourne un libellé lisible pour la préfecture départementale"""
    code = (dept_code or "").strip()
    if not code:
        return "préfecture du département"
    
    code = code.zfill(2)
    slug = get_dept_slug(code)
    if slug.startswith("dept"):
        return f"préfecture du département {code}"
    
    words = slug.replace("-", " ").split()
    title = " ".join(w.capitalize() for w in words)
    return f"préfecture de {title}"


def extract_grounding_urls(metadata: Optional[Dict[str, Any]]) -> List[str]:
    """Extraction URLs depuis grounding metadata"""
    urls: List[str] = []
    if not metadata:
        return urls
    
    chunks = metadata.get("groundingChunks") or metadata.get("grounding_chunks") or []
    
    for chunk in chunks[:10]:
        web = chunk.get("web") if isinstance(chunk, dict) else None
        if not web and isinstance(chunk, dict):
            # Certains formats utilisent snake_case
            web = chunk.get("webData") or chunk.get("web_data")
        if isinstance(web, dict):
            uri = web.get("uri") or web.get("url")
            if uri:
                urls.append(str(uri))
    
    return urls


# ═══════════════════════════════════════════════════════════════════════════
# ENRICHISSEUR PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════

class PermitEnricher:
    """Enrichisseur haute performance avec pattern multi-search"""
    
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("GOOGLE_API_KEY manquante dans .env")
        
        self.stats = EnricherStats()
        self._cache: Dict[str, PermitData] = {}
        self._cache_lock = threading.Lock()
        self.gemini = GeminiClient(api_key, settings.GEMINI_MODEL)
        
        # Configurations Gemini
        self.search_generation_config = {
            "temperature": Config.SEARCH_TEMPERATURE,
            "maxOutputTokens": Config.SEARCH_OUTPUT_TOKENS,
        }
        
        self.json_generation_config = {
            "temperature": Config.JSON_TEMPERATURE,
            "maxOutputTokens": Config.JSON_OUTPUT_TOKENS,
        }
    
    def _cache_key(self, project: Dict) -> str:
        """Génère clé cache unique"""
        project_id = project.get("project_id")
        if project_id:
            return str(project_id)
        
        components = [
            project.get("project_url", ""),
            project.get("project_title", ""),
            project.get("commune", ""),
            project.get("dept", ""),
            project.get("year", "")
        ]
        composite = "|".join(str(part) for part in components)
        return hashlib.sha1(composite.encode("utf-8")).hexdigest()
    
    def _execute_search(self, prompt: str, name: str) -> SearchResult:
        """Exécute une recherche ciblée avec validation"""
        start = time.time()
        
        try:
            result = self.gemini.generate(
                prompt,
                generation_config=self.search_generation_config,
                use_google_search=True
            )
            text = result.get("text", "")
            metadata = result.get("grounding_metadata")
            
            self.stats.increment('api_calls')
            self.stats.increment('search_calls')
            
            # Validation
            text = (text or "").strip()
            urls = extract_grounding_urls(metadata)
            
            if len(text) < 10 and not urls:
                logger.debug(f"  ✗ {name}: response too short ({len(text)} chars)")
                return SearchResult(
                    label=name,
                    text="Non trouvé",
                    success=False,
                    duration=time.time() - start,
                    api_calls=1
                )
            
            # Détection succès
            success = any(kw in text.lower() for kw in ['permis', 'pc', 'trouvé']) or len(urls) > 0
            
            logger.debug(f"  ✓ {name}: {len(text)} chars, {len(urls)} URLs, success={success}")
            
            return SearchResult(
                label=name,
                text=text,
                urls=urls,
                success=success,
                duration=time.time() - start,
                api_calls=1
            )
        
        except Exception as e:
            logger.debug(f"  ✗ {name} error: {e}")
            self.stats.increment('errors')
            return SearchResult(
                label=name,
                text=f"Erreur: {str(e)}",
                success=False,
                duration=time.time() - start,
                api_calls=1
            )
    
    def _aggregate_results(self, title: str, commune: str, dept: str, 
                          results: List[SearchResult]) -> PermitData:
        """Agrège les résultats via Gemini JSON"""
        total_calls = sum(r.api_calls for r in results)
        
        trimmed_blocks: List[str] = []
        for result in results[:Config.NUM_SEARCHES]:
            snippet = (result.text or "Non trouvé").strip()
            trimmed_blocks.append(f"{result.label.upper()}: {snippet[:700]}")
        
        while len(trimmed_blocks) < Config.NUM_SEARCHES:
            trimmed_blocks.append("AUCUNE DONNÉE")
        
        if len(results) > Config.NUM_SEARCHES:
            extras = []
            for result in results[Config.NUM_SEARCHES:]:
                snippet = (result.text or "Non trouvé").strip()
                extras.append(f"{result.label.upper()}: {snippet[:200]}")
            if extras:
                trimmed_blocks[-1] += "\nSUPPLÉMENT:\n" + "\n".join(extras)
        
        try:
            # Préparation prompt
            prompt = Prompts.AGGREGATION.format(
                title=title,
                commune=commune,
                dept=dept,
                r1=trimmed_blocks[0],
                r2=trimmed_blocks[1],
                r3=trimmed_blocks[2],
                r4=trimmed_blocks[3],
                r5=trimmed_blocks[4]
            )
            
            # Appel Gemini
            result = self.gemini.generate(
                prompt,
                generation_config=self.json_generation_config,
                response_mime_type="application/json"
            )
            
            self.stats.increment('api_calls')
            self.stats.increment('aggregation_calls')
            
            # Parsing JSON
            text = result.get("text", "")
            if not text:
                raise ValueError("Empty JSON response")
            
            data = JSONCleaner.parse(str(text))
            
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
                api_calls=total_calls + 1
            )
        
        except Exception as e:
            logger.error(f"  ✗ Aggregation error: {e}")
            self.stats.increment('errors')
            return PermitData(
                search_summary=f"Erreur agrégation: {str(e)}",
                api_calls=total_calls + 1
            )
    
    def search_permit(self, project: Dict, use_cache: bool = True) -> PermitData:
        """Point d'entrée principal: recherche multi-sources + agrégation"""
        
        start_time = time.time()
        
        # Cache check
        cache_key = self._cache_key(project)
        if use_cache:
            with self._cache_lock:
                cached = self._cache.get(cache_key)
            if cached:
                self.stats.increment('cache_hits')
                logger.info(f"  ⚡ Cache: {project.get('project_title', '')[:40]}")
                return cached
        
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
            "region_slug": region_slug,
            "prefecture_hint": get_prefecture_hint(dept)
        }
        
        # Retry loop
        for attempt in range(Config.MAX_RETRIES):
            try:
                logger.info(f"🔍 [{attempt+1}/{Config.MAX_RETRIES}] {title[:45]}...")
                
                # ════════════════════════════════════════════════════════
                # PHASE 1: 4 RECHERCHES CIBLÉES (séquentiel stable)
                # ════════════════════════════════════════════════════════
                
                searches = [
                    (Prompts.SEARCH_PREF_GENERAL, "Préfecture générale"),
                    (Prompts.SEARCH_PREF_ARRETE, "Arrêté préfectoral"),
                    (Prompts.SEARCH_PREF_RAA, "RAA préfecture"),
                    (Prompts.SEARCH_PREF_ARCHIVE, "Archives préfecture"),
                    (Prompts.SEARCH_PREF_COMMUNICATION, "Communication préfecture"),
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
                
                if not any(r.success or r.urls for r in results):
                    if attempt < Config.MAX_RETRIES - 1:
                        logger.debug("  Aucun signal préfectoral, nouvelle tentative")
                        continue
                    
                    total_calls = sum(r.api_calls for r in results)
                    permit_data = PermitData(
                        search_summary="Aucune source préfectorale concluante",
                        api_calls=total_calls
                    )
                    permit_data.duration = time.time() - start_time
                    
                    if use_cache:
                        with self._cache_lock:
                            self._cache[cache_key] = permit_data
                    
                    logger.info("  ⚠️ 0.00 - N/A (préfecture introuvable)")
                    return permit_data
                
                # ════════════════════════════════════════════════════════
                # PHASE 2: AGRÉGATION JSON
                # ════════════════════════════════════════════════════════
                
                permit_data = self._aggregate_results(title, commune, dept, results)
                permit_data.duration = time.time() - start_time
                
                # Cache
                if use_cache:
                    with self._cache_lock:
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
    
    def close(self):
        """Libère ressources clients"""
        try:
            self.gemini.close()
        except Exception:
            pass


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
    try:
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
    finally:
        enricher.close()


if __name__ == "__main__":
    main()
