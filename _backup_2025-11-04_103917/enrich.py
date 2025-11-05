#!/usr/bin/env python3
"""
ENRICHISSEMENT v13.3 - VERSION FINALE ROBUSTE
- Système de scoring multi-critères
- Validation contenu avant/après extraction
- Quick PDF scan pour éviter extractions inutiles
- Cache HEAD requests
- Classification de pages DREAL (vs cas par cas)
- Validation stricte numéros PC vs cas par cas
- Logs structurés sans bruit

Auteur: Veille Stockage POC
Date: Octobre 2025
"""

import sys
import json
import time
import re
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import argparse
import logging
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import pandas as pd
from selectolax.parser import HTMLParser

from config import settings
from utils import (
    HTTPClient, 
    HTTPDownloadTooLarge, 
    extract_pdf_robust, 
    is_poor_text,
    quick_pdf_scan,
    PDFMetadataCache
)
from validation import (
    score_url_relevance,
    validate_content_match,
    validate_permit_data,
    URLScore
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# LOGGING CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

def configure_logging(debug: bool):
    """Configure les logs avec filtrage intelligent"""
    app_level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    
    main_logger = logging.getLogger(__name__)
    main_logger.setLevel(app_level)
    
    logging.getLogger("validation").setLevel(app_level)
    logging.getLogger("utils").setLevel(app_level)
    logging.getLogger("config").setLevel(app_level)
    
    # Silencer librairies bruyantes
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpcore.connection").setLevel(logging.WARNING)
    logging.getLogger("httpcore.http11").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("google.auth").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    
    if debug:
        logging.getLogger("httpx").setLevel(logging.INFO)
        main_logger.debug("🔧 Mode DEBUG activé (logs détaillés)")


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

class Config:
    CSE_ID = os.getenv("GOOGLE_CSE_ID")
    
    MAX_SEARCH_RESULTS = 10
    MAX_PAGES_TO_SCRAPE = 8
    MAX_PDFS_PER_PAGE = 3
    MAX_PDF_SIZE_MB = 20
    
    URL_SCORE_THRESHOLD = 0.25
    QUICK_SCAN_PAGES = 2
    MIN_COMMUNE_MENTIONS = 2
    
    MAX_WORKERS_SCRAPING = 3
    MAX_WORKERS_GEMINI = 2
    
    GEMINI_MODEL = "gemini-2.5-flash"
    MAX_OUTPUT_TOKENS = 2048
    
    SEARCH_DELAY = 1.0
    SCRAPE_DELAY = 0.3
    PROJECT_DELAY = 1.5
    
    # Nouvelles options
    ENABLE_PAGE_CLASSIFICATION = True
    REJECT_CAS_PAR_CAS_AS_PC = True
    VALIDATE_PC_FORMAT = True


DEPT_NAMES = {
    "01": "ain", "03": "allier", "07": "ardeche", "15": "cantal",
    "26": "drome", "38": "isere", "42": "loire", "43": "haute-loire",
    "63": "puy-de-dome", "69": "rhone", "73": "savoie", "74": "haute-savoie"
}


# ═══════════════════════════════════════════════════════════════════════════
# PAGE CLASSIFICATION (NOUVEAU)
# ═══════════════════════════════════════════════════════════════════════════

class PageType(Enum):
    """Types de pages DREAL détectées"""
    PERMIS_CONSTRUIRE = "permis_construire"
    CAS_PAR_CAS = "cas_par_cas"
    CONSULTATION_PUBLIQUE = "consultation"
    AVIS_MRAE = "avis_mrae"
    ARRETE_PREFECTORAL = "arrete"
    AUTRE = "autre"

    def __str__(self):
        return self.value


class PageClassifier:
    """Classifie les pages DREAL pour éviter confusions cas par cas / PC"""
    
    @staticmethod
    def classify(url: str, content: str) -> Tuple[PageType, str]:
        """Classifie une page DREAL"""
        if not content:
            return PageType.AUTRE, "Contenu vide"
        
        content_lower = content.lower()
        url_lower = url.lower()
        
        # ========== DÉTECTION : CAS PAR CAS (À REJETER) ==========
        if re.search(r'\d{4}-[a-z]{3}-[a-z]{3}-(?:icpe|kkp)', content_lower):
            if re.search(r'(?:cas\s+par\s+cas|examen\s+préalable|dossier\s+reçu)', content_lower):
                return PageType.CAS_PAR_CAS, "Numéro cas par cas + mention 'cas par cas'"
        
        if re.search(r'dossier\s+n°\s*20\d{2}-[a-z]{3}', content_lower):
            if re.search(r'(?:décision|cas\s+par\s+cas)', content_lower):
                return PageType.CAS_PAR_CAS, "Format numéro cas par cas avec 'décision'"
        
        # ========== DÉTECTION : PERMIS DE CONSTRUIRE ==========
        if re.search(r'\bpc\s*(?:\d{2,3}\s+)?(?:\d{3}\s+)?(?:\d{2}\s+)?\d{5}', content_lower):
            if re.search(r'(?:permis\s+de\s+construire|pc)', content_lower):
                return PageType.PERMIS_CONSTRUIRE, "Numéro PC détecté + mention 'permis de construire'"
        
        if re.search(r'permis\s+(?:de\s+)?construire', content_lower):
            if re.search(r'(?:dépôt|délivrance|autorisation|date)', content_lower):
                return PageType.PERMIS_CONSTRUIRE, "Mention explicite 'permis de construire'"
        
        # ========== DÉTECTION : CONSULTATION PUBLIQUE ==========
        if re.search(r'consultation(?:\s+du)?(?:\s+public)?|participation', content_lower):
            if 'http' in url_lower or 'participation' in content_lower:
                return PageType.CONSULTATION_PUBLIQUE, "Page consultation du public détectée"
        
        # ========== DÉTECTION : AVIS MRAE ==========
        if re.search(r'mrae|avis\s+délibéré|autorité\s+environnementale', content_lower):
            if '.pdf' in url_lower or 'avis' in content_lower:
                return PageType.AVIS_MRAE, "Avis MRAe détecté"
        
        # ========== DÉTECTION : ARRÊTÉ PRÉFECTORAL ==========
        if re.search(r'arrêté|arrete|autorisation\s+préfectorale', content_lower):
            if re.search(r'(?:n°|numéro|dossier)', content_lower):
                return PageType.ARRETE_PREFECTORAL, "Arrêté préfectoral détecté"
        
        return PageType.AUTRE, "Type de page non identifié"


def classify_dreal_page(url: str, content: str) -> PageType:
    """Wrapper pour classification"""
    page_type, reason = PageClassifier.classify(url, content)
    logger.debug(f"      Classification: {page_type} ({reason})")
    return page_type


# ═══════════════════════════════════════════════════════════════════════════
# VALIDATION PERMIT NUMBERS (NOUVEAU)
# ═══════════════════════════════════════════════════════════════════════════

def validate_and_clean_permit_number(permit_number: Optional[str], page_type: PageType) -> Tuple[Optional[str], float, str]:
    """
    Valide et nettoie un numéro de permis
    
    Returns:
        (permit_number_cleaned, confidence_adjustment, reason)
    """
    if not permit_number:
        return None, 1.0, "Aucun numéro extrait"
    
    permit_number = permit_number.strip()
    
    # ========== REJETER : Numéros cas par cas ==========
    if re.match(r'^\d{4}-[a-z]{3}-[a-z]{3}', permit_number.lower()):
        logger.warning(f"❌ Numéro CAS PAR CAS confondu avec PC: {permit_number}")
        return None, 0.0, f"Numéro cas par cas rejeté: {permit_number}"
    
    # ========== PATTERN PC FRANÇAIS STANDARD ==========
    pc_standard = re.match(r'^pc\s*(\d{2,3})\s*(\d{3})\s*(\d{2})\s*(\d{5})$', permit_number.lower())
    if pc_standard:
        insee, annee, seq, num = pc_standard.groups()
        cleaned = f"PC {insee} {annee} {seq} {num}"
        logger.debug(f"    ✓ PC standard validé: {cleaned}")
        return cleaned, 1.0, "PC format standard"
    
    # ========== PATTERN PC COMPACT ==========
    pc_compact = re.match(r'^pc?(\d{2,3})(\d{3})(\d{2})(\d{5})$', permit_number.lower())
    if pc_compact:
        insee, annee, seq, num = pc_compact.groups()
        cleaned = f"PC {insee} {annee} {seq} {num}"
        logger.debug(f"    ✓ PC compact nettoyé: {cleaned}")
        return cleaned, 0.9, "PC format compact (confiance -0.1)"
    
    # ========== PATTERN ARRÊTÉ PRÉFECTORAL ==========
    if re.match(r'^(?:ap|ara-ap)[-\s]\d+', permit_number.lower()):
        logger.debug(f"    ⚠️  Numéro arrêté (pas PC): {permit_number}")
        return None, 0.5, f"Arrêté préfectoral (pas numéro PC): {permit_number}"
    
    # ========== PATTERN DOSSIER ==========
    if re.match(r'^dossier\s+n°', permit_number.lower()):
        logger.debug(f"    ⚠️  Numéro dossier (pas PC): {permit_number}")
        return None, 0.4, f"Numéro dossier administratif (pas PC): {permit_number}"
    
    # ========== FORMAT STANDARD SANS PRÉFIXE ==========
    if re.match(r'^\d{2,3}\s+\d{3}\s+\d{2}\s+\d{5}$', permit_number):
        cleaned = f"PC {permit_number}"
        logger.debug(f"    ⚠️  PC sans préfixe 'PC': {cleaned}")
        return cleaned, 0.7, "PC sans préfixe (confiance -0.3)"
    
    # ========== FORMAT NON RECONNU ==========
    if len(permit_number) < 5:
        logger.debug(f"    ✗ Format trop court: {permit_number}")
        return None, 0.0, f"Format invalide (trop court): {permit_number}"
    
    logger.warning(f"⚠️  Format PC non standard: {permit_number}")
    return permit_number, 0.6, f"Format non standard (confiance réduite): {permit_number}"


# ═══════════════════════════════════════════════════════════════════════════
# FILTRE TEMPOREL
# ═══════════════════════════════════════════════════════════════════════════

class TemporalFilter:
    """Filtre les sources par date"""
    
    @staticmethod
    def extract_year_from_url(url: str) -> Optional[int]:
        """Extrait année depuis URL"""
        match = re.search(r'[/_-](\d{4})[/_-]', url)
        if match:
            year = int(match.group(1))
            if 2020 <= year <= 2030:
                return year
        return None
    
    @staticmethod
    def extract_year_from_text(text: str) -> Optional[int]:
        """Extrait année depuis texte"""
        matches = re.findall(r'\b(202[0-9])\b', text[:500])
        if matches:
            return int(matches[0])
        return None
    
    @staticmethod
    def is_relevant(url: str, text: str, project_year: int) -> bool:
        """Vérifie si source est pertinente temporellement"""
        year_min = project_year - 1
        year_max = project_year + 3
        
        year = TemporalFilter.extract_year_from_url(url)
        if not year:
            year = TemporalFilter.extract_year_from_text(text)
        
        if not year:
            return True
        
        is_ok = year_min <= year <= year_max
        if not is_ok:
            logger.debug(f"      ✗ Filtre temporel: {year} hors fenêtre [{year_min}, {year_max}]")
        return is_ok


# ═══════════════════════════════════════════════════════════════════════════
# CLASSIFICATION DES SOURCES
# ═══════════════════════════════════════════════════════════════════════════

class SourcePriority:
    @staticmethod
    def classify_url(url: str, title: str) -> Tuple[int, str]:
        url_lower = url.lower()
        title_lower = title.lower()
        
        if "arrêté" in title_lower or "arrete" in title_lower or "decision" in title_lower:
            if ".pdf" in url_lower:
                return (0, "arrete_pdf")
        
        if "consultation" in url_lower and "public" in url_lower:
            if ".gouv.fr" in url_lower:
                return (1, "consultation_html")
        
        if "mrae" in title_lower or "avis délibéré" in title_lower:
            if ".pdf" in url_lower:
                return (2, "avis_mrae_pdf")
        
        if "avis" in title_lower and ".pdf" in url_lower:
            return (3, "avis_pdf")
        
        if "developpement-durable.gouv.fr" in url_lower:
            return (4, "dreal_html")
        
        if ".gouv.fr" in url_lower:
            return (5, "gouv_html")
        
        return (10, "other")
    
    @staticmethod
    def get_size_limit_mb(url: str, title: str) -> float:
        priority, doc_type = SourcePriority.classify_url(url, title)
        if priority <= 3:
            return Config.MAX_PDF_SIZE_MB
        if "étude" in title.lower() or "etude" in title.lower() or "impact" in title.lower():
            return min(5, Config.MAX_PDF_SIZE_MB)
        return min(10, Config.MAX_PDF_SIZE_MB)
    
    @staticmethod
    def should_extract_pdf(url: str, title: str, size_mb: Optional[float]) -> bool:
        if size_mb is None:
            return True
        priority, _ = SourcePriority.classify_url(url, title)
        if priority <= 3:
            return size_mb <= Config.MAX_PDF_SIZE_MB
        limit = SourcePriority.get_size_limit_mb(url, title)
        return size_mb <= limit


# ═══════════════════════════════════════════════════════════════════════════
# CLIENTS
# ═══════════════════════════════════════════════════════════════════════════

class GoogleCustomSearch:
    """Client Google Custom Search API"""
    
    def __init__(self, api_key: str, cx: str):
        if not api_key or not cx:
            raise ValueError("API_KEY et CSE_ID requis")
        self.api_key = api_key
        self.cx = cx
        self.client = httpx.Client(timeout=30)
        logger.info(f"✓ CSE: {cx[:20]}...")
    
    def search(self, query: str, num: int = 10) -> List[Dict]:
        """Effectue une recherche Google Custom Search"""
        try:
            response = self.client.get(
                "https://www.googleapis.com/customsearch/v1",
                params={
                    "key": self.api_key,
                    "cx": self.cx,
                    "q": query,
                    "num": min(num, 10)
                }
            )
            response.raise_for_status()
            items = response.json().get("items", [])
            return [
                {
                    "title": i.get("title", ""),
                    "url": i.get("link", ""),
                    "snippet": i.get("snippet", "")
                }
                for i in items[:num]
            ]
        except Exception as e:
            logger.debug(f"Search error: {e}")
            return []
    
    def close(self):
        self.client.close()


class GeminiClient:
    """Client Gemini API pour extraction d'infos permis"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.Client(timeout=30)
    
    def extract_from_source(self, source: Dict, commune: str, demandeur: str, project_year: int) -> Dict:
        """
        Extraction depuis UNE source avec validation intégrée
        
        V2.0 - Améliorations:
        - Utilise page_type pour adapter le prompt
        - Valide le numéro PC retourné
        - Rejet explicite cas par cas
        """
        source_type = source.get("type", "unknown")
        page_type = source.get("page_type", PageType.AUTRE)
        content = source.get("content", "")[:8000]
        url = source.get("url", "")
        
        # ✗ REJET PRÉALABLE : Cas par cas
        if page_type == PageType.CAS_PAR_CAS:
            logger.warning(f"  ❌ Source cas par cas ignorée (pas de PC)")
            return {"confidence": 0.0, "reason": "Page cas par cas"}
        
        # Contexte adapté selon type de source
        context_map = {
            PageType.PERMIS_CONSTRUIRE: "PAGE PERMIS DE CONSTRUIRE (haute priorité). Contient numéro PC et dates dépôt/délivrance.",
            PageType.CONSULTATION_PUBLIQUE: "PAGE CONSULTATION PUBLIQUE. Contient dépôt du permis et période consultation.",
            PageType.AVIS_MRAE: "AVIS MRAe. Introduction contient date dépôt du permis.",
            PageType.ARRETE_PREFECTORAL: "ARRÊTÉ PRÉFECTORAL. Peut contenir numéro PC ou référence permis.",
            PageType.AUTRE: "Document administratif officiel."
        }
        context = context_map.get(page_type, "Document administratif.")
        
        year_min = project_year - 1
        year_max = project_year + 3
        
        # ✓ NOUVEAU: Prompt avec instruction stricte PC vs CAS PAR CAS
        prompt = f"""{context}

PROJET CIBLE:
- Commune: {commune}
- Demandeur: {demandeur}
- Année: {project_year}

INSTRUCTIONS CRITIQUES - LIRE ATTENTIVEMENT:
1. Ce document parle-t-il d'un PERMIS DE CONSTRUIRE ou d'un CAS PAR CAS ?
   - PERMIS DE CONSTRUIRE : Numéro format "PC [INSEE] [ANNEE] [NUMERO]"
   - CAS PAR CAS : Numéro format "20XX-XYZ-KKP-ICPE-XXXX" → IGNORER COMPLÈTEMENT
2. Si le document est un CAS PAR CAS (décision préalable), retourne {{"confidence": 0.0}}
3. Vérifie si ce document concerne EXACTEMENT la commune "{commune}"
4. Cherche dates entre {year_min} et {year_max}
5. N'extrais QUE les dates de PERMIS DE CONSTRUIRE, pas de cas par cas

CONTENU:
{content}

Extrais SEULEMENT si PERMIS DE CONSTRUIRE:
- is_construction_permit: true/false (c'est bien un PC ?)
- commune_found: quelle commune est mentionnée ?
- commune_match: ce doc concerne-t-il {commune} ? (true/false)
- permit_number: format "PC XXX YYY ZZ AAAAA" ou null
- deposit_date: YYYY-MM-DD ou null (dépôt du PC)
- issue_date: YYYY-MM-DD ou null (délivrance du PC)
- consultation_start: YYYY-MM-DD ou null (début consultation)
- consultation_end: YYYY-MM-DD ou null (fin consultation)

Convertir dates françaises DD/MM/YYYY → YYYY-MM-DD
IGNORER dates hors fenêtre {year_min}-{year_max}
IGNORER numéros CAS PAR CAS (format 20XX-XXX-...)

JSON:
{{
  "is_construction_permit": true/false,
  "commune_found": "string",
  "commune_match": true/false,
  "permit_number": "string ou null",
  "deposit_date": "YYYY-MM-DD ou null",
  "issue_date": "YYYY-MM-DD ou null",
  "consultation_start": "YYYY-MM-DD ou null",
  "consultation_end": "YYYY-MM-DD ou null",
  "confidence": 0.0-1.0
}}

Confidence:
- 0.0: Cas par cas OU commune mismatch OU rien trouvé
- 0.3: Commune match mais infos partielles
- 0.6: PC + 1-2 dates, .gouv.fr
- 0.8: PC + 2-3 dates, commune match
- 1.0: PC + date signature + commune match clair
"""
        
        try:
            response = self.client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{Config.GEMINI_MODEL}:generateContent",
                params={"key": self.api_key},
                json={
                    "contents": [{
                        "role": "user",
                        "parts": [{"text": prompt}]
                    }],
                    "generationConfig": {
                        "maxOutputTokens": Config.MAX_OUTPUT_TOKENS,
                        "temperature": 0.0,
                        "responseMimeType": "application/json"
                    }
                }
            )
            response.raise_for_status()
            
            parts = []
            for candidate in response.json().get("candidates", []):
                for part in candidate.get("content", {}).get("parts", []):
                    if "text" in part:
                        parts.append(part["text"])
            
            raw_text = "".join(parts).strip()
            if not raw_text:
                return {"confidence": 0.0}
            
            # Parse JSON
            fenced = re.match(r"``````", raw_text, re.DOTALL)
            if fenced:
                raw_text = fenced.group(1).strip()
            
            try:
                data = json.loads(raw_text)
                
                # ✓ NOUVEAU: Vérifier is_construction_permit
                if not data.get("is_construction_permit", False):
                    logger.debug(f"    ⚠️  Gemini: pas un PC ou cas par cas détecté")
                    return {"confidence": 0.0, "reason": "Not a construction permit"}
                
                # ✓ NOUVEAU: Vérifier commune_match
                if not data.get("commune_match", True):
                    logger.debug(f"    ⚠️  Gemini: commune mismatch (trouvé: {data.get('commune_found')})")
                    return {"confidence": 0.0, "reason": "Commune mismatch"}
                
                data["source_url"] = url
                data["source_type"] = source_type
                data["page_type"] = str(page_type)
                
                return data
            
            except json.JSONDecodeError as exc:
                logger.debug(f"Gemini JSON parse error: {exc}")
        
        except Exception as e:
            logger.debug(f"Gemini error: {e}")
        
        return {"confidence": 0.0}
    
    def close(self):
        self.client.close()


# ═══════════════════════════════════════════════════════════════════════════
# EXTRACTEURS
# ═══════════════════════════════════════════════════════════════════════════

def scrape_html(url: str, http_client: HTTPClient) -> Optional[str]:
    """Scrape et nettoie du contenu HTML"""
    try:
        html, _ = http_client.get_text(url)
        tree = HTMLParser(html)
        for tag in tree.css("script, style, nav, header, footer"):
            tag.decompose()
        if tree.body:
            return tree.body.text(separator=" ", strip=True)
    except:
        pass
    return None


def extract_pdf_text(url: str, title: str, http_client: HTTPClient, commune: str) -> Optional[str]:
    """
    Extrait texte d'un PDF avec validation pré/post-téléchargement
    """
    try:
        # HEAD request pour taille
        head_size_mb: Optional[float] = None
        try:
            head_response = http_client.head(url)
            content_length = head_response.headers.get("Content-Length")
            if content_length:
                try:
                    head_size_mb = int(content_length) / (1024 * 1024)
                except ValueError:
                    head_size_mb = None
        except Exception as head_error:
            logger.debug(f"      HEAD failed: {head_error}")
        
        if head_size_mb is not None and not SourcePriority.should_extract_pdf(url, title, head_size_mb):
            logger.debug(f"      ⏭️  Skip HEAD size: {head_size_mb:.1f}MB")
            return None
        
        # Téléchargement
        size_limit_mb = SourcePriority.get_size_limit_mb(url, title)
        max_bytes = int(size_limit_mb * 1024 * 1024)
        
        pdf_bytes, _ = http_client.get_bytes(url, max_bytes=max_bytes)
        size_mb = len(pdf_bytes) / (1024 * 1024)
        
        if not SourcePriority.should_extract_pdf(url, title, size_mb):
            logger.debug(f"      ⏭️  Skip body size: {size_mb:.1f}MB")
            return None
        
        # Quick scan (2 premières pages)
        if Config.QUICK_SCAN_PAGES > 0:
            if not quick_pdf_scan(pdf_bytes, commune, Config.QUICK_SCAN_PAGES):
                logger.debug(f"      ⏭️  Quick scan: commune '{commune}' absente des {Config.QUICK_SCAN_PAGES} premières pages")
                return None
        
        # Extraction complète
        text, method = extract_pdf_robust(pdf_bytes)
        if text and not is_poor_text(text):
            logger.debug(f"      ✓ Extrait via {method}: {len(text)} chars")
            return text
    
    except HTTPDownloadTooLarge:
        logger.debug("      ⏭️  Skip: téléchargement interrompu (fichier trop volumineux)")
    except Exception as e:
        logger.debug(f"      ✗ Erreur: {e}")
    
    return None


# ═══════════════════════════════════════════════════════════════════════════
# QUERIES (AMÉLIORÉ)
# ═══════════════════════════════════════════════════════════════════════════

def build_queries(project: Dict) -> List[str]:
    """
    Construit des requêtes Google ciblées pour PERMIS DE CONSTRUIRE
    
    V2.0 - Amélioration Phase 3:
    - Cherche EXPLICITEMENT permis de construire
    - Exclut pages cas par cas
    - Priorise pages consultation + arrêtés
    """
    commune = project.get("commune", "")
    dept = project.get("dept", "")
    year = int(project.get("year", 2023))
    dept_name = DEPT_NAMES.get(dept, f"dept{dept}")
    
    queries = []
    
    if not commune:
        return queries
    
    # ========== QUERY 1 : PERMIS CONSTRUIRE DIRECT ==========
    if dept:
        queries.append(
            f'"{commune}" ("permis de construire" OR "PC") '
            f'stockage batteries électricité énergie '
            f'-"cas par cas" -"examen préalable" '
            f'site:{dept_name}.gouv.fr'
        )
    else:
        queries.append(
            f'"{commune}" ("permis de construire" OR "PC") '
            f'stockage batteries -"cas par cas"'
        )
    
    # ========== QUERY 2 : ARRÊTÉ PRÉFECTORAL ==========
    queries.append(
        f'"{commune}" ("arrêté préfectoral" OR "arrêté" OR "autorisation") '
        f'stockage batteries filetype:pdf'
    )
    
    # ========== QUERY 3 : CONSULTATION PUBLIQUE ==========
    queries.append(
        f'"{commune}" ("consultation du public" OR "participation du public") '
        f'"permis" stockage batteries '
        f'site:{dept_name}.gouv.fr'
    )
    
    # ========== QUERY 4 : DÉCISIONS DREAL (moins spécifique) ==========
    queries.append(
        f'"{commune}" stockage batteries "avis" '
        f'site:developpement-durable.gouv.fr -"cas par cas"'
    )
    
    # ========== QUERY 5 : DÉCISION PRÉFECTURE ==========
    if dept:
        queries.append(
            f'"{commune}" permis construire stockage '
            f'site:{dept_name}.gouv.fr'
        )
    
    logger.debug(f"      {len(queries)} requêtes générées")
    return queries


# ═══════════════════════════════════════════════════════════════════════════
# DATACLASS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class PermitData:
    """Données de permis extraites"""
    permit_number: Optional[str] = None
    deposit_date: Optional[str] = None
    issue_date: Optional[str] = None
    consultation_start: Optional[str] = None
    consultation_end: Optional[str] = None
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    confidence: float = 0.0
    summary: str = ""
    
    def to_csv_dict(self, project: Dict) -> Dict:
        return {
            **project,
            "permit_number": self.permit_number,
            "permit_deposit_date": self.deposit_date,
            "permit_issue_date": self.issue_date,
            "permit_consultation_start": self.consultation_start,
            "permit_consultation_end": self.consultation_end,
            "permit_source": self.source_url,
            "permit_source_type": self.source_type,
            "permit_confidence": self.confidence,
            "permit_summary": self.summary
        }


# ═══════════════════════════════════════════════════════════════════════════
# ENRICHISSEUR (VERSION AMÉLIORÉE)
# ═══════════════════════════════════════════════════════════════════════════

class PermitEnricher:
    """Enrichisseur de projets avec données de permis"""
    
    def __init__(self, api_key: str, cse_id: str):
        self.search_client = GoogleCustomSearch(api_key, cse_id)
        self.gemini_client = GeminiClient(api_key)
        self.http_client = HTTPClient(delay=Config.SCRAPE_DELAY)
        self.pdf_cache = PDFMetadataCache(settings.CACHE_DIR)
    
    def enrich(self, project: Dict) -> PermitData:
        """
        VERSION AMÉLIORÉE avec scoring, validation et classification de pages
        """
        title = project.get("project_title", "")
        commune = project.get("commune", "")
        dept = project.get("dept", "")
        demandeur = project.get("demandeur", "")
        year = int(project.get("year", 2023))
        
        logger.info(f"🔍 {title[:50]}... (commune: {commune}, année: {year})")
        
        # ========== ÉTAPE 1 : RECHERCHE GOOGLE ==========
        queries = build_queries(project)
        all_results = []
        
        for query in queries:
            results = self.search_client.search(query, Config.MAX_SEARCH_RESULTS)
            all_results.extend(results)
            time.sleep(Config.SEARCH_DELAY)
        
        if not all_results:
            return PermitData(summary="Aucun résultat Google Search")
        
        # Dédupliquer par URL
        seen_urls = set()
        unique_results = []
        for r in all_results:
            if r["url"] not in seen_urls:
                unique_results.append(r)
                seen_urls.add(r["url"])
        
        logger.info(f"📊 {len(unique_results)} URLs uniques trouvées")
        
        # ========== ÉTAPE 2 : SCORING DES URLs ==========
        scored_urls = []
        for result in unique_results:
            score = score_url_relevance(
                url=result["url"],
                snippet=result.get("snippet", ""),
                title=result.get("title", ""),
                commune=commune,
                dept=dept,
                demandeur=demandeur
            )
            scored_urls.append((score, result))
        
        scored_urls.sort(key=lambda x: (x[0].priority, -x[0].score))
        
        # Log des scores
        logger.info(f"📈 Top 10 URLs par score:")
        for score, result in scored_urls[:10]:
            status = "✓" if score.should_download else "✗"
            logger.info(f"  {status} [P{score.priority}] {score.score:.2f} | {result['url'][:70]}")
            if score.rejection_reason:
                logger.warning(f"        ⚠️  {score.rejection_reason}")
        
        # Filtrer URLs à télécharger
        urls_to_download = [
            (score, result) for score, result in scored_urls
            if score.should_download
        ]
        
        if not urls_to_download:
            return PermitData(summary="Toutes URLs filtrées (scores trop bas)")
        
        logger.info(f"📥 {len(urls_to_download)} URLs à télécharger (score >= {Config.URL_SCORE_THRESHOLD})")
        
        # ========== ÉTAPE 3 : TÉLÉCHARGEMENT INTELLIGENT ==========
        sources_with_content = []
        
        # Phase 1: Haute priorité (P1-P2)
        high_priority = [(s, r) for s, r in urls_to_download if s.priority <= 2][:5]
        
        for score, result in high_priority:
            logger.info(f"📥 [P{score.priority}] {result['url'][:70]}")
            content = self._download_and_extract(result["url"], result.get("title", ""), commune, year)
            
            if content:
                sources_with_content.append({
                    "url": result["url"],
                    "title": result.get("title", ""),
                    "content": content["content"],
                    "page_type": content["page_type"],
                    "score": score.score,
                    "priority": score.priority,
                    "type": SourcePriority.classify_url(result["url"], result.get("title", ""))[1]
                })
        
        # Phase 2: Si <3 sources, élargir à P3
        if len(sources_with_content) < 3:
            medium_priority = [(s, r) for s, r in urls_to_download if s.priority == 3][:3]
            for score, result in medium_priority:
                logger.info(f"📥 [P{score.priority}] {result['url'][:70]} (fallback)")
                content = self._download_and_extract(result["url"], result.get("title", ""), commune, year)
                
                if content:
                    sources_with_content.append({
                        "url": result["url"],
                        "title": result.get("title", ""),
                        "content": content["content"],
                        "page_type": content["page_type"],
                        "score": score.score,
                        "priority": score.priority,
                        "type": SourcePriority.classify_url(result["url"], result.get("title", ""))[1]
                    })
        
        if not sources_with_content:
            return PermitData(summary="Tous téléchargements échoués ou invalidés")
        
        logger.info(f"✅ {len(sources_with_content)} sources valides pour extraction Gemini")
        
        # ========== ÉTAPE 4 : EXTRACTION GEMINI AVEC VALIDATION ==========
        best_result = PermitData()
        
        for source in sources_with_content:
            # Extraction Gemini
            result = self.gemini_client.extract_from_source(
                source,
                commune,
                demandeur,
                year
            )
            
            if result.get("confidence", 0) > best_result.confidence:
                # ✓ NOUVEAU: Validation du numéro PC
                page_type = PageType(source.get("page_type", "autre")) if source.get("page_type") else PageType.AUTRE
                
                permit_number = result.get("permit_number")
                cleaned_pc, pc_confidence_adj, pc_reason = validate_and_clean_permit_number(permit_number, page_type)
                
                if not cleaned_pc and result.get("confidence", 0) > 0.7:
                    logger.warning(f"  ❌ {pc_reason}")
                    continue
                
                # Validation contenu
                is_valid, conf_adjustment, reason = validate_content_match(
                    source["content"],
                    commune,
                    demandeur,
                    min_mentions=Config.MIN_COMMUNE_MENTIONS
                )
                
                if is_valid:
                    # Confiance = combinaison : Gemini × PC validation × contenu
                    base_confidence = result.get("confidence", 0)
                    adjusted_confidence = base_confidence * conf_adjustment * pc_confidence_adj
                    
                    best_result = PermitData(
                        permit_number=cleaned_pc or result.get("permit_number"),
                        deposit_date=result.get("deposit_date"),
                        issue_date=result.get("issue_date"),
                        consultation_start=result.get("consultation_start"),
                        consultation_end=result.get("consultation_end"),
                        source_url=source["url"],
                        source_type=source["type"],
                        confidence=adjusted_confidence,
                        summary=f"Trouvé via {source['type']} | {reason} | {pc_reason}"
                    )
                    
                    logger.info(f"  ✅ Meilleur résultat: {adjusted_confidence:.2f}")
                    logger.debug(f"      PC: {cleaned_pc} | {pc_reason}")
                    
                    # Early stop si très haute confiance
                    if adjusted_confidence >= 0.95:
                        break
                else:
                    logger.warning(f"  ❌ Rejeté après validation: {reason}")
        
        # ========== ÉTAPE 5 : VALIDATION FINALE ==========
        if best_result.confidence > 0:
            permit_dict = {
                "deposit_date": best_result.deposit_date,
                "issue_date": best_result.issue_date,
                "confidence": best_result.confidence,
                "source_url": best_result.source_url
            }
            
            is_valid, warnings = validate_permit_data(project, permit_dict)
            
            for warning in warnings:
                if "❌" in warning:
                    logger.error(warning)
                else:
                    logger.warning(warning)
            
            if not is_valid:
                logger.error(f"❌ Validation finale échouée")
                return PermitData(confidence=0.0, summary="Rejeté validation finale")
        
        # Log final
        details = []
        if best_result.permit_number:
            details.append(f"PC:{best_result.permit_number}")
        if best_result.issue_date:
            details.append(f"Obtenu:{best_result.issue_date}")
        
        status = "✅" if best_result.confidence >= 0.6 else "⚠️"
        logger.info(f"  {status} {best_result.confidence:.2f} - {' | '.join(details) if details else 'N/A'}")
        
        return best_result
    
    def _download_and_extract(self, url: str, title: str, commune: str, year: int) -> Optional[Dict]:
        """
        Télécharge, classifie et extrait le contenu
        
        NOUVEAU: 
        - Classification de page AVANT extraction
        - Rejet automatique pages cas par cas
        - Retourne metadata pour meilleure analyse
        """
        try:
            content = None
            page_type = PageType.AUTRE
            
            if ".pdf" in url.lower():
                # Télécharger PDF
                content = extract_pdf_text(url, title, self.http_client, commune)
                page_type = PageType.AUTRE
                
            else:
                # Scraper HTML
                content = scrape_html(url, self.http_client)
                
                # ✓ NOUVEAU: Classifier la page AVANT d'accepter
                if content and Config.ENABLE_PAGE_CLASSIFICATION:
                    page_type = classify_dreal_page(url, content)
                    
                    # ✗ REJET: Pages cas par cas
                    if page_type == PageType.CAS_PAR_CAS and Config.REJECT_CAS_PAR_CAS_AS_PC:
                        logger.info(f"    ⏭️  REJET: Page cas par cas (pas de PC attendu)")
                        return None
            
            # Validation temporelle
            if content and TemporalFilter.is_relevant(url, content, year):
                logger.debug(f"    ✓ Contenu valide: {len(content)} chars | type: {page_type}")
                return {
                    "content": content,
                    "page_type": page_type,
                    "url": url
                }
            
        except Exception as e:
            logger.debug(f"    ✗ Erreur: {e}")
        
        return None
    
    def close(self):
        self.search_client.close()
        self.gemini_client.close()
        self.http_client.close()


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="🎯 Enrichissement v13.3 - Scoring + Validation + Classification")
    parser.add_argument("--input", required=True, help="Fichier CSV d'entrée")
    parser.add_argument("--output", help="Fichier CSV de sortie (optionnel)")
    parser.add_argument("--limit", type=int, help="Limiter nb de projets")
    parser.add_argument("--debug", action="store_true", help="Mode debug")
    args = parser.parse_args()
    
    configure_logging(args.debug)
    
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"❌ Fichier introuvable: {input_path}")
        sys.exit(1)
    
    if not settings.GOOGLE_API_KEY or not Config.CSE_ID:
        logger.error("❌ Variables d'environnement GOOGLE_API_KEY et GOOGLE_CSE_ID requises")
        sys.exit(1)
    
    output_path = Path(args.output) if args.output else (
        settings.OUTPUT_DIR / "enriched" / (input_path.stem.replace("analyzed", "enriched") + ".csv")
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(input_path, dtype=str).fillna("")
    if args.limit:
        df = df.head(args.limit)
    
    logger.info(f"📂 Entrée: {input_path}")
    logger.info(f"📂 Sortie: {output_path}")
    logger.info(f"📊 Projets: {len(df)}")
    
    enricher = PermitEnricher(settings.GOOGLE_API_KEY, Config.CSE_ID)
    
    try:
        start_time = time.time()
        results = []
        
        for idx, row in df.iterrows():
            logger.info(f"\n{'='*80}")
            logger.info(f"PROJET {idx+1}/{len(df)}")
            
            try:
                permit = enricher.enrich(row.to_dict())
                results.append(permit.to_csv_dict(row.to_dict()))
            except KeyboardInterrupt:
                logger.warning("⚠️ Interruption utilisateur")
                break
            except Exception as e:
                logger.error(f"Erreur projet {idx+1}: {e}", exc_info=args.debug)
                results.append({**row.to_dict(), "permit_confidence": 0.0})
            
            time.sleep(Config.PROJECT_DELAY)
        
        duration = time.time() - start_time
        
        # Sauvegarder résultats
        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False, encoding="utf-8")
        
        # Génération rapport JSON
        report = {
            "metadata": {
                "input_file": str(input_path),
                "output_file": str(output_path),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": round(duration, 1),
                "projects_processed": len(results)
            },
            "statistics": {
                "permits_found": sum(1 for r in results if r.get("permit_number")),
                "high_confidence": sum(1 for r in results if float(r.get("permit_confidence", 0)) >= 0.6),
                "avg_confidence": round(sum(float(r.get("permit_confidence", 0)) for r in results) / len(results), 2) if results else 0
            }
        }
        
        report_path = output_path.parent / (output_path.stem + "_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Statistiques
        found = sum(1 for r in results if r.get("permit_number"))
        high_conf = sum(1 for r in results if float(r.get("permit_confidence", 0)) >= 0.6)
        avg_conf = sum(float(r.get("permit_confidence", 0)) for r in results) / len(results) if results else 0
        
        print(f"\n{'='*80}")
        print(f"✅ RÉSULTATS")
        print(f"{'='*80}")
        print(f"Permis trouvés:      {found}/{len(results)} ({found/len(results)*100:.1f}%)")
        print(f"Confiance ≥ 0.6:     {high_conf} ({high_conf/len(results)*100:.1f}%)")
        print(f"Confiance moyenne:   {avg_conf:.2f}")
        print(f"Durée totale:        {duration/60:.1f} min")
        print(f"Temps/projet:        {duration/len(results):.1f}s")
        print(f"Fichier sauvegardé:  {output_path}")
        print(f"Rapport JSON:        {report_path}")
        print(f"{'='*80}")
        
        logger.info(f"📊 Rapport JSON sauvegardé: {report_path}")
        
    finally:
        enricher.close()


if __name__ == "__main__":
    main()
