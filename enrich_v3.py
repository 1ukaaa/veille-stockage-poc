#!/usr/bin/env python3
"""
ENRICHISSEMENT v3 - Préfectures + Recherche SERP
------------------------------------------------

Évolutions clé:
- Intégration d'un moteur SERP (SerpAPI) mimant les résultats Google classiques
- Fallback Custom Search Engine conservé pour compatibilité
- Pipeline IA Gemini facultatif pour valider les preuves de dépôt détectées
- Architecture prête pour brancher un index local de documents préfectoraux
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse
from collections import defaultdict

import httpx
import pandas as pd
from selectolax.parser import HTMLParser

from config import settings
from utils import (
    HTTPClient,
    HTTPDownloadTooLarge,
    extract_pdf_robust,
    quick_pdf_scan,
)
from validation import get_commune_variations, normalize_commune

logger = logging.getLogger(__name__)


# =============================================================================
# Logging
# =============================================================================

def configure_logging(debug: bool = False) -> None:
    """Configure la sortie console"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# =============================================================================
# Constantes & Config
# =============================================================================

def _slugify(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text.lower())
    cleaned = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    cleaned = (
        cleaned.replace("œ", "oe")
        .replace("'", "")
        .replace("’", "")
        .replace("_", "-")
        .replace(",", "")
    )
    cleaned = re.sub(r"[^a-z0-9-]+", "-", cleaned)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned


DEPARTMENT_NAMES = {
    "01": "Ain",
    "02": "Aisne",
    "03": "Allier",
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "07": "Ardèche",
    "08": "Ardennes",
    "09": "Ariège",
    "10": "Aube",
    "11": "Aude",
    "12": "Aveyron",
    "13": "Bouches-du-Rhône",
    "14": "Calvados",
    "15": "Cantal",
    "16": "Charente",
    "17": "Charente-Maritime",
    "18": "Cher",
    "19": "Corrèze",
    "2A": "Corse-du-Sud",
    "2B": "Haute-Corse",
    "21": "Côte-d'Or",
    "22": "Côtes-d'Armor",
    "23": "Creuse",
    "24": "Dordogne",
    "25": "Doubs",
    "26": "Drôme",
    "27": "Eure",
    "28": "Eure-et-Loir",
    "29": "Finistère",
    "30": "Gard",
    "31": "Haute-Garonne",
    "32": "Gers",
    "33": "Gironde",
    "34": "Hérault",
    "35": "Ille-et-Vilaine",
    "36": "Indre",
    "37": "Indre-et-Loire",
    "38": "Isère",
    "39": "Jura",
    "40": "Landes",
    "41": "Loir-et-Cher",
    "42": "Loire",
    "43": "Haute-Loire",
    "44": "Loire-Atlantique",
    "45": "Loiret",
    "46": "Lot",
    "47": "Lot-et-Garonne",
    "48": "Lozère",
    "49": "Maine-et-Loire",
    "50": "Manche",
    "51": "Marne",
    "52": "Haute-Marne",
    "53": "Mayenne",
    "54": "Meurthe-et-Moselle",
    "55": "Meuse",
    "56": "Morbihan",
    "57": "Moselle",
    "58": "Nièvre",
    "59": "Nord",
    "60": "Oise",
    "61": "Orne",
    "62": "Pas-de-Calais",
    "63": "Puy-de-Dôme",
    "64": "Pyrénées-Atlantiques",
    "65": "Hautes-Pyrénées",
    "66": "Pyrénées-Orientales",
    "67": "Bas-Rhin",
    "68": "Haut-Rhin",
    "69": "Rhône",
    "70": "Haute-Saône",
    "71": "Saône-et-Loire",
    "72": "Sarthe",
    "73": "Savoie",
    "74": "Haute-Savoie",
    "75": "Paris",
    "76": "Seine-Maritime",
    "77": "Seine-et-Marne",
    "78": "Yvelines",
    "79": "Deux-Sèvres",
    "80": "Somme",
    "81": "Tarn",
    "82": "Tarn-et-Garonne",
    "83": "Var",
    "84": "Vaucluse",
    "85": "Vendée",
    "86": "Vienne",
    "87": "Haute-Vienne",
    "88": "Vosges",
    "89": "Yonne",
    "90": "Territoire de Belfort",
    "91": "Essonne",
    "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis",
    "94": "Val-de-Marne",
    "95": "Val-d'Oise",
    "971": "Guadeloupe",
    "972": "Martinique",
    "973": "Guyane",
    "974": "La Réunion",
    "976": "Mayotte",
}


class V2Config:
    """Paramètres spécifiques à la v2"""

    MAX_QUERIES = 18
    RESULTS_PER_QUERY = 12
    MAX_RESULTS_TO_ANALYZE = 28
    MAX_PDF_SIZE_MB = 20
    GOOGLE_REQUEST_DELAY = 0.8

    PERMIT_CONFIDENCE_TARGET = 0.8
    ICPE_CONFIDENCE_TARGET = 0.75

    TEXT_MIN_CHARS = 400
    MAX_HTML_CHARS = 120_000
    AI_TEXT_CHARS = 14_000
    AI_MAX_DOCS = 3
    GEMINI_MODEL = "gemini-2.0-flash"
    GROUND_SEARCH_MODEL = "gemini-2.5-flash"

    # Heuristiques detection
    DEPOSIT_KEYWORDS = [
        "preuve de dépôt",
        "preuve du dépôt",
        "preuve de depôt",
        "dépôt du dossier",
        "déposé le",
        "déposée le",
        "déposés le",
        "accusé de réception",
        "récépissé de dépôt",
        "récépissé de declaration",
        "recu le",
        "reçue le",
        "date de dépôt",
        "dossier enregistré le",
        "transmis le",
        "transmise le",
        "transmission du dossier",
        "dossier transmis",
    ]
    PERMIT_KEYWORDS = [
        "permis de construire",
        "autorisation d'urbanisme",
        "permis modificatif",
        "pc ",
    ]
    ICPE_KEYWORDS = [
        "déclaration icpe",
        "declaration icpe",
        "icpe",
        "rubrique 2925",
        "rubrique 4734",
        "installations classées",
        "classees",
    ]
    BATTERY_KEYWORDS = [
        "batterie",
        "batteries",
        "stockage d'énergie",
        "stockage d’energie",
        "stockage d electricite",
        "station de stockage",
        "bess",
    ]
    CONSULTATION_KEYWORDS = [
        "consultation du public",
        "participation du public",
        "avis au public",
    ]


# =============================================================================
# Modèles de données
# =============================================================================


class SearchIntent(Enum):
    PERMIT = "permit"
    PERMIT_DEPOSIT = "permit_deposit"
    CONSULTATION = "consultation"
    ICPE = "icpe"


class SearchEngineType(Enum):
    AUTO = "auto"
    CSE = "cse"
    SERPAPI = "serpapi"
    GROUND = "ground"


INTENT_SELECTION_ORDER = [
    (SearchIntent.ICPE, 10),
    (SearchIntent.PERMIT, 6),
    (SearchIntent.PERMIT_DEPOSIT, 6),
    (SearchIntent.CONSULTATION, 4),
]


@dataclass
class ProjectContext:
    raw: Dict[str, str]
    project_id: str
    commune: str
    dept: str
    year: Optional[int]
    demandeur: str
    region: str
    title: str

    @property
    def dept_name(self) -> Optional[str]:
        return DEPARTMENT_NAMES.get(self.dept.upper())

    @property
    def dept_slug(self) -> Optional[str]:
        name = self.dept_name
        return _slugify(name) if name else None

    @property
    def prefecture_domains(self) -> List[str]:
        domains = []
        slug = self.dept_slug
        if slug:
            domains.append(f"{slug}.gouv.fr")
            domains.append(f"www.{slug}.gouv.fr")
            domains.append(f"prefecture-{slug}.gouv.fr")
        if self.region:
            domains.append("prefectures-regions.gouv.fr")
            domains.append(f"{_slugify(self.region)}.gouv.fr")
        return list(dict.fromkeys(domains))

    @property
    def commune_variations(self) -> List[str]:
        return get_commune_variations(self.commune) if self.commune else []

    @property
    def title_terms(self) -> List[str]:
        if not self.title:
            return []
        words = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ'’\-]{4,}", self.title)
        cleaned: List[str] = []
        for word in words:
            token = word.strip("-'’").lower()
            if len(token) < 4:
                continue
            if token in {"projet", "stockage", "energie", "énergie", "batteries", "batterie"}:
                continue
            cleaned.append(token)
        deduped: List[str] = []
        for token in cleaned:
            if token not in deduped:
                deduped.append(token)
        return deduped


@dataclass
class SearchQuery:
    query: str
    intent: SearchIntent
    reason: str


@dataclass
class SearchResult:
    url: str
    title: str
    snippet: str
    intent: SearchIntent
    score: float
    reason: str


@dataclass
class DocumentContent:
    url: str
    title: str
    text: str
    intent: SearchIntent
    source_type: str

    def short(self) -> str:
        return f"{self.source_type}:{self.url}"


class EvidenceKind(Enum):
    PERMIT = "permit"
    ICPE = "icpe"


@dataclass
class Evidence:
    kind: EvidenceKind
    url: str
    source_type: str
    confidence: float
    summary: str
    permit_number: Optional[str] = None
    deposit_date: Optional[str] = None
    issue_date: Optional[str] = None
    reference: Optional[str] = None


# =============================================================================
# Utils analyse
# =============================================================================


def _contains_variation(text: str, variations: Iterable[str]) -> bool:
    text_lower = text.lower()
    return any(var in text_lower for var in variations if var)


def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _classify_source(url: str, prefecture_domains: List[str]) -> str:
    domain = _extract_domain(url)
    if any(domain.endswith(d) for d in prefecture_domains):
        return "prefecture"
    if domain.endswith(".gouv.fr"):
        return "gouv"
    if any(token in domain for token in ("mairie", "ville-", "agglo", "cc-")):
        return "collectivite"
    if "prefecture" in domain:
        return "prefecture"
    return "other"


def _clean_html(html: str) -> str:
    parser = HTMLParser(html)
    texts = [node.text(strip=True) for node in parser.css("body")]
    if texts:
        return "\n".join(texts)
    return parser.text(separator="\n") or ""


DATE_NUMERIC = re.compile(
    r"\b(0?[1-9]|[12]\d|3[01])[/\-\.](0?[1-9]|1[0-2])[/\-\.](20\d{2})\b"
)
MONTHS = {
    "janvier": "01",
    "fevrier": "02",
    "février": "02",
    "mars": "03",
    "avril": "04",
    "mai": "05",
    "juin": "06",
    "juillet": "07",
    "aout": "08",
    "août": "08",
    "septembre": "09",
    "octobre": "10",
    "novembre": "11",
    "decembre": "12",
    "décembre": "12",
}
DATE_TEXT = re.compile(
    r"\b(0?[1-9]|[12]\d|3[01])\s+(janvier|février|fevrier|mars|avril|mai|juin|"
    r"juillet|août|aout|septembre|octobre|novembre|décembre|decembre)\s+(20\d{2})\b",
    re.IGNORECASE,
)


def _format_date(day: str, month: str, year: str, textual: bool = False) -> str:
    if textual:
        month = MONTHS.get(month.lower(), "01")
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def _find_date(text: str, keywords: List[str]) -> Optional[str]:
    lower = text.lower()
    for match in DATE_NUMERIC.finditer(text):
        start = match.start()
        window = lower[max(0, start - 80) : start + 80]
        if any(k in window for k in keywords):
            day, month, year = match.groups()
            return _format_date(day, month, year)

    for match in DATE_TEXT.finditer(text):
        start = match.start()
        window = lower[max(0, start - 80) : start + 80]
        if any(k in window for k in keywords):
            day, month, year = match.groups()
            return _format_date(day, month, year, textual=True)
    return None


PC_PATTERNS = [
    re.compile(r"\bPC\s?(\d{2,3})\s?(\d{3})\s?(\d{2})\s?(\d{5})\b", re.IGNORECASE),
    re.compile(r"\b(\d{2,3})\s?(\d{3})\s?(\d{2})\s?(\d{5})\b"),
    re.compile(r"\bPC\d{13}\b", re.IGNORECASE),
]
CAS_PAR_CAS_PATTERN = re.compile(r"20\d{2}-[A-Z]{2,3}-[A-Z]{3}-\d{3,5}", re.IGNORECASE)


def _extract_permit_number(text: str) -> Optional[str]:
    if CAS_PAR_CAS_PATTERN.search(text):
        return None
    for pattern in PC_PATTERNS:
        match = pattern.search(text)
        if match:
            groups = match.groups()
            if len(groups) == 4:
                return f"PC {groups[0]} {groups[1]} {groups[2]} {groups[3]}"
            return match.group(0).upper()
    return None


ICPE_REF_PATTERN = re.compile(
    r"\b20\d{2}-[A-Z]{2,3}-[A-Z]{3}-\d{3,5}\b|\bICPE\s?\d{4,}\b|\b[A-Z]-\d-[A-Z0-9]{5,}\b",
    re.IGNORECASE,
)

PERMIT_DEPOSIT_HINTS = [
    "dépôt",
    "depot",
    "déposé",
    "déposée",
    "reception",
    "réception",
    "transmis",
    "transmise",
    "preuve",
    "enregistré",
    "reçu",
]

ICPE_DEPOSIT_HINTS = [
    "dépôt",
    "depot",
    "reçu",
    "recus",
    "récépissé",
    "recepisse",
    "transmis",
    "transmise",
    "preuve",
    "enregistré",
]


def _extract_icpe_reference(text: str) -> Optional[str]:
    match = ICPE_REF_PATTERN.search(text)
    if match:
        return match.group(0)
    return None


def _count_keywords(text: str, keywords: List[str]) -> int:
    lower = text.lower()
    return sum(1 for kw in keywords if kw in lower)


def _has_battery_context(text: str) -> bool:
    return _count_keywords(text, V2Config.BATTERY_KEYWORDS) > 0


# =============================================================================
# Recherche
# =============================================================================


class SearchQueryBuilder:
    """Génère des requêtes ciblées"""

    def build(self, ctx: ProjectContext) -> List[SearchQuery]:
        queries: List[SearchQuery] = []

        # Requêtes standard sur la commune, l'année, le demandeur
        base_terms = [
            f"\"{ctx.commune}\" permis de construire batterie",
            f"\"{ctx.commune}\" \"permis de construire\" stockage énergie",
        ]
        if ctx.year:
            base_terms.append(
                f"\"{ctx.commune}\" \"permis de construire\" {ctx.year} batterie"
            )
        if ctx.demandeur:
            base_terms.append(
                f"\"{ctx.demandeur}\" \"permis de construire\" \"{ctx.commune}\""
            )
        for q in base_terms:
            queries.append(SearchQuery(query=q, intent=SearchIntent.PERMIT, reason="base_permit"))

        # Préfecture sites - classique
        for domain in ctx.prefecture_domains[:3]:
            queries.append(SearchQuery(
                query=f"site:{domain} \"{ctx.commune}\" \"permis de construire\"",
                intent=SearchIntent.PERMIT_DEPOSIT,
                reason=f"prefecture_pc_{domain}",
            ))
            queries.append(SearchQuery(
                query=f"site:{domain} \"{ctx.commune}\" \"déclaration ICPE\" batterie",
                intent=SearchIntent.ICPE,
                reason=f"prefecture_icpe_{domain}",
            ))
            queries.append(SearchQuery(
                query=f"site:{domain} \"{ctx.commune}\" \"preuve de dépôt\"",
                intent=SearchIntent.ICPE,
                reason=f"prefecture_preuve_{domain}",
            ))

        # Sur le demandeur
        if ctx.demandeur:
            queries.append(SearchQuery(
                query=f"\"{ctx.demandeur}\" \"accusé de réception\" permis",
                intent=SearchIntent.PERMIT_DEPOSIT,
                reason="demandeur_receipt",
            ))
            queries.append(SearchQuery(
                query=f"\"{ctx.demandeur}\" \"preuve de dépôt\" ICPE",
                intent=SearchIntent.ICPE,
                reason="demandeur_preuve_icpe",
            ))

        queries.append(SearchQuery(
            query=f"\"{ctx.commune}\" \"participation du public\" batteries",
            intent=SearchIntent.CONSULTATION,
            reason="consultation_commune",
        ))
        queries.append(SearchQuery(
            query=f"\"{ctx.commune}\" \"récépissé\" \"déclaration ICPE\"",
            intent=SearchIntent.ICPE,
            reason="icpe_receipt",
        ))
        if ctx.commune:
            queries.append(SearchQuery(
                query=f"\"{ctx.commune}\" \"preuve de dépôt\" ICPE",
                intent=SearchIntent.ICPE,
                reason="commune_preuve_icpe",
            ))

        # Nouveauté : Requêtes systématiques sur project_title (exact + title_terms pertinents)
        if ctx.title.strip():
            queries.append(SearchQuery(
                query=f"\"{ctx.title}\" \"dépôt\" ICPE",
                intent=SearchIntent.ICPE,
                reason="title_long_preuve",
            ))

        # Sur tous les title_terms (jusqu'à 5), en combinant pour maximiser la couverture
        for term in ctx.title_terms[:5]:
            queries.append(SearchQuery(
                query=f"\"{term}\" \"preuve de dépôt\" ICPE",
                intent=SearchIntent.ICPE,
                reason=f"title_preuve_{term}",
            ))
            if ctx.prefecture_domains:
                queries.append(SearchQuery(
                    query=f"site:{ctx.prefecture_domains[0]} \"{term}\" ICPE",
                    intent=SearchIntent.ICPE,
                    reason=f"title_pref_{term}",
                ))
        
        # Combo de title_terms (croisements pour attraper les cas abrégés ou acronymes)
        for i, term1 in enumerate(ctx.title_terms[:4]):
            for term2 in ctx.title_terms[i+1:5]:
                queries.append(SearchQuery(
                    query=f"\"{term1}\" \"{term2}\" \"dépôt\" ICPE",
                    intent=SearchIntent.ICPE,
                    reason=f"title_combo_{term1}_{term2}",
                ))

        # Limiter / dédupliquer (déjà dans la logique initiale)
        seen = set()
        final_queries = []
        for query in queries:
            if query.query not in seen:
                seen.add(query.query)
                final_queries.append(query)
            if len(final_queries) >= V2Config.MAX_QUERIES:
                break

        return final_queries


class GoogleCustomSearchClient:
    """Client simple pour GCS"""

    def __init__(self, api_key: str, cx: str):
        self.api_key = api_key
        self.cx = cx
        self.client = httpx.Client(timeout=45.0)

    def search(self, query: str, max_results: int) -> List[Dict]:
        items: List[Dict] = []
        start = 1
        while len(items) < max_results and start <= 90:
            params = {
                "key": self.api_key,
                "cx": self.cx,
                "q": query,
                "num": min(10, max_results - len(items)),
                "start": start,
            }
            try:
                response = self.client.get(
                    "https://www.googleapis.com/customsearch/v1", params=params
                )
                response.raise_for_status()
                chunk = response.json().get("items", [])
                if not chunk:
                    break
                items.extend(chunk)
                start += 10
                time.sleep(V2Config.GOOGLE_REQUEST_DELAY)
            except Exception as exc:
                logger.warning(f"Search error for '{query}': {exc}")
                break
        return items[:max_results]

    def close(self):
        self.client.close()


class SerpApiClient:
    """Client SerpAPI pour obtenir la SERP Google standard"""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("SERPAPI_KEY requis pour l'utilisation du moteur serpapi")
        self.api_key = api_key
        self.client = httpx.Client(timeout=45.0)

    def search(self, query: str, max_results: int) -> List[Dict]:
        params = {
            "engine": "google",
            "q": query,
            "hl": "fr",
            "google_domain": "google.fr",
            "num": min(20, max_results),
            "api_key": self.api_key,
        }
        try:
            response = self.client.get("https://serpapi.com/search", params=params)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.warning(f"SerpAPI error '{query}': {exc}")
            return []

        organic = payload.get("organic_results", [])
        results: List[Dict] = []
        for item in organic:
            results.append(
                {
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "snippet": item.get("snippet")
                    or " ".join(item.get("snippet_highlighted_words", [])),
                }
            )
            if len(results) >= max_results:
                break
        return results

    def close(self):
        self.client.close()


class GeminiGroundSearchClient:
    """Recherche via Gemini + Google Grounding"""

    def __init__(self, api_key: str, model: str = V2Config.GROUND_SEARCH_MODEL):
        if not api_key:
            raise ValueError("GEMINI_API_KEY requis pour la recherche ground")
        self.api_key = api_key
        self.model = model
        self.client = httpx.Client(timeout=90)

    def close(self):
        self.client.close()

    def search(self, query: str, max_results: int) -> List[Dict]:
        prompt = (
            "Utilise uniquement l'outil google_search pour trouver des documents officiels (.gouv.fr, .developpement-durable.gouv.fr, préfets) "
            f"qui répondent à la requête suivante: {query}. "
            f"Retourne STRICTEMENT un JSON de la forme "
            f"[{{\"title\":\"...\",\"url\":\"...\",\"snippet\":\"...\"}}, ...] avec au plus {max_results} éléments. "
            "Chaque URL doit être unique et pointer vers un document ou article administratif."
        )
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "tools": [{"google_search": {}}],
            "tool_config": {"google_search": {"max_results": max(4, max_results)}},
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": 1024,
                "responseMimeType": "application/json",
            },
        }
        try:
            resp = self.client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent",
                params={"key": self.api_key},
                json=payload,
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning(f"Gemini ground search error '{query}': {exc}")
            return []

        text = self._extract_text(resp.json())
        if not text:
            return []
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.debug("Gemini ground search JSON invalide")
            return []

        results: List[Dict] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            url = item.get("url")
            if not url:
                continue
            results.append(
                {
                    "title": item.get("title", ""),
                    "url": url,
                    "snippet": item.get("snippet", ""),
                }
            )
            if len(results) >= max_results:
                break
        return results

    @staticmethod
    def _extract_text(payload: Dict) -> str:
        for candidate in payload.get("candidates", []):
            for part in candidate.get("content", {}).get("parts", []):
                if "text" in part:
                    return part["text"]
        return ""


class CandidateScorer:
    """Score heuristique des résultats"""

    def __init__(self, ctx: ProjectContext):
        self.ctx = ctx

    def score(self, result: Dict, intent: SearchIntent, reason: str) -> Optional[SearchResult]:
        url = result.get("link", "")
        title = result.get("title", "")
        snippet = result.get("snippet", "")
        if not url:
            return None

        domain = _extract_domain(url)
        url_lower = url.lower()
        score = 0.0
        if any(domain.endswith(d) for d in self.ctx.prefecture_domains):
            score += 0.45
        elif domain.endswith(".gouv.fr"):
            score += 0.35
        elif "gouv" in domain or "prefecture" in domain:
            score += 0.25

        text = " ".join([title.lower(), snippet.lower()])
        if _contains_variation(text, self.ctx.commune_variations):
            score += 0.2
        if self.ctx.demandeur and self.ctx.demandeur.lower() in text:
            score += 0.1

        # Bonus si URL clairement documentaire/PDF
        if url_lower.endswith(".pdf") or any(
            token in url_lower for token in ("/download/", "/telechargement/", "/file/")
        ):
            score += 0.15

        if intent in (SearchIntent.PERMIT, SearchIntent.PERMIT_DEPOSIT):
            score += 0.15 * _count_keywords(text, V2Config.PERMIT_KEYWORDS)
        if intent == SearchIntent.ICPE:
            score += 0.15 * _count_keywords(text, V2Config.ICPE_KEYWORDS)
        if intent == SearchIntent.CONSULTATION:
            score += 0.1 * _count_keywords(text, V2Config.CONSULTATION_KEYWORDS)

        deposit_hits = _count_keywords(text, V2Config.DEPOSIT_KEYWORDS)
        if deposit_hits:
            score += min(0.08 * deposit_hits, 0.24)

        # Malus pour les pages génériques (navigation AngularJS)
        if "#!/" in url or "/vous-etes/" in url_lower:
            score -= 0.25
        if "particuliers" in url_lower and deposit_hits == 0 and intent != SearchIntent.CONSULTATION:
            score -= 0.1

        return SearchResult(
            url=url,
            title=title,
            snippet=snippet,
            intent=intent,
            score=max(0.0, min(score, 1.0)),
            reason=reason,
        )


class DocumentFetcher:
    """Télécharge et nettoie le contenu"""

    def __init__(self, http_client: HTTPClient):
        self.http_client = http_client

    def fetch(self, result: SearchResult, ctx: ProjectContext) -> Optional[DocumentContent]:
        url = result.url
        try:
            if url.lower().endswith(".pdf"):
                return self._fetch_pdf(result, ctx)
            text, final_url = self.http_client.get_text(url)
            html_text = _clean_html(text)[: V2Config.MAX_HTML_CHARS]
            if len(html_text) < V2Config.TEXT_MIN_CHARS:
                logger.debug(f" Document trop court ({len(html_text)} chars) - {url}")
                return None
            source_type = _classify_source(final_url, ctx.prefecture_domains)
            return DocumentContent(
                url=final_url,
                title=result.title,
                text=html_text,
                intent=result.intent,
                source_type=source_type,
            )
        except Exception as exc:
            logger.debug(f"Fetch failed for {url}: {exc}")
            return None

    def _fetch_pdf(self, result: SearchResult, ctx: ProjectContext) -> Optional[DocumentContent]:
        try:
            pdf_bytes, final_url = self.http_client.get_bytes(
                result.url, max_bytes=V2Config.MAX_PDF_SIZE_MB * 1024 * 1024
            )
        except HTTPDownloadTooLarge:
            logger.debug(f" PDF trop volumineux: {result.url}")
            return None
        except Exception as exc:
            logger.debug(f" Download PDF échec ({result.url}): {exc}")
            return None

        # 1. Scan rapide : commune sur 2 pages max
        if quick_pdf_scan(pdf_bytes, ctx.commune):
            text, method = extract_pdf_robust(pdf_bytes)
            if not text or len(text) < V2Config.TEXT_MIN_CHARS:
                logger.debug(f" PDF peu exploitable ({method})")
                return None
            source_type = _classify_source(result.url, ctx.prefecture_domains)
            return DocumentContent(
                url=result.url,
                title=result.title,
                text=text[: V2Config.MAX_HTML_CHARS],
                intent=result.intent,
                source_type=source_type,
            )

        # 2. Fallback : URL/file très probable → scan complet
        project_keywords = [ctx.commune.lower(), ctx.demandeur.lower(), "icpe", "stockage", "batterie"]
        full_url_match = any(kw and kw in result.url.lower() for kw in project_keywords)
        if not full_url_match and ctx.title_terms:
            for tkw in ctx.title_terms:
                if tkw and tkw in result.url.lower():
                    full_url_match = True
                    break

        if full_url_match:
            text, method = extract_pdf_robust(pdf_bytes)
            if not text or len(text) < V2Config.TEXT_MIN_CHARS:
                logger.debug(f" PDF peu exploitable ({method}) [fallback]")
                return None
            commune_in_text = ctx.commune.lower() in text.lower()
            deposit_kw = any(kw in text.lower() for kw in V2Config.DEPOSIT_KEYWORDS)
            if commune_in_text or deposit_kw:
                source_type = _classify_source(result.url, ctx.prefecture_domains)
                logger.debug(f" PDF accepté par fallback commune_in_text={commune_in_text} deposit_kw={deposit_kw}")
                return DocumentContent(
                    url=result.url,
                    title=result.title,
                    text=text[: V2Config.MAX_HTML_CHARS],
                    intent=result.intent,
                    source_type=source_type,
                )
            else:
                logger.debug(f" PDF ignoré : fallback mais pas de commune/preuve dans texte [{result.url}]")
                return None

        return None


class GeminiVerifier:
    """Analyse IA pour confirmer qu'un document est une preuve pertinente"""

    def __init__(self, api_key: str, model: str = V2Config.GEMINI_MODEL):
        if not api_key:
            raise ValueError("Gemini API key manquante")
        self.api_key = api_key
        self.model = model
        self.client = httpx.Client(timeout=90)

    def close(self):
        self.client.close()

    def analyze(self, ctx: ProjectContext, document: DocumentContent) -> List[Evidence]:
        text = document.text[: V2Config.AI_TEXT_CHARS]
        if not text:
            return []

        prompt = self._build_prompt(ctx, document, text)
        try:
            response = self.client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent",
                params={"key": self.api_key},
                json={
                    "contents": [
                        {
                            "role": "user",
                            "parts": [{"text": prompt}],
                        }
                    ],
                    "generationConfig": {
                        "temperature": 0.1,
                        "maxOutputTokens": 1024,
                        "responseMimeType": "application/json",
                    },
                },
            )
            response.raise_for_status()
        except Exception as exc:
            logger.debug(f"AI call failed: {exc}")
            return []

        raw = self._extract_text(response.json())
        if not raw:
            return []
        data = self._safe_json(raw)
        if not isinstance(data, list):
            return []

        evidences: List[Evidence] = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            ev_type = (entry.get("type") or "").upper()
            confidence = float(entry.get("confidence") or 0.0)
            summary = entry.get("summary") or entry.get("explanation") or "Analyse IA"
            deposit_date = entry.get("deposit_date")
            issue_date = entry.get("issue_date")
            reference = entry.get("reference") or entry.get("icpe_reference")
            permit_number = entry.get("permit_number")
            commune_match = bool(entry.get("commune_match", False))

            if ev_type == "ICPE_DEPOSIT" and confidence > 0:
                evidences.append(
                    Evidence(
                        kind=EvidenceKind.ICPE,
                        url=document.url,
                        source_type=document.source_type,
                        confidence=min(1.0, confidence + (0.05 if commune_match else 0)),
                        summary=summary,
                        deposit_date=deposit_date,
                        reference=reference,
                    )
                )
            elif ev_type == "PERMIT" and confidence > 0:
                evidences.append(
                    Evidence(
                        kind=EvidenceKind.PERMIT,
                        url=document.url,
                        source_type=document.source_type,
                        confidence=min(1.0, confidence + (0.05 if commune_match else 0)),
                        summary=summary,
                        permit_number=permit_number,
                        deposit_date=deposit_date,
                        issue_date=issue_date,
                    )
                )

        return evidences

    def _build_prompt(self, ctx: ProjectContext, document: DocumentContent, text: str) -> str:
        return (
            "Tu es un contrôleur juridique des installations classées. "
            "Détecte si le document ci-dessous constitue une preuve de dépôt ICPE (ou un permis de construire) "
            "pour le projet cible. Réponds avec un JSON STRICT : liste d'objets.\n\n"
            "Format attendu pour chaque élément:\n"
            "{\n"
            '  "type": "ICPE_DEPOSIT" | "PERMIT" | "OTHER",\n'
            '  "confidence": 0.0-1.0,\n'
            '  "commune_match": true/false,\n'
            '  "applicant_match": true/false,\n'
            '  "reference": "texte ou null",\n'
            '  "permit_number": "texte ou null",\n'
            '  "deposit_date": "YYYY-MM-DD ou null",\n'
            '  "issue_date": "YYYY-MM-DD ou null",\n'
            '  "summary": "phrase courte"\n'
            "}\n\n"
            f"PROJET CIBLE:\n"
            f"- Titre: {ctx.title}\n"
            f"- Commune: {ctx.commune} ({ctx.dept})\n"
            f"- Demandeur: {ctx.demandeur}\n"
            f"- Année cible: {ctx.year}\n\n"
            f"DOCUMENT: {document.url}\n"
            f"Type source: {document.source_type}\n"
            f"Contenu (tronqué):\n{text}\n"
        )

    @staticmethod
    def _extract_text(payload: Dict) -> str:
        parts = []
        for candidate in payload.get("candidates", []):
            content = candidate.get("content", {})
            for part in content.get("parts", []):
                text = part.get("text")
                if text:
                    parts.append(text)
        return "\n".join(parts).strip()

    @staticmethod
    def _safe_json(raw: str):
        raw = raw.strip()
        if not raw:
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            match = re.search(r"(\[.*\])", raw, re.S)
            if match:
                try:
                    return json.loads(match.group(1))
                except json.JSONDecodeError:
                    return []
        return []



class EvidenceAnalyzer:
    """Transforme un document en éventuelles preuves"""

    def __init__(self, ctx: ProjectContext):
        self.ctx = ctx

    def analyze(self, content: DocumentContent) -> List[Evidence]:
        evidences: List[Evidence] = []
        text = content.text
        text_lower = text.lower()
        commune_ok = (
            True if not self.ctx.commune_variations else _contains_variation(text_lower, self.ctx.commune_variations)
        )
        deposit_mentions = _count_keywords(text_lower, V2Config.DEPOSIT_KEYWORDS)
        has_proof_phrase = "preuve de dépôt" in text_lower or "preuve du dépôt" in text_lower

        if not commune_ok and content.source_type not in ("prefecture", "gouv"):
            return evidences

        if any(keyword in text_lower for keyword in V2Config.PERMIT_KEYWORDS):
            deposit_hit = deposit_mentions > 0
            permit_number = _extract_permit_number(text)
            deposit_date = _find_date(text, PERMIT_DEPOSIT_HINTS)
            issue_date = _find_date(text, ["délivrance", "délivré", "décision", "accordé"])
            has_battery = _has_battery_context(text_lower)
            if deposit_hit or permit_number:
                confidence = 0.3
                if content.source_type == "prefecture":
                    confidence += 0.35
                elif content.source_type == "gouv":
                    confidence += 0.25
                if commune_ok:
                    confidence += 0.1
                if deposit_hit:
                    confidence += 0.15
                if has_proof_phrase:
                    confidence += 0.05
                if permit_number:
                    confidence += 0.15
                if deposit_date:
                    confidence += 0.1
                if issue_date:
                    confidence += 0.1
                if has_battery:
                    confidence += 0.05
                summary_parts = []
                if permit_number:
                    summary_parts.append(f"PC {permit_number}")
                if deposit_date:
                    summary_parts.append(f"dépôt {deposit_date}")
                if issue_date:
                    summary_parts.append(f"délivrance {issue_date}")
                if deposit_hit:
                    summary_parts.append("preuve dépôt détectée")
                summary = ", ".join(summary_parts) or "Mention permis détectée"
                evidences.append(
                    Evidence(
                        kind=EvidenceKind.PERMIT,
                        url=content.url,
                        source_type=content.source_type,
                        confidence=min(confidence, 1.0),
                        summary=summary,
                        permit_number=permit_number,
                        deposit_date=deposit_date,
                        issue_date=issue_date,
                    )
                )

        icpe_context = (
            "icpe" in text_lower
            or "installations classées" in text_lower
            or "installations classees" in text_lower
            or "déclaration icpe" in text_lower
            or "declaration icpe" in text_lower
            or content.intent == SearchIntent.ICPE
        )
        if icpe_context:
            icpe_reference = _extract_icpe_reference(text)
            if not icpe_reference:
                ref_match = re.search(
                    r"référence de (?:votre|ce) dossier est ([A-Z0-9\-]+)", text, re.IGNORECASE
                )
                if ref_match:
                    icpe_reference = ref_match.group(1)
            deposit_hit = deposit_mentions > 0
            has_battery = _has_battery_context(text_lower)
            if icpe_reference or (deposit_hit and icpe_context):
                confidence = 0.35
                if content.source_type == "prefecture":
                    confidence += 0.35
                elif content.source_type == "gouv":
                    confidence += 0.2
                if commune_ok:
                    confidence += 0.1
                if deposit_hit:
                    confidence += 0.1
                if has_proof_phrase:
                    confidence += 0.1
                if has_battery:
                    confidence += 0.1
                deposit_date = _find_date(text, ICPE_DEPOSIT_HINTS)
                if deposit_date:
                    confidence += 0.1
                summary_parts = []
                if icpe_reference:
                    summary_parts.append(icpe_reference)
                if deposit_date:
                    summary_parts.append(f"dépôt {deposit_date}")
                if deposit_hit:
                    summary_parts.append("récépissé ICPE mentionné")
                summary = ", ".join(summary_parts) or "Déclaration ICPE détectée"
                evidences.append(
                    Evidence(
                        kind=EvidenceKind.ICPE,
                        url=content.url,
                        source_type=content.source_type,
                        confidence=min(confidence, 1.0),
                        summary=summary,
                        reference=icpe_reference,
                        deposit_date=deposit_date,
                    )
                )

        return evidences


class EvidenceAccumulator:
    """Garde les meilleures preuves rencontrées"""

    def __init__(self):
        self.permit: Optional[Evidence] = None
        self.icpe: Optional[Evidence] = None

    def consider(self, evidence: Evidence) -> None:
        if evidence.kind == EvidenceKind.PERMIT:
            if not self.permit or evidence.confidence > self.permit.confidence:
                self.permit = evidence
        elif evidence.kind == EvidenceKind.ICPE:
            if not self.icpe or evidence.confidence > self.icpe.confidence:
                self.icpe = evidence

    def satisfied(self) -> bool:
        permit_ok = (
            self.permit and self.permit.confidence >= V2Config.PERMIT_CONFIDENCE_TARGET
        )
        icpe_ok = self.icpe and self.icpe.confidence >= V2Config.ICPE_CONFIDENCE_TARGET
        return permit_ok and icpe_ok

    def to_row(self) -> Dict[str, str]:
        def safe(value: Optional[str]) -> str:
            return value if value else ""

        row = {
            "permit_number": safe(self.permit.permit_number) if self.permit else "",
            "permit_deposit_date": safe(self.permit.deposit_date) if self.permit else "",
            "permit_issue_date": safe(self.permit.issue_date) if self.permit else "",
            "permit_consultation_start": "",
            "permit_consultation_end": "",
            "permit_source": safe(self.permit.url) if self.permit else "",
            "permit_source_type": self.permit.source_type if self.permit else "",
            "permit_confidence": f"{self.permit.confidence:.2f}" if self.permit else "0.00",
            "permit_summary": safe(self.permit.summary) if self.permit else "",
            "icpe_deposit_date": safe(self.icpe.deposit_date) if self.icpe else "",
            "icpe_reference": safe(self.icpe.reference) if self.icpe else "",
            "icpe_source": safe(self.icpe.url) if self.icpe else "",
            "icpe_confidence": f"{self.icpe.confidence:.2f}" if self.icpe else "0.00",
            "icpe_summary": safe(self.icpe.summary) if self.icpe else "",
        }
        return row


# =============================================================================
# Orchestrateur
# =============================================================================


class PrefecturePermitEnricherV3:
    def __init__(
        self,
        api_key: str,
        cse_id: str,
        use_ai: bool = False,
        ai_max_docs: Optional[int] = None,
        gemini_api_key: Optional[str] = None,
        search_engine: SearchEngineType = SearchEngineType.AUTO,
        serpapi_key: Optional[str] = None,
        serpapi_max_queries: int = 2,
        ground_api_key: Optional[str] = None,
        ground_model: str = V2Config.GROUND_SEARCH_MODEL,
    ):
        if not api_key or not cse_id:
            raise ValueError("GOOGLE_API_KEY et GOOGLE_CSE_ID requis")
        self.search_engine = search_engine
        self.cse_client: Optional[GoogleCustomSearchClient] = None
        self.serp_client: Optional[SerpApiClient] = None
        self.ground_client: Optional[GeminiGroundSearchClient] = None
        if search_engine == SearchEngineType.CSE:
            self.cse_client = GoogleCustomSearchClient(api_key, cse_id)
        elif search_engine == SearchEngineType.SERPAPI:
            self.serp_client = SerpApiClient(serpapi_key or os.getenv("SERPAPI_KEY", ""))
        elif search_engine == SearchEngineType.GROUND:
            ground_key = ground_api_key or os.getenv("GEMINI_SEARCH_KEY") or os.getenv("GEMINI_API_KEY")
            self.ground_client = GeminiGroundSearchClient(ground_key, ground_model)
        else:  # AUTO => SerpAPI pour les requêtes critiques + CSE pour le reste
            self.cse_client = GoogleCustomSearchClient(api_key, cse_id)
            serp_key = serpapi_key or os.getenv("SERPAPI_KEY")
            ground_key = ground_api_key or os.getenv("GEMINI_SEARCH_KEY") or os.getenv("GEMINI_API_KEY")
            if ground_key:
                logger.info("🌐 Gemini Ground Search activé (mode auto)")
                self.ground_client = GeminiGroundSearchClient(ground_key, ground_model)
            elif serp_key:
                logger.info("🔎 SERPAPI disponible : utilisation combinée (prioritaire sur 2 requêtes)")
                self.serp_client = SerpApiClient(serp_key)

        if not (self.cse_client or self.serp_client or self.ground_client):
            raise ValueError("Aucun moteur de recherche disponible (CSE, SerpAPI ou Gemini Ground)")

        self.http_client = HTTPClient(delay=settings.RATE_LIMIT)
        self.ai_max_docs = ai_max_docs or V2Config.AI_MAX_DOCS
        self.serp_max_queries = max(0, serpapi_max_queries)
        self.serp_queries_used = 0
        self.ai_verifier: Optional[GeminiVerifier] = None
        if use_ai:
            key = gemini_api_key or os.getenv("GEMINI_API_KEY") or api_key
            try:
                self.ai_verifier = GeminiVerifier(key, V2Config.GEMINI_MODEL)
                logger.info("🤖 Vérification Gemini activée")
            except Exception as exc:
                logger.error(f"Impossible d'initialiser Gemini: {exc}")
                self.ai_verifier = None

    def enrich_project(self, project_row: Dict[str, str]) -> Dict[str, str]:
        self.serp_queries_used = 0
        ctx = ProjectContext(
            raw=project_row,
            project_id=project_row.get("project_id", ""),
            commune=project_row.get("commune", ""),
            dept=(project_row.get("dept") or "").upper(),
            year=self._safe_int(project_row.get("year")),
            demandeur=project_row.get("demandeur", ""),
            region=project_row.get("region", ""),
            title=project_row.get("project_title", ""),
        )

        query_builder = SearchQueryBuilder()
        scorer = CandidateScorer(ctx)
        fetcher = DocumentFetcher(self.http_client)
        analyzer = EvidenceAnalyzer(ctx)
        accumulator = EvidenceAccumulator()
        ai_calls = 0

        queries = query_builder.build(ctx)
        logger.info(f"🔎 {ctx.project_id} - {ctx.commune} | {len(queries)} requêtes")

        seen_urls = set()
        ordered_results: List[SearchResult] = []

        # Priorité: requêtes Ground ou SerpAPI limitées (titre complet + combinaison commune/demandeur)
        if (self.serp_client or self.ground_client) and self.serp_max_queries > 0:
            serp_queries = self._build_serp_priority_queries(ctx)
            for query in serp_queries:
                raw_results = self._priority_search(query.query, V2Config.RESULTS_PER_QUERY)
                if not raw_results:
                    continue
                for raw in raw_results:
                    scored = CandidateScorer(ctx).score(raw, query.intent, query.reason)
                    if not scored or scored.url in seen_urls:
                        continue
                    seen_urls.add(scored.url)
                    ordered_results.append(scored)
                if self.serp_queries_used >= self.serp_max_queries:
                    break
        scorer = CandidateScorer(ctx)
        for query in queries:
            raw_results = self._search_general(query.query, V2Config.RESULTS_PER_QUERY)
            for raw in raw_results:
                scored = scorer.score(raw, query.intent, query.reason)
                if not scored:
                    continue
                if scored.url in seen_urls:
                    continue
                seen_urls.add(scored.url)
                ordered_results.append(scored)

        ordered_results.sort(key=lambda r: r.score, reverse=True)
        to_analyze = self._select_candidates(ordered_results)
        logger.info(f"  ➤ {len(to_analyze)} résultats retenus")

        for idx, result in enumerate(to_analyze, start=1):
            logger.info(f"    [{idx}/{len(to_analyze)}] {result.score:.2f} - {result.url}")
            content = fetcher.fetch(result, ctx)
            if not content:
                continue
            evidences = analyzer.analyze(content)
            for evidence in evidences:
                accumulator.consider(evidence)
                logger.info(
                    f"      {evidence.kind.value.upper()} {evidence.confidence:.2f} - {evidence.summary}"
                )
            if (
                self.ai_verifier
                and ai_calls < self.ai_max_docs
                and (not evidences or accumulator.icpe is None)
            ):
                ai_results = self.ai_verifier.analyze(ctx, content)
                if ai_results:
                    ai_calls += 1
                    for evidence in ai_results:
                        accumulator.consider(evidence)
                        logger.info(
                            f"      🤖 {evidence.kind.value.upper()} {evidence.confidence:.2f} - {evidence.summary}"
                        )
            if accumulator.satisfied():
                break

        return {**project_row, **accumulator.to_row()}

    @staticmethod
    def _safe_int(value: Optional[str]) -> Optional[int]:
        try:
            return int(value) if value else None
        except ValueError:
            return None

    def close(self):
        if self.cse_client:
            self.cse_client.close()
        if self.serp_client:
            self.serp_client.close()
        if self.ground_client:
            self.ground_client.close()
        self.http_client.close()
        if self.ai_verifier:
            self.ai_verifier.close()

    def _select_candidates(self, ordered_results: List[SearchResult]) -> List[SearchResult]:
        if not ordered_results:
            return []

        buckets: Dict[SearchIntent, List[SearchResult]] = defaultdict(list)
        for result in ordered_results:
            buckets[result.intent].append(result)

        for bucket in buckets.values():
            bucket.sort(key=lambda r: r.score, reverse=True)

        selected: List[SearchResult] = []
        seen_urls = set()

        def take(intent: SearchIntent, quota: int):
            bucket = buckets.get(intent, [])
            count = 0
            while (
                bucket
                and count < quota
                and len(selected) < V2Config.MAX_RESULTS_TO_ANALYZE
            ):
                candidate = bucket.pop(0)
                if candidate.url in seen_urls:
                    continue
                selected.append(candidate)
                seen_urls.add(candidate.url)
                count += 1

        for intent, quota in INTENT_SELECTION_ORDER:
            take(intent, quota)

        if len(selected) < V2Config.MAX_RESULTS_TO_ANALYZE:
            for candidate in ordered_results:
                if candidate.url in seen_urls:
                    continue
                selected.append(candidate)
                seen_urls.add(candidate.url)
                if len(selected) >= V2Config.MAX_RESULTS_TO_ANALYZE:
                    break

        return selected

    def _search_general(self, query: str, max_results: int) -> List[Dict]:
        if self.ground_client:
            ground_results = self.ground_client.search(query, max_results)
            if ground_results:
                return ground_results
        if self.cse_client:
            cse_results = self.cse_client.search(query, max_results)
            if cse_results:
                return cse_results
        return self._serp_search(query, max_results)

    def _priority_search(self, query: str, max_results: int) -> List[Dict]:
        if self.ground_client:
            results = self.ground_client.search(query, max_results)
            if results:
                self.serp_queries_used += 1
                return results
        return self._serp_search(query, max_results)

    def _serp_search(self, query: str, max_results: int) -> List[Dict]:
        if not self.serp_client or self.serp_queries_used >= self.serp_max_queries:
            return []
        self.serp_queries_used += 1
        return self.serp_client.search(query, max_results)

    def _build_serp_priority_queries(self, ctx: ProjectContext) -> List[SearchQuery]:
        queries: List[SearchQuery] = []
        title = ctx.title.strip()
        if title:
            queries.append(
                SearchQuery(
                    query=f"\"{title}\" \"preuve de dépôt\" icpe",
                    intent=SearchIntent.ICPE,
                    reason="serp_title_preuve",
                )
            )
        combo = []
        if ctx.commune:
            combo.append(ctx.commune)
        if ctx.demandeur:
            combo.append(ctx.demandeur)
        if not combo and ctx.title_terms:
            combo = ctx.title_terms[:2]
        if combo:
            combo_query = " ".join(f"\"{term}\"" for term in combo)
            queries.append(
                SearchQuery(
                    query=f"{combo_query} \"preuve de dépôt\" icpe",
                    intent=SearchIntent.ICPE,
                    reason="serp_combo_preuve",
                )
            )
        return queries


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Enrichissement v3 - Préfectures, SERP et preuves de dépôt"
    )
    parser.add_argument("--input", required=True, help="CSV analysé en entrée")
    parser.add_argument("--output", help="CSV de sortie (optionnel)")
    parser.add_argument("--limit", type=int, help="Limiter nb de projets")
    parser.add_argument("--debug", action="store_true", help="Logs détaillés")
    parser.add_argument("--use-ai", action="store_true", help="Active la vérification Gemini")
    parser.add_argument(
        "--ai-max-docs",
        type=int,
        help=f"Nombre max de documents analysés par l'IA (défaut {V2Config.AI_MAX_DOCS})",
    )
    parser.add_argument(
        "--search-engine",
        choices=[e.value for e in SearchEngineType],
        default=SearchEngineType.AUTO.value,
        help="Moteur de recherche à utiliser (auto par défaut). L'option serpapi nécessite SERPAPI_KEY.",
    )
    parser.add_argument(
        "--serpapi-key",
        help="Clé SerpAPI (sinon lire SERPAPI_KEY dans l'environnement)",
    )
    parser.add_argument(
        "--serpapi-max",
        type=int,
        default=2,
        help="Nombre max de requêtes SerpAPI par projet (défaut 2, augmentez-le si vous utilisez uniquement SerpAPI)",
    )
    parser.add_argument(
        "--ground-key",
        help="Clé Gemini dédiée à la recherche (sinon GEMINI_SEARCH_KEY ou GEMINI_API_KEY)",
    )
    parser.add_argument(
        "--ground-model",
        default=V2Config.GROUND_SEARCH_MODEL,
        help=f"Modèle Gemini utilisé pour la recherche (défaut {V2Config.GROUND_SEARCH_MODEL})",
    )
    args = parser.parse_args()

    configure_logging(args.debug)

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Fichier introuvable: {input_path}")
        sys.exit(1)

    if not settings.GOOGLE_API_KEY or not settings.GOOGLE_API_KEY.strip():
        logger.error("Variable GOOGLE_API_KEY manquante")
        sys.exit(1)
    cse_id = getattr(settings, "GOOGLE_CSE_ID", None) or os.getenv("GOOGLE_CSE_ID")
    if not cse_id:
        logger.error("Variable GOOGLE_CSE_ID manquante")
        sys.exit(1)

    output_path = (
        Path(args.output)
        if args.output
        else settings.OUTPUT_DIR
        / "enriched"
        / (input_path.stem.replace("analyzed", "enriched_v2") + ".csv")
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path, dtype=str).fillna("")
    if args.limit:
        df = df.head(args.limit)

    search_engine_type = SearchEngineType(args.search_engine)

    enricher = PrefecturePermitEnricherV3(
        settings.GOOGLE_API_KEY,
        cse_id,
        use_ai=args.use_ai,
        ai_max_docs=args.ai_max_docs,
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        search_engine=search_engine_type,
        serpapi_key=args.serpapi_key,
        serpapi_max_queries=args.serpapi_max,
        ground_api_key=args.ground_key,
        ground_model=args.ground_model,
    )

    try:
        results = []
        start = time.time()
        for idx, row in df.iterrows():
            logger.info("\n" + "=" * 90)
            logger.info(f"Projet {idx + 1}/{len(df)} - {row.get('commune', '')}")
            try:
                enriched_row = enricher.enrich_project(row.to_dict())
                results.append(enriched_row)
            except Exception as exc:
                logger.error(f"  Erreur enrichissement: {exc}", exc_info=args.debug)
                fallback = row.to_dict()
                fallback.update(
                    {
                        "permit_confidence": "0.00",
                        "permit_summary": f"Erreur enrichissement: {exc}",
                        "icpe_confidence": "0.00",
                    }
                )
                results.append(fallback)
            time.sleep(0.5)

        df_out = pd.DataFrame(results)
        df_out.to_csv(output_path, index=False, encoding="utf-8")

        permits_found = sum(1 for r in results if r.get("permit_number"))
        icpe_found = sum(1 for r in results if r.get("icpe_reference"))
        avg_conf = (
            sum(float(r.get("permit_confidence", 0) or 0) for r in results) / len(results)
            if results
            else 0.0
        )
        report = {
            "metadata": {
                "input_file": str(input_path),
                "output_file": str(output_path),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_seconds": round(time.time() - start, 1),
                "projects_processed": len(results),
            },
            "statistics": {
                "permits_found": permits_found,
                "icpe_deposits_found": icpe_found,
                "average_permit_confidence": round(avg_conf, 2),
            },
        }
        report_path = output_path.parent / (output_path.stem + "_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print("=" * 80)
        print("ENRICHISSEMENT v2 - Résumé")
        print("=" * 80)
        print(f"Projets traités:        {len(results)}")
        print(f"Permis détectés:        {permits_found}")
        print(f"Preuves dépôt ICPE:     {icpe_found}")
        print(f"Confiance moyenne PC:   {avg_conf:.2f}")
        print(f"CSV:                    {output_path}")
        print(f"Rapport:                {report_path}")
        print("=" * 80)

    finally:
        enricher.close()


if __name__ == "__main__":
    main()
