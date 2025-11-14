#!/usr/bin/env python3
"""
Scraper DREAL Occitanie – Portail SIDE (Search.svc)
=====================================================
Version 4 - Corrigée et basée sur l'analyse du payload et de la réponse.

- Cible l'API JSON '/PAE/Portal/Recherche/Search.svc/Search'.
- Construit le payload JSON exact capturé lors des tests.
- Gère sa propre instance 'httpx.Client' pour les requêtes POST.
- Parse la structure de réponse JSON spécifique à Occitanie (Resource.Ttl, PrimaryDocs).
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from hashlib import sha1
from html import unescape
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

import httpx  # Importe httpx pour créer un client POST local
from models import Project  # Utilise le modèle de données central

logger = logging.getLogger(__name__)

# ======================================================================
# Configuration
# ======================================================================
BASE_URL = "https://side.developpement-durable.gouv.fr"
SEARCH_ENDPOINT = f"{BASE_URL}/PAE/Portal/Recherche/Search.svc/Search"
REGION_SLUG = "occitanie"
REGION_LABEL = "Occitanie"

SCENARIO_CODE = "AE-GENERAL"
SEARCH_CONTEXT = 1  # 1 est dans le payload, pas 14
RESULT_SIZE = 50
MAX_PAGES = 20

# Termes de recherche BESS
QUERY_TERMS = [
    "batterie", "stockage d'énergie", "stockage d'électricité",
    "stockage par batterie", "système de stockage", "station de stockage",
    "unité de stockage", "bess", "accumulateur"
]

# Mots-clés de validation
NEGATIVE_KEYWORDS = [
    "self stockage", "dechet", "logistique", "camping", "hangar",
    "stabulation", "alcool", "vin", "grain", "fourrage",
    "engrais", "chambre froide", "froid", "alimentaire", "depot de bus",
    "transport", "vehicule", "carburant", "essence", "gaz", "hydrogene",
    "fuel", "bois energie", "granule", "station d epuration", "step",
]
NEGATIVE_KEYWORDS = [nk.lower() for nk in NEGATIVE_KEYWORDS]

BESS_PATTERN = re.compile(r"\bb\.?e\.?s\.?s\b", re.I)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")

# ======================================================================
# Fonctions utilitaires
# ======================================================================

def _normalize_text(value: str) -> str:
    if not value: return ""
    text = unescape(value)
    text = HTML_TAG_PATTERN.sub(" ", text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text.lower()).strip()
    return text

def _sha1(text: str) -> str:
    return sha1(text.encode("utf-8")).hexdigest()

def _extract_dept_from_title(title: str) -> str:
    """Extrait le département (ex: (31)) du titre."""
    if not title: return "??"
    match = re.search(r"\((\d{2})\)", title)
    if match: return match.group(1)
    return "??"

# ======================================================================
# Construction de la requête (BASÉ SUR payload_occitanie.json)
# ======================================================================

def _build_query_payload(search_term: str, year: str, page: int) -> Dict:
    """
    Construit le payload JSON exact basé sur le fichier 'payload_occitanie.json'
    capturé qui a fonctionné.
    """
    
    # Structure copiée de payload_occitanie.json
    payload = {
      "query": {
        "AdvancedQuery": {
          "queryGroups": [
            {
              "logical": None, # 'null' en JSON
              "queryClauses": [
                {
                  "index": "Title_idx",
                  "logical": 0,
                  "operator": 0,
                  "otherValue": None, # 'null' en JSON
                  "value": search_term # Paramètre 1: Le terme BESS
                }
              ]
            }
          ]
        },
        "AdvancedQueryDisplay": f"(Titre={search_term})",
        # Paramètre 2: L'année
        "FacetFilter": json.dumps({"_1722": year, "_212": REGION_LABEL}),
        "ForceSearch": True,
        "InitialSearch": False,
        "Page": page, # Paramètre 3: La page
        "PageRange": 3,
        "QueryGuid": str(uuid4()), # Génère un nouveau GUID
        "ResultSize": RESULT_SIZE,
        "ScenarioCode": SCENARIO_CODE,
        "ScenarioDisplayMode": "display-standard",
        "SearchContext": SEARCH_CONTEXT,
        "SearchGridFieldsShownOnResultsDTO": [],
        "SearchTerms": f" {search_term}",
        "SortField": "DateOfModification_sort",
        "SortOrder": 0,
        "TemplateParams": {
          "Scenario": "", "Scope": "PAE", "Size": None,
          "Source": "", "Support": "", "UseCompact": False
        },
        "UseSpellChecking": None
        # Le champ 'Url' du payload original n'est pas nécessaire
      }
      # Le champ 'sst' du payload original n'est pas nécessaire
    }
    return payload

# ======================================================================
# Logique de scraping
# ======================================================================

def _post_search(payload: Dict) -> Dict:
    """Envoie la requête POST à l'API Search.svc."""
    try:
        with httpx.Client(timeout=30.0) as post_client:
            response = post_client.post(SEARCH_ENDPOINT, json=payload)
            response.raise_for_status()
            data = response.json()
            if not data.get("success", False):
                logger.error(f"Erreur API (success=false): {data.get('message', 'Aucun message')}")
                return {}
            return data.get("d") or {}
    except httpx.ReadTimeout:
        logger.warning(f"Timeout API pour le payload: {payload.get('query', {}).get('SearchTerms')}")
        return {}
    except Exception as e:
        logger.error(f"Erreur requête POST: {e}", exc_info=True)
        return {}


def _collect_results_for_term(term: str, year: str) -> List[Dict]:
    """Récupère tous les résultats pour un terme (gère la pagination)"""
    logger.info(f"[SIDE] Requête: Année={year}, Terme={term}")
    collected: List[Dict] = []
    seen_ids: set[str] = set()

    for page in range(MAX_PAGES):
        payload = _build_query_payload(term, year, page)
        data = _post_search(payload)
        results = data.get("Results") or []
        
        if not results:
            logger.debug(f"[SIDE] Page {page}: 0 résultats, arrêt.")
            break

        for result in results:
            # Utilise RscId comme identifiant unique
            identifier = result.get("Resource", {}).get("RscId")
            if not identifier or identifier in seen_ids:
                continue
            seen_ids.add(identifier)
            collected.append(result)

        logger.debug(f"[SIDE] Page {page}: {len(results)} résultats ({len(collected)} cumulés)")
        total = data.get("SearchInfo", {}).get("NBResults")
        if total and len(seen_ids) >= total:
            break
            
    return collected


def _is_bess_candidate(result: Dict) -> bool:
    """
    Valide si le résultat est un projet BESS (post-filtre).
    [CORRIGÉ] Utilise Resource.Ttl et CustomResult (issus du log V2).
    """
    resource = result.get("Resource", {})
    title = resource.get("Ttl", "")
    summary = result.get("CustomResult", "") # Contient la description
    
    text = f"{title} {summary}"
    norm = _normalize_text(text)
    if not norm: return False

    # Le filtre "stockage" a déjà été appliqué par l'API (AdvancedQuery).
    # Nous devons juste vérifier les exclusions.
    if any(nk in norm for nk in NEGATIVE_KEYWORDS):
        logger.debug(f"[BESS-FILTER] Exclu (mot-clé négatif): {title[:80]}")
        return False

    return True


def _classify_primary_docs_v2(docs: List[Dict]) -> Tuple[Optional[str], Optional[str]]:
    """
    [CORRIGÉ] Classifie les documents basés sur la structure 'PrimaryDocs'
    vue dans le log V2.
    """
    url_cerfa = None
    url_decision = None

    for doc in docs:
        label = _normalize_text(doc.get("Label", ""))
        link = doc.get("Link")
        if not link:
            continue
            
        if "dossier" in label or "demande" in label:
            if not url_cerfa:
                url_cerfa = link
        elif "décision" in label or "decision" in label:
            if not url_decision:
                url_decision = link

    return url_cerfa, url_decision


def _build_project_from_side_v2(result: Dict) -> Optional[Project]:
    """
    [CORRIGÉ] Construit l'objet Project final en se basant sur la
    structure de réponse JSON V2 (Resource.Ttl, PrimaryDocs, etc.).
    """
    resource = result.get("Resource", {})
    title = resource.get("Ttl")
    if not title:
        logger.warning("Résultat sans 'Resource.Ttl', ignoré.")
        return None

    friendly_url = result.get("FriendlyUrl")
    identifier = resource.get("RscId")
    if not friendly_url and identifier:
        friendly_url = f"{BASE_URL}/PAE/doc/SYRACUSE/{identifier}"

    # Extraction depuis la nouvelle structure
    docs = result.get("PrimaryDocs", [])
    url_cerfa, url_decision = _classify_primary_docs_v2(docs)
    
    year = resource.get("Dt", "??") # Champ 'Dt' vu dans le log
    dept = _extract_dept_from_title(title)

    return Project(
        project_id=_sha1(f"{identifier}-{friendly_url}"),
        region=REGION_SLUG,
        dept=dept,
        year=year,
        project_title=title[:200],
        project_url=friendly_url or (url_decision or url_cerfa or ""),
        url_cerfa=url_cerfa,
        url_decision=url_decision,
    )

# ======================================================================
# API Principale (Interface pour discover.py)
# ======================================================================

def discover_projects(
    year: Optional[str] = None,
    client: Optional[httpx.Client] = None, # 'client' (Adaptive) n'est PAS utilisé
    dept: Optional[str] = None,
    seed_url: Optional[str] = None,
) -> List[Project]:
    """
    Découvre les projets BESS pour la région Occitanie via l'API SIDE.
    """

    if not year:
        logger.error("Le scraper Occitanie (SIDE) nécessite un argument --year.")
        return []

    logger.info(
        "\n%s\nSCRAPER OCCITANIE (SIDE - V4 Corrigée)\n"
        " • Année cible = %s\n"
        " • Département = %s\n%s",
        "=" * 70, year, dept or "TOUS", "=" * 70
    )
    
    # Note : 'client' (AdaptiveHTTPClient) n'est pas utilisé pour les POST.
    # _post_search() crée son propre client httpx.

    try:
        raw_hits = 0
        unique_hits = 0
        bess_candidates = 0
        aggregated: Dict[str, Dict] = {}

        for term in QUERY_TERMS:
            results = _collect_results_for_term(term, year)
            raw_hits += len(results)
            for result in results:
                # Utilise RscId (plus fiable) comme identifiant
                identifier = result.get("Resource", {}).get("RscId")
                if identifier:
                    aggregated.setdefault(identifier, result)

        unique_hits = len(aggregated)
        logger.info(
            "[SIDE] %s requêtes → %s notices uniques (raw: %s)",
            len(QUERY_TERMS), unique_hits, raw_hits
        )

        projects: List[Project] = []
        dept_filter = dept.zfill(2) if dept and dept.isdigit() else None

        for identifier, result in aggregated.items():
            if not _is_bess_candidate(result):
                continue

            bess_candidates += 1
            project = _build_project_from_side_v2(result)
            if not project or not project.project_url:
                continue

            if year and project.year != str(year):
                continue
            if dept_filter and project.dept != dept_filter:
                continue

            projects.append(project)

        logger.info(
            "\n%s\nRÉSUMÉ OCCITANIE\n"
            " • Notices BESS retenues: %s / %s uniques\n"
            " • Année filtrée: %s\n"
            " • Département filtré: %s\n"
            " • TOTAL: %s projets\n%s",
            "=" * 70, bess_candidates, unique_hits,
            year or "TOUTES", dept_filter or "TOUS",
            len(projects), "=" * 70
        )
        return projects

    except Exception as exc:
        logger.error("Erreur scraper Occitanie: %s", exc, exc_info=True)
        return []