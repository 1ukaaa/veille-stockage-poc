#!/usr/bin/env python3
"""
Scraper DREAL générique – Portail SIDE (Search.svc)
=====================================================
Version 5.2 - Transformé en BIBLIOTHÈQUE (LIB)

- Identique au V5.1
- Correction faute de frappe '_is_bbess_candidate'
- La section d'exécution (main) a été SUPPRIMÉE.
- Ce script est destiné à être IMPORTÉ par un "runner"
  (ex: run_all_single_csv.py)
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
import csv
import argparse
from hashlib import sha1
from html import unescape
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import uuid4
from pathlib import Path
from dataclasses import dataclass, fields

import httpx  # Importe httpx pour créer un client POST local

# ======================================================================
# Modèle de données (précédemment dans models.py)
# ======================================================================

@dataclass
class Project:
    project_id: str
    region: str
    dept: str
    year: str
    project_title: str
    project_url: str
    url_cerfa: Optional[str] = None
    url_decision: Optional[str] = None

    def get_csv_headers(self) -> List[str]:
        """Retourne les en-têtes pour le CSV."""
        return [f.name for f in fields(self)]

    def to_csv_row(self) -> List[str]:
        """Retourne les valeurs pour le CSV."""
        return [getattr(self, f.name) or "" for f in fields(self)]

# ======================================================================
# Configuration (Valeurs par défaut et constantes)
# ======================================================================

# Note: le logger est configuré par le script 'runner' principal
logger = logging.getLogger(__name__)

BASE_URL = "https://side.developpement-durable.gouv.fr"
SEARCH_ENDPOINT = f"{BASE_URL}/PAE/Portal/Recherche/Search.svc/Search"

SCENARIO_CODE = "AE-GENERAL"
SEARCH_CONTEXT = 1
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
# Construction de la requête
# ======================================================================

def _build_query_payload(
    search_term: str, 
    year: str, 
    page: int, 
    region_label: str
) -> Dict:
    """
    Construit le payload JSON.
    ATTENTION : Basé sur le payload 'occitanie.json'.
    Le filtre 'region_label' utilise le champ "_212", qui est
    probablement spécifique à l'Occitanie.
    """
    
    # Structure copiée de payload_occitanie.json
    payload = {
      "query": {
        "AdvancedQuery": {
          "queryGroups": [
            {
              "logical": None, 
              "queryClauses": [
                {
                  "index": "Title_idx",
                  "logical": 0,
                  "operator": 0,
                  "otherValue": None,
                  "value": search_term
                }
              ]
            }
          ]
        },
        "AdvancedQueryDisplay": f"(Titre={search_term})",
        # ATTENTION : "_212" est le point de défaillance probable
        "FacetFilter": json.dumps({"_1722": year, "_212": region_label}),
        "ForceSearch": True,
        "InitialSearch": False,
        "Page": page,
        "PageRange": 3,
        "QueryGuid": str(uuid4()),
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
      }
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


def _collect_results_for_term(
    term: str, 
    year: str, 
    region_label: str
) -> List[Dict]:
    """Récupère tous les résultats pour un terme (gère la pagination)"""
    logger.info(f"[SIDE] Requête: Année={year}, Région={region_label}, Terme={term}")
    collected: List[Dict] = []
    seen_ids: set[str] = set()

    for page in range(MAX_PAGES):
        payload = _build_query_payload(term, year, page, region_label)
        data = _post_search(payload)
        results = data.get("Results") or []
        
        if not results:
            logger.debug(f"[SIDE] Page {page}: 0 résultats, arrêt.")
            break

        for result in results:
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
    ATTENTION : Basé sur la structure 'Resource.Ttl' et 'CustomResult'
    observée pour l'Occitanie (V2/V4).
    """
    resource = result.get("Resource", {})
    title = resource.get("Ttl", "") # Point de défaillance probable
    summary = result.get("CustomResult", "") # Point de défaillance probable
    
    text = f"{title} {summary}"
    norm = _normalize_text(text)
    if not norm: return False

    if any(nk in norm for nk in NEGATIVE_KEYWORDS):
        logger.debug(f"[BESS-FILTER] Exclu (mot-clé négatif): {title[:80]}")
        return False

    return True


def _classify_primary_docs_v2(docs: List[Dict]) -> Tuple[Optional[str], Optional[str]]:
    """
    ATTENTION : Classifie les documents basés sur la structure 'PrimaryDocs'
    vue pour l'Occitanie.
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


def _build_project_from_side_v2(
    result: Dict, 
    region_slug: str
) -> Optional[Project]:
    """
    ATTENTION : Construit l'objet Project final en se basant sur la
    structure de réponse JSON V2 (Occitanie).
    """
    resource = result.get("Resource", {})
    title = resource.get("Ttl") # Point de défaillance probable
    if not title:
        logger.warning("Résultat sans 'Resource.Ttl', ignoré.")
        return None

    friendly_url = result.get("FriendlyUrl")
    identifier = resource.get("RscId")
    if not friendly_url and identifier:
        friendly_url = f"{BASE_URL}/PAE/doc/SYRACUSE/{identifier}"

    # Extraction depuis la structure Occitanie V2
    docs = result.get("PrimaryDocs", []) # Point de défaillance probable
    url_cerfa, url_decision = _classify_primary_docs_v2(docs)
    
    year = resource.get("Dt", "??")
    dept = _extract_dept_from_title(title)

    return Project(
        project_id=_sha1(f"{identifier}-{friendly_url}"),
        region=region_slug, # Paramètre injecté
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
    year: str,
    region_label: str,
    region_slug: str,
    dept: Optional[str] = None,
) -> List[Project]:
    """
    Découvre les projets BESS pour la région via l'API SIDE.
    """

    logger.info(
        "\n%s\nSCRAPER SIDE (V5.2 - LIB)\n"
        " • Région Label = %s\n"
        " • Région Slug = %s\n"
        " • Année cible = %s\n"
        " • Département = %s\n%s",
        "=" * 70, region_label, region_slug, year, dept or "TOUS", "=" * 70
    )
    
    try:
        raw_hits = 0
        unique_hits = 0
        bess_candidates = 0
        aggregated: Dict[str, Dict] = {}

        for term in QUERY_TERMS:
            # Passe les arguments de région à la collecte
            results = _collect_results_for_term(term, year, region_label)
            raw_hits += len(results)
            for result in results:
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
            
            # === CORRECTION APPLIQUÉE ICI ===
            if not _is_bess_candidate(result):
                continue
            # ==================================

            bess_candidates += 1
            # Passe le slug à la construction du projet
            project = _build_project_from_side_v2(result, region_slug)
            if not project or not project.project_url:
                continue

            if year and project.year != str(year):
                continue
            if dept_filter and project.dept != dept_filter:
                continue

            projects.append(project)

        logger.info(
            "\n%s\nRÉSUMÉ %s\n"
            " • Notices BESS retenues: %s / %s uniques\n"
            " • Année filtrée: %s\n"
            " • Département filtré: %s\n"
            " • TOTAL: %s projets\n%s",
            "=" * 70, region_slug.upper(), bess_candidates, unique_hits,
            year or "TOUTES", dept_filter or "TOUS",
            len(projects), "=" * 70
        )
        return projects

    except Exception as exc:
        logger.error("Erreur scraper %s: %s", region_slug, exc, exc_info=True)
        return []

# ======================================================================
# Écriture CSV (Fonction utilitaire)
# ======================================================================

def _write_to_csv(projects: List[Project], output_file: Path):
    """Écrit la liste des projets dans un fichier CSV."""
    if not projects:
        logger.warning("Aucun projet trouvé, aucun fichier CSV ne sera créé.")
        return

    try:
        with output_file.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            headers = projects[0].get_csv_headers()
            writer.writerow(headers)
            
            for project in projects:
                writer.writerow(project.to_csv_row())
        
        logger.info(f"✅ Succès : {len(projects)} projets écrits dans {output_file}")

    except Exception as e:
        logger.error(f"Erreur lors de l'écriture du CSV : {e}", exc_info=True)

# FIN DU FICHIER (Pas de 'if __name__ == "__main__":')