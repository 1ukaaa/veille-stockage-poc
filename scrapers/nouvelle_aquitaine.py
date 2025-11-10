#!/usr/bin/env python3
"""
Scraper DREAL Nouvelle-Aquitaine – Portail SIDE (Search.svc)
================================================================

Architecture du portail:
    - Les résultats régionaux sont exposés via le service JSON
      `/PAE/Portal/Recherche/Search.svc/Search`.
    - Chaque requête prend un objet `query` (payload Solr) qui
      combine un filtre AgenceCatalogage + des mots-clés.
    - Les notices retournent directement l'URL HTML (`FriendlyUrl`)
      et les liens PDF primaires (`PrimaryDocUrl` / `PrimaryDoc_xml`).

Stratégie:
    1. On exécute une série de requêtes ciblées (batterie, stockage
       d'énergie, BESS, etc.) afin de couvrir les différentes
       formulations possibles.
    2. On fusionne/dé-duplique toutes les notices via leur Identifiant
       Syracuse.
    3. On applique un filtre BESS (mots-clés positifs + contexte
       électrique, listes d'exclusion pour self-stockage, déchets,
       logistique, etc.).
    4. On expose la notice SIDE (`FriendlyUrl`) comme URL principale
       et, si disponibles, on mappe les PDF (CERFA vs décision) via
       l'alias fourni dans `PrimaryDoc_xml`.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from hashlib import sha1
from html import unescape
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

import httpx

from config import settings
from models import Project

logger = logging.getLogger(__name__)

# ======================================================================
# Configuration
# ======================================================================
BASE_URL = "https://side.developpement-durable.gouv.fr"
SEARCH_ENDPOINT = f"{BASE_URL}/PAE/Portal/Recherche/Search.svc/Search"

REGION_SLUG = "nouvelle-aquitaine"
REGION_LABEL = "Nouvelle-Aquitaine"

SCENARIO_CODE = "AE-GENERAL"
REGION_FILTER = 'AgenceCatalogage_idx:"*Nouvelle-Aquitaine"'
SEARCH_CONTEXT = 11
RESULT_SIZE = 50
MAX_PAGES = 20

QUERY_TERMS = [
    "batter*",  # Batterie, batteries…
    '"stockage d\'energie"',
    '"stockage d\'énergie"',
    '"stockage d\'electricite"',
    '"stockage d\'électricité"',
    '"stockage par batterie"',
    '"système de stockage"',
    '"systeme de stockage"',
    '"station de stockage"',
    '"unité de stockage"',
    '"unite de stockage"',
    "BESS",
]

BATTERY_KEYWORDS = [
    "batterie",
    "batteries",
    "battery",
    "bess",
    "accumulateur",
    "stockage par batterie",
]

ELECTRIC_HINTS = [
    "energie",
    "energie",
    "énergie",
    "electric",
    "électr",
]

NEGATIVE_KEYWORDS = [
    "self stockage",
    "self-stockage",
    "dechet",
    "déchet",
    "decharge",
    "dechèterie",
    "decheterie",
    "logistique",
    "plateforme logistique",
    "camping",
    "hangar",
    "stabulation",
    "alcool",
    "alcools",
    "vin",
    "spiriteux",
    "spiritueux",
    "cidre",
    "grain",
    "fourrage",
    "engrais",
    "chambre froide",
    "froid",
    "alimentaire",
    "depot de bus",
    "depot de cars",
    "transport",
    "vehicule",
    "vehicules",
    "carburant",
    "essence",
    "gaz",
    "hydrogene",
    "hydrogène",
    "fuel",
    "bois energie",
    "bois-énergie",
    "granule",
    "granulé",
    "station d epuration",
    "step",
]

NEGATIVE_KEYWORDS = [nk.lower() for nk in NEGATIVE_KEYWORDS]
BESS_PATTERN = re.compile(r"\bb\.?e\.?s\.?s\b", re.I)
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")
IDENTIFIER_PATTERN = re.compile(r"/SYRACUSE/(\d+)")

DEPARTMENT_NAMES = {
    "CHARENTE": "16",
    "CHARENTE MARITIME": "17",
    "CORREZE": "19",
    "CREUSE": "23",
    "DORDOGNE": "24",
    "GIRONDE": "33",
    "LANDES": "40",
    "LOT ET GARONNE": "47",
    "PYRENEES ATLANTIQUES": "64",
    "DEUX SEVRES": "79",
    "VIENNE": "86",
    "HAUTE VIENNE": "87",
}


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    text = unescape(value)
    text = HTML_TAG_PATTERN.sub(" ", text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text.lower()).strip()
    return text


def _normalize_key(value: str) -> str:
    normalized = _normalize_text(value)
    normalized = re.sub(r"[^a-z0-9]", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


DEPARTMENT_LOOKUP = {
    _normalize_key(name): code for name, code in DEPARTMENT_NAMES.items()
}


def _sha1(text: str) -> str:
    return sha1(text.encode("utf-8")).hexdigest()


def _build_query_payload(query_string: str, page: int) -> Dict:
    """Construit l'objet `query` attendu par Search.svc."""
    return {
        "query": {
            "QueryGuid": str(uuid4()),
            "SearchContext": SEARCH_CONTEXT,
            "Grid": None,
            "AdvancedQuery": None,
            "AdvancedQueryDisplay": None,
            "DisabledMemorizedFilters": None,
            "QueryString": query_string,
            "SelectionUid": 0,
            "OriginalQueryString": None,
            "CloudTerms": [],
            "ScenarioCode": SCENARIO_CODE,
            "SearchGridFieldsShownOnResultsDTO": [],
            "SelectedSearchGridFieldsShownOnResultsIds": None,
            "FacetFilter": "{}",
            "HiddenFacetFilter": "{}",
            "FacetContains": None,
            "SortOrder": 0,
            "SortField": "DateOfInsertion_sort",
            "SortFieldRandomSeed": None,
            "Page": page,
            "ResultSize": RESULT_SIZE,
            "SessionGuid": None,
            "ForceSearch": True,
            "ApplyTemplate": True,
            "TemplateValue": None,
            "TemplateParams": {
                "ScenarioCode": "",
                "SourceCode": "",
                "UseCompact": False,
                "Support": "",
                "Size": None,
                "Scope": "PAE",
            },
            "InjectFields": True,
            "InitialSearch": page == 0,
            "XslPath": None,
            "UseCanvas": False,
            "AsyncOFInsertion": None,
            "UseSpellChecking": None,
            "Monolingual": None,
            "InjectOpenFind": False,
            "ForceOpenFindUpdate": False,
            "SearchLabel": query_string,
            "InjectFacets": None,
            "InjectSuggestFacet": None,
            "Highlight": None,
            "RefreshFacet": None,
            "Block": None,
            "IgnoreVariables": False,
            "GroupField": None,
            "GroupSize": 0,
            "Url": None,
            "UseGrouping": None,
            "UseCache": False,
            "SiteCodeRestriction": None,
            "RawQueryParameters": None,
            "PageRange": 3,
            "ListSelectionPage": None,
            "SearchTerms": query_string,
            "ScenarioDisplayMode": "display-standard",
            "GeoBounds": None,
            "DecodedFacets": [],
            "DecodedFacetContains": [],
            "HierarchicalFacetPrefixes": None,
            "ExceptTotalFacet": True,
            "MemorizeGrid": False,
        }
    }


def _post_search(client: httpx.Client, payload: Dict) -> Dict:
    response = client.post(SEARCH_ENDPOINT, json=payload)
    response.raise_for_status()
    data = response.json()
    if not data.get("success", False):
        raise RuntimeError(f"Search API error: {data.get('message')}")
    return data.get("d") or {}


def _collect_results_for_query(client: httpx.Client, query_string: str) -> List[Dict]:
    logger.info(f"[SIDE] Requête: {query_string}")
    collected: List[Dict] = []
    seen_ids: set[str] = set()

    for page in range(MAX_PAGES):
        payload = _build_query_payload(query_string, page)
        data = _post_search(client, payload)
        results = data.get("Results") or []
        logger.debug(
            "[SIDE] page %s – %s résultats (cumul=%s)",
            page,
            len(results),
            len(collected),
        )

        if not results:
            break

        for result in results:
            identifier = _get_identifier(result)
            if not identifier or identifier in seen_ids:
                continue
            seen_ids.add(identifier)
            collected.append(result)

        total = data.get("SearchInfo", {}).get("NBResults")
        if total and len(seen_ids) >= total:
            break

    return collected


def _get_identifier(result: Dict) -> str:
    field_list = result.get("FieldList") or {}
    for key in ("Identifier", "Field950b", "id"):
        values = field_list.get(key)
        if values:
            return str(values[0])

    friendly_url = result.get("FriendlyUrl", "")
    if friendly_url:
        match = IDENTIFIER_PATTERN.search(friendly_url)
        if match:
            return match.group(1)

    return _sha1(json.dumps(result, sort_keys=True))[:16]


def _is_bess_candidate(result: Dict) -> bool:
    field_list = result.get("FieldList") or {}
    title = " ".join(field_list.get("Title") or [])
    subjects = " ".join(field_list.get("SubjectTopicSuggest_exact") or [])
    summary = result.get("CustomResult") or result.get("CompactResult") or ""

    text = " ".join(part for part in [title, subjects, summary] if part)
    norm = _normalize_text(text)
    if not norm:
        return False

    contains_battery = any(keyword in norm for keyword in BATTERY_KEYWORDS)
    contains_stockage_elec = "stockag" in norm and any(
        hint in norm for hint in ELECTRIC_HINTS
    )

    if not (contains_battery or contains_stockage_elec or BESS_PATTERN.search(norm)):
        return False

    if any(nk in norm for nk in NEGATIVE_KEYWORDS):
        logger.debug("[BESS-FILTER] Exclu (mot-clé négatif): %s", title[:80])
        return False

    return True


def _parse_primary_documents(field_list: Dict) -> List[Tuple[str, str]]:
    docs: List[Tuple[str, str]] = []
    xml_entries = field_list.get("PrimaryDoc_xml") or []
    url_entries = field_list.get("PrimaryDocUrl") or []

    for idx, xml_blob in enumerate(xml_entries):
        url_match = re.search(r"<url>(.*?)</url>", xml_blob or "", re.I)
        alias_match = re.search(r"<alias>(.*?)</alias>", xml_blob or "", re.I)
        url = url_match.group(1).strip() if url_match else ""
        alias = alias_match.group(1).strip() if alias_match else ""

        if not url and idx < len(url_entries):
            url = url_entries[idx]

        if url:
            docs.append((url, alias))

    if not docs and url_entries:
        docs.extend((url, "") for url in url_entries)

    return docs


def _classify_primary_docs(docs: Iterable[Tuple[str, str]]) -> Tuple[Optional[str], Optional[str]]:
    url_cerfa = None
    url_decision = None

    for url, alias in docs:
        alias_norm = _normalize_text(alias)
        if not url_cerfa and (
            "formulaire" in alias_norm
            or "cerfa" in alias_norm
            or "dossier" in alias_norm
            or "demande" in alias_norm
        ):
            url_cerfa = url
        if not url_decision and (
            "decision" in alias_norm
            or "décision" in alias_norm
            or "avis" in alias_norm
            or "arrete" in alias_norm
            or "arrêté" in alias_norm
        ):
            url_decision = url

    return url_cerfa, url_decision


def _extract_year(field_list: Dict) -> str:
    for key in ("YearOfPublication", "DateOfPublication", "DateOfInsertion"):
        values = field_list.get(key)
        if not values:
            continue
        match = YEAR_PATTERN.search(values[0])
        if match:
            return match.group(0)
    return "????"


def _extract_dept(field_list: Dict, title: str) -> str:
    locations = (
        field_list.get("SubjectLocation_exact")
        or field_list.get("SubjectLocation")
        or field_list.get("ThesaurusLabel_exact")
        or []
    )

    for loc in locations:
        norm = _normalize_key(loc)
        if norm in DEPARTMENT_LOOKUP:
            return DEPARTMENT_LOOKUP[norm]

    match = re.search(r"\((\d{2})(?:\d{3})?\)", title or "")
    if match:
        return match.group(1)

    return "??"


def _build_project(result: Dict) -> Optional[Project]:
    field_list = result.get("FieldList") or {}
    title = (field_list.get("Title") or [""])[0].strip()
    if not title:
        return None

    friendly_url = result.get("FriendlyUrl") or ""
    identifier = _get_identifier(result)

    if not friendly_url and identifier:
        friendly_url = f"{BASE_URL}/PAE/doc/SYRACUSE/{identifier}"

    docs = _parse_primary_documents(field_list)
    url_cerfa, url_decision = _classify_primary_docs(docs)

    year = _extract_year(field_list)
    dept = _extract_dept(field_list, title)

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


@dataclass
class SearchStats:
    raw_hits: int = 0
    unique_hits: int = 0
    bess_candidates: int = 0


def discover_projects(
    year: Optional[str] = None,
    client: Optional[httpx.Client] = None,
    dept: Optional[str] = None,
    **_
) -> List[Project]:
    """
    Découvre les projets BESS pour la région Nouvelle-Aquitaine.

    Args:
        year: Filtre optionnel (YYYY)
        client: httpx.Client réutilisable (sinon créé localement)
        dept: Filtre optionnel sur le code département (2 chiffres)

    Returns:
        Liste de Project prêts pour export CSV.
    """

    logger.info(
        "\n%s\nSCRAPER NOUVELLE-AQUITAINE (SIDE)\n"
        " • Filtres région = %s\n"
        " • Requêtes clés = %s\n%s",
        "=" * 70,
        REGION_LABEL,
        len(QUERY_TERMS),
        "=" * 70,
    )

    managed_client = client is None
    http_client = client or httpx.Client(
        timeout=settings.TIMEOUT,
        headers={"User-Agent": settings.USER_AGENT},
    )

    try:
        stats = SearchStats()
        aggregated: Dict[str, Dict] = {}

        for term in QUERY_TERMS:
            query = f"({REGION_FILTER}) AND ({term})"
            results = _collect_results_for_query(http_client, query)
            stats.raw_hits += len(results)

            for result in results:
                identifier = _get_identifier(result)
                aggregated.setdefault(identifier, result)

        stats.unique_hits = len(aggregated)
        logger.info(
            "[SIDE] %s requêtes → %s notices uniques (raw: %s)",
            len(QUERY_TERMS),
            stats.unique_hits,
            stats.raw_hits,
        )

        projects: List[Project] = []
        dept_filter = dept.zfill(2) if dept and dept.isdigit() else None

        for identifier, result in aggregated.items():
            if not _is_bess_candidate(result):
                continue

            stats.bess_candidates += 1
            project = _build_project(result)
            if not project or not project.project_url:
                logger.debug("[SIDE] Notice sans URL: %s", identifier)
                continue

            if year and project.year != str(year):
                continue

            if dept_filter and project.dept != dept_filter:
                continue

            projects.append(project)

        logger.info(
            "\n%s\nRÉSUMÉ NOUVELLE-AQUITAINE\n"
            " • Notices BESS retenues: %s / %s uniques\n"
            " • Année filtrée: %s\n"
            " • Département filtré: %s\n"
            " • TOTAL: %s projets\n%s",
            "=" * 70,
            stats.bess_candidates,
            stats.unique_hits,
            year or "TOUTES",
            dept_filter or "TOUS",
            len(projects),
            "=" * 70,
        )

        return projects

    except Exception as exc:
        logger.error("Erreur scraper Nouvelle-Aquitaine: %s", exc, exc_info=True)
        return []

    finally:
        if managed_client:
            http_client.close()
