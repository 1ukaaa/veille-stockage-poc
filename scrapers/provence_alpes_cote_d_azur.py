#!/usr/bin/env python3
"""
Scraper DREAL Provence-Alpes-Côte d'Azur (PACA)

Architecture:
    - URLs départementales figées par année (pages statiques en SPIP)
    - Chaque page liste les projets (cartes) pour l'année/département
    - Pagination via liens <a class="fr-pagination__link ...">
    - On filtre les cartes directement (titre + description) pour repérer les projets BESS,
      puis on expose l'URL de la fiche projet (les documents seront gérés par extract.py)
"""

import hashlib
import logging
import re
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
from selectolax.parser import HTMLParser

from models import Project

logger = logging.getLogger(__name__)

# ======================================================================
# Configuration
# ======================================================================
DELAY = 0.15
BASE_URL = "https://www.paca.developpement-durable.gouv.fr"
MAX_PAGES_PER_DEPT = 25  # Sécu pagination

NATIONAL_GATEWAY_API = (
    "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/Get"
)
NATIONAL_PORTAL_BASE = "https://evaluation-environnementale.ecologie.gouv.fr"
PACA_REGION_CODE = "93"
PACA_DEPTS = ["04", "05", "06", "13", "83", "84"]

PACA_DEPT_URLS: Dict[str, Dict[str, str]] = {
    "2024": {
        "04": "04-alpes-de-haute-provence-r3465.html",
        "05": "05-hautes-alpes-r3466.html",
        "06": "06-alpes-maritimes-r3467.html",
        "13": "13-bouches-du-rhone-r3468.html",
        "83": "83-var-r3469.html",
        "84": "84-vaucluse-r3470.html",
    },
    "2023": {
        "04": "04-alpes-de-haute-provence-r3367.html",
        "05": "05-hautes-alpes-r3368.html",
        "06": "06-alpes-maritimes-r3369.html",
        "13": "13-bouches-du-rhone-r3370.html",
        "83": "83-var-r3371.html",
        "84": "84-vaucluse-r3372.html",
    },
}


# ======================================================================
# Helpers
# ======================================================================
def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _normalize_url(url: str) -> str:
    if not url:
        return url
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))


def _extract_dept_code(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\((\d{2})\)", text)
    if m:
        return m.group(1)
    m = re.search(r"\b(\d{2})\s*-\s*[A-Za-z]", text)
    if m:
        return m.group(1)
    return ""


CARD_INCLUDE = [
    "stockag",
    "batter",
    "batterie",
    "stockage par batterie",
    "stockage batterie",
    "accumulateur",
    "station de stockage",
    "système de stockage",
    "systeme de stockage",
]

DETAIL_INCLUDE = [
    "stockage d'électricité",
    "stockage d electricite",
    "stockage d’énergie",
    "stockage d energie",
    "stockage par batteries",
    "stockage par batterie",
    "stockage batterie",
    "batteries",
    "batterie",
    "système de stockage",
    "systeme de stockage",
    "station de stockage",
    "unité de stockage",
    "unite de stockage",
]

NEGATIVE_KEYWORDS = [
    "micro-centrale",
    "micro centrale",
    "hydroélectrique",
    "hydroelectrique",
    "camping",
    "self-stockage",
    "self stockage",
    "déchet",
    "dechet",
    "déchèterie",
    "decheterie",
    "centrale de froid",
    "centrale de chaud",
    "gaz naturel",
    "hydrogène",
    "hydrogene",
    "photovolta",
    "solaire",
    "carrière",
    "carriere",
    "imperméabilisation",
    "impermEabilisation",
    "poste électrique",
    "poste electrique",
    "poste de transformation",
    "logement",
    "camp de vacances",
    "gaz",
]

DOC_URL_PATTERN = re.compile(r"\.(pdf|zip)(?:$|[?#])", re.I)
BESS_PATTERN = re.compile(r"\bb\.?e\.?s\.?s\b", re.I)
ESS_PATTERN = re.compile(r"\be\.?s\.?s\b", re.I)


def _matches_card_hint(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    include = _contains_storage_keyword(text, CARD_INCLUDE)
    exclude = any(token in lower for token in NEGATIVE_KEYWORDS)
    return include and not exclude


def _is_bess_detail(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    include = _contains_storage_keyword(text, DETAIL_INCLUDE)
    exclude = any(token in lower for token in NEGATIVE_KEYWORDS)
    return include and not exclude


def _is_document_url(url: str) -> bool:
    return bool(DOC_URL_PATTERN.search((url or "").lower()))


def _contains_storage_keyword(text: str, keywords: List[str]) -> bool:
    if not text:
        return False
    if BESS_PATTERN.search(text) or ESS_PATTERN.search(text):
        return True
    lower = text.lower()
    return any(token in lower for token in keywords)


def _extract_article_text(detail_html: str) -> str:
    """
    Extrait uniquement le contenu pertinent de la fiche projet.

    Les pages SPIP incluent souvent, dans la colonne latérale ou en pied de page,
    des liens vers d'autres articles (déchetteries, campings, etc.) qui peuvent
    introduire de faux positifs dans les mots-clés négatifs. On restreint donc
    l'analyse au bloc central de la fiche lorsque c'est possible.
    """
    tree = HTMLParser(detail_html or "")
    selectors = [
        "div.contenu-article",
        "div.col-article",
        "article.article",
        "article",
        "main",
    ]
    for selector in selectors:
        node = tree.css_first(selector)
        if not node:
            continue
        text = node.text(separator=" ", strip=True)
        if text:
            return text
    return tree.text(separator=" ", strip=True)


def _parse_cards(page_html: str, base_url: str) -> List[Dict]:
    tree = HTMLParser(page_html)
    container = tree.css_first("div.liste-articles")
    if not container:
        return []

    cards = []
    for item in container.css("div.item-liste-articles"):
        title_node = item.css_first("h2.fr-card__title") or item.css_first("h3.fr-card__title")
        if not title_node:
            continue
        link = title_node.css_first("a")
        if not link:
            continue

        href = link.attributes.get("href", "").strip()
        if not href:
            continue

        description_node = item.css_first("p.fr-card__desc")
        description = (description_node.text() or "").strip() if description_node else ""
        cards.append(
            {
                "title": (link.text() or "").strip(),
                "url": _normalize_url(urljoin(base_url, href)),
                "description": description,
            }
        )
    return cards


def _extract_next_page(page_html: str, base_url: str) -> Optional[str]:
    tree = HTMLParser(page_html)
    next_link = tree.css_first("a.fr-pagination__link--next")
    if not next_link:
        return None

    href = next_link.attributes.get("href", "").strip()
    if not href:
        return None
    return _normalize_url(urljoin(base_url, href))


# ======================================================================
# Département
# ======================================================================
def _collect_department_projects(
    client,
    dept_code: str,
    dept_url: str,
    year: str,
) -> List[Project]:
    projects: List[Project] = []
    visited_pages = set()
    current_url = _normalize_url(dept_url)
    page_index = 1

    while current_url and current_url not in visited_pages and page_index <= MAX_PAGES_PER_DEPT:
        visited_pages.add(current_url)
        logger.info(f"[{dept_code}] ⏳ Page {page_index}: {current_url}")

        try:
            page_html, final_url = client.get_text(current_url)
        except Exception as exc:
            logger.error(f"[{dept_code}] ❌ Impossible de charger {current_url}: {exc}")
            break

        cards = _parse_cards(page_html, final_url)
        logger.info(f"[{dept_code}] ✓ {len(cards)} fiches récupérées")

        for card in cards:
            card_text = f"{card['title']} {card['description']}"
            if not _matches_card_hint(card_text):
                continue

            if _is_document_url(card["url"]):
                projects.append(
                    Project(
                        project_id=_sha1(card["url"]),
                        region="provence-alpes-cote-d-azur",
                        dept=dept_code,
                        year=year,
                        project_title=card["title"],
                        project_url=card["url"],
                    )
                )
                continue

            try:
                detail_html, final_detail_url = client.get_text(card["url"])
            except Exception as exc:
                logger.debug(f"[{dept_code}] ❌ Détail inaccessible {card['url']}: {exc}")
                continue

            detail_body = _extract_article_text(detail_html)
            detail_text = f"{card_text} {detail_body}".strip()

            if not _is_bess_detail(detail_text):
                continue

            projects.append(
                Project(
                    project_id=_sha1(final_detail_url),
                    region="provence-alpes-cote-d-azur",
                    dept=dept_code,
                    year=year,
                    project_title=card["title"],
                    project_url=_normalize_url(final_detail_url),
                )
            )

        next_page = _extract_next_page(page_html, final_url)
        if not next_page:
            break

        current_url = next_page
        page_index += 1
        time.sleep(DELAY)

    logger.info(f"[{dept_code}] ✅ {len(projects)} projets BESS retenus\n")
    return projects


# ======================================================================
# API nationale (2024+)
# ======================================================================
def _call_national_api(year: int, dept_filter: Optional[str] = None) -> List[Project]:
    logger.info(f"[API NATIONALE] Année={year} dept={dept_filter or 'TOUS'}")
    params = {
        "start": 0,
        "length": 200,
        "descending_order_id": "true",
        "place": "Provence-Alpes-Côte d'Azur",
        "searchAll": "stockage",
    }

    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as api_client:
            response = api_client.get(NATIONAL_GATEWAY_API, params=params)
            response.raise_for_status()
            data = response.json()
    except Exception as exc:
        logger.error(f"[API NATIONALE] ❌ Erreur: {exc}")
        return []

    items = data.get("data", []) if isinstance(data, dict) else []
    projects: List[Project] = []

    for item in items:
        title = item.get("projectTitle", "")
        department = item.get("department", "")
        municipality = item.get("municipality", "")
        description = item.get("description", "")
        combined = f"{title} {department} {municipality} {description}"
        if not _is_bess_detail(combined):
            continue

        published_date = item.get("publishedDate") or ""
        years = [int(y) for y in re.findall(r"\b(20\d{2})\b", published_date or title)]
        if year not in years:
            continue

        dept_code = _extract_dept_code(department or municipality)
        if dept_filter and dept_code and dept_code != dept_filter.zfill(2):
            continue
        if dept_code and dept_code not in PACA_DEPTS:
            continue

        document_id = item.get("documentId")
        project_url = ""
        if document_id:
            project_url = f"{NATIONAL_PORTAL_BASE}/#/public/view-document/{document_id}"

        if not project_url:
            project_url = item.get("url") or ""

        project = Project(
            project_id=_sha1(project_url or title),
            region="provence-alpes-cote-d-azur",
            dept=dept_code or (dept_filter.zfill(2) if dept_filter else ""),
            year=str(year),
            project_title=title[:200],
            project_url=project_url or "",
        )
        projects.append(project)
        logger.info(f"[API NATIONALE] ✓ {title[:80]}")

    logger.info(f"[API NATIONALE] ✅ {len(projects)} projets retenus")
    return projects


def _discover_from_api_portal(client, year: str, dept: Optional[str] = None) -> List[Project]:
    try:
        year_int = int(year)
    except ValueError:
        logger.error(f"[API NATIONALE] Année invalide: {year}")
        return []
    return _call_national_api(year_int, dept)


# ======================================================================
# Entrée publique
# ======================================================================
def discover_projects(
    year: str,
    client,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None,  # unused mais conservé pour compat
) -> List[Project]:
    logger.info(f"\n{'='*70}")
    logger.info(f"SCRAPER PACA - Année {year}")
    logger.info(f"{'='*70}\n")

    projects_map: Dict[str, Project] = {}

    def _append_projects(new_projects: List[Project]):
        for proj in new_projects:
            projects_map.setdefault(proj.project_url, proj)

    # 1) Pages statiques (2023/2024)
    if year in PACA_DEPT_URLS:
        target_depts = PACA_DEPT_URLS[year]
        if dept:
            dept_code = dept.zfill(2)
            if dept_code not in target_depts:
                logger.error(f"Département {dept_code} introuvable pour {year}")
                return []
            target_depts = {dept_code: target_depts[dept_code]}

        for dept_code, relative_path in sorted(target_depts.items()):
            dept_url = urljoin(BASE_URL + "/", relative_path)
            logger.info(f"📍 Département {dept_code} → {dept_url}")
            dept_projects = _collect_department_projects(
                client=client,
                dept_code=dept_code,
                dept_url=dept_url,
                year=year,
            )
            _append_projects(dept_projects)
            time.sleep(DELAY)
    else:
        logger.debug(f"Aucune page statique configurée pour {year}")

    # 2) API nationale pour 2024+ (et exclusif pour >=2025)
    if int(year) >= 2024:
        logger.info(f"\n🌐 Ajout résultats API nationale pour {year}")
        api_projects = _discover_from_api_portal(client, year, dept)
        _append_projects(api_projects)

    if not projects_map:
        logger.warning("Aucun projet PACA n'a été détecté")

    final_projects = list(projects_map.values())
    logger.info(f"\n{'='*70}")
    logger.info(f"✅ TOTAL PACA {year}: {len(final_projects)} projets (pages + API)")
    logger.info(f"{'='*70}\n")
    return final_projects
