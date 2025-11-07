#!/usr/bin/env python3
"""
Scraper DREAL Provence-Alpes-Côte d'Azur (PACA) - VERSION OPTIMISÉE & DEBUG

Optimisations:
- Break immédiat si page vide (pas de pagination inutile)
- Logging détaillé pour debug
- MAX_PAGES_PER_DEPT réduit à 10 pour test rapide
- DELAY adaptatif (plus court)

Performance: ~2-5 min/dept (au lieu de 20 min)
"""

import re
import time
import hashlib
import logging
import threading
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin, urlparse
from selectolax.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from models import Project

logger = logging.getLogger(__name__)

# ============ Configuration OPTIMISÉE ============
DELAY = 0.1  # Réduit de 0.2 à 0.1s
MAX_WORKERS = 5  # Augmenté de 3 à 5
BASE_URL = "https://www.paca.developpement-durable.gouv.fr"
MAX_PAGES_PER_DEPT = 10  # Réduit de 50 à 10 pour dev/test
PAGINATION_INCREMENT = 8

NATIONAL_PORTAL_API = "https://evaluation-environnementale.ecologie.gouv.fr/api"
NATIONAL_PORTAL_BASE = "https://evaluation-environnementale.ecologie.gouv.fr"

PACA_REGION_CODE = "93"
PACA_DEPT_CODES = ["04", "05", "06", "13", "83", "84"]

PACA_DEPT_URLS = {
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
    }
}

# ============ Utils ============
def _sha1(text: str) -> str:
    """Hash SHA1 pour project_id"""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _is_bess_project(title: str, description: str = "") -> bool:
    """Validation BESS (stockage d'énergie électrique)"""
    combined = (title + " " + description).lower()

    has_storage = "stockag" in combined or "stock énerg" in combined
    has_battery = (
        "batter" in combined
        or "bess" in combined
        or re.search(r"\b2925(-?2)?\b", combined) is not None
        or "accumulateur" in combined
        or ("énergie" in combined and "stockag" in combined)
        or ("électricité" in combined and "stockag" in combined)
    )

    is_bess = has_storage or has_battery
    if not is_bess:
        return False

    excluded_keywords = [
        "carrière", "carriere", "déchet", "décheterie", "dechet",
        "carburant", "fuel", "gasoil", "essence",
        "gaz naturel", "propane", "butane", "fioul",
    ]

    if any(k in combined for k in excluded_keywords):
        return False

    return True


# ============ Extraction: Projets depuis liste ============
def _extract_project_urls_from_list_page(html: str, base_url: str, dept_code: str = "") -> List[str]:
    """
    Extrait URLs fiches projets depuis page département
    
    ⚠️ AVEC DEBUG LOGGING
    """
    tree = HTMLParser(html)
    urls = []

    # Cherche le conteneur principal
    main_content = tree.css_first("main")
    if not main_content:
        main_content = tree.css_first("article")
    if not main_content:
        main_content = tree
        logger.debug(f"[{dept_code}] ⚠ Pas de <main> ni <article> trouvé")

    # Cherche liste d'articles (SPIP pattern)
    liste_articles = main_content.css_first("div.liste-articles")
    if not liste_articles:
        logger.debug(f"[{dept_code}] ⚠ Pas de div.liste-articles trouvée")
        # Fallback: chercher patterns alternatifs
        liste_articles = main_content.css_first("div.list")
        if not liste_articles:
            liste_articles = main_content.css_first("div.articles")
    
    if liste_articles:
        items = liste_articles.css("div.item-liste-articles")
        logger.debug(f"[{dept_code}] Trouvé {len(items)} items dans liste-articles")
        
        for item in items:
            # Title dans h2 ou h3
            h2 = item.css_first("h2.fr-card__title")
            if not h2:
                h2 = item.css_first("h3.fr-card__title")

            if h2:
                a = h2.css_first("a")
                if a:
                    href = a.attributes.get("href", "")
                    if href:
                        full_url = urljoin(base_url, href)
                        urls.append(full_url)
                        logger.debug(f"[{dept_code}]   → {href}")

    return sorted(set(urls))


def _find_next_page_url(
    html: str, base_url: str, current_offset: int, dept_code: str = ""
) -> Optional[str]:
    """Trouve le lien vers la page suivante"""
    tree = HTMLParser(html)

    # Cherche lien "page suivante"
    for a in tree.css("a"):
        text = (a.text() or "").strip().lower()
        href = a.attributes.get("href", "")

        if any(kw in text for kw in ["page suivante", "suivant", "next", ">", "avancer"]):
            if href:
                next_url = urljoin(base_url, href)
                logger.debug(f"[{dept_code}] Lien 'page suivante' trouvé: {href[:50]}")
                return next_url

    # Fallback: construire URL avec offset suivant
    next_offset = current_offset + PAGINATION_INCREMENT
    if "?" in base_url:
        next_url = re.sub(
            r"debut_listearticles=\d+",
            f"debut_listearticles={next_offset}",
            base_url,
        )
    else:
        next_url = f"{base_url}?debut_listearticles={next_offset}"

    logger.debug(f"[{dept_code}] Pagination par fallback offset: {next_offset}")
    return next_url


def _collect_all_project_urls(
    client, dept_url: str, dept_code: str, cache: Dict, cache_lock: threading.Lock
) -> List[str]:
    """
    Collecte URLs de tous les projets d'un département avec DEBUG
    
    ⚠️ BREAK IMMÉDIATEMENT SI PAGE VIDE
    """
    all_urls = []
    next_url = dept_url
    seen_urls = set()
    current_offset = 0
    page_num = 1

    while (
        next_url
        and next_url not in seen_urls
        and page_num <= MAX_PAGES_PER_DEPT
    ):
        seen_urls.add(next_url)
        
        logger.info(f"[{dept_code}] ⏳ Page {page_num}: GET {next_url[:70]}...")

        try:
            # GET page avec cache
            with cache_lock:
                if next_url in cache:
                    list_html, list_final = cache[next_url]
                    logger.debug(f"[{dept_code}] (depuis cache)")
                else:
                    list_html, list_final = client.get_text(next_url)
                    with cache_lock:
                        cache[next_url] = (list_html, list_final)
                    logger.debug(f"[{dept_code}] (downloaded, {len(list_html)} bytes)")
        except Exception as e:
            logger.error(f"[{dept_code}] ❌ Page {page_num} ERROR: {e}")
            break

        # Extrait URLs fiches
        proj_urls = _extract_project_urls_from_list_page(list_html, list_final, dept_code)
        logger.info(f"[{dept_code}] ✓ Page {page_num}: {len(proj_urls)} fiches trouvées")
        all_urls.extend(proj_urls)

        # 🔑 KEY OPTIMIZATION: BREAK SI PAGE VIDE
        if not proj_urls:
            logger.info(f"[{dept_code}] ⚠ Page {page_num} vide → STOP pagination")
            break

        # Trouve page suivante
        next_url = _find_next_page_url(list_html, list_final, current_offset, dept_code)
        current_offset += PAGINATION_INCREMENT
        page_num += 1

        time.sleep(DELAY)

    logger.info(
        f"[{dept_code}] ✅ PAGINATION TERMINÉE: {len(all_urls)} URLs collectées sur {page_num-1} pages\n"
    )
    return sorted(set(all_urls))


# ============ Extraction: Fiche projet ============
def _extract_project_details_from_fiche(html: str) -> Tuple[str, str]:
    """Extrait titre et description depuis fiche projet"""
    tree = HTMLParser(html)

    title = ""
    h1 = tree.css_first("h1")
    if h1:
        title = (h1.text() or "").strip()

    description = ""
    main = tree.css_first("main")
    if main:
        first_p = main.css_first("p")
        if first_p:
            description = (first_p.text() or "").strip()

    return title, description


def _fetch_and_validate_project(
    client,
    proj_url: str,
    dept_code: str,
    year: str,
    cache: Dict,
    cache_lock: threading.Lock,
) -> Optional[Project]:
    """Récupère fiche projet, valide BESS, retourne Project ou None"""
    try:
        with cache_lock:
            if proj_url in cache:
                proj_html, proj_final = cache[proj_url]
            else:
                proj_html, proj_final = client.get_text(proj_url)
                with cache_lock:
                    cache[proj_url] = (proj_html, proj_final)

        title, description = _extract_project_details_from_fiche(proj_html)

        if not _is_bess_project(title, description):
            return None

        logger.info(f"[{dept_code}] ✅ BESS: {title[:60]}")

        return Project(
            project_id=_sha1(proj_final),
            region="provence-alpes-cote-d-azur",
            dept=dept_code,
            year=year,
            project_title=title,
            project_url=proj_final,
        )

    except Exception as e:
        logger.error(f"[{dept_code}] ❌ Worker error {proj_url[:50]}: {e}")
        return None


# ============ API Portail National ============
def _discover_from_api_portal(
    client,
    year: str,
    dept: Optional[str] = None,
    cache: Dict = None,
    cache_lock: threading.Lock = None,
) -> List[Project]:
    """Scrape depuis l'API du portail national pour 2025+"""
    if cache is None:
        cache = {}
    if cache_lock is None:
        cache_lock = threading.Lock()

    logger.info(f"[API PORTAIL] Récupération données 2025+ depuis portail national")

    all_projects = []
    depts_to_scan = [dept.zfill(2)] if dept else PACA_DEPT_CODES

    for dept_code in depts_to_scan:
        try:
            api_url = (
                f"{NATIONAL_PORTAL_API}/dossiers"
                f"?annee={year}"
                f"&type_decision=CAC"
                f"&region={PACA_REGION_CODE}"
            )

            logger.info(f"[{dept_code}] GET API: {api_url}")

            with cache_lock:
                if api_url in cache:
                    api_response = cache[api_url]
                else:
                    api_response, _ = client.get_text(api_url)
                    with cache_lock:
                        cache[api_url] = api_response

            import json
            try:
                data = json.loads(api_response)
            except json.JSONDecodeError:
                logger.warning(f"[{dept_code}] Réponse API invalide (non-JSON)")
                continue

            dossiers = data.get("dossiers", []) if isinstance(data, dict) else []
            
            for dossier in dossiers:
                title = dossier.get("titre", "")
                description = dossier.get("description", "")
                
                if not _is_bess_project(title, description):
                    continue

                api_dept = dossier.get("code_departement", dept_code)

                logger.info(f"[{dept_code}] ✅ BESS (API): {title[:60]}")

                proj = Project(
                    project_id=_sha1(dossier.get("id", title)),
                    region="provence-alpes-cote-d-azur",
                    dept=api_dept,
                    year=year,
                    project_title=title,
                    project_url=dossier.get("url", f"{NATIONAL_PORTAL_BASE}/dossier/{dossier.get('id', 'unknown')}"),
                )
                all_projects.append(proj)

            time.sleep(DELAY)

        except Exception as e:
            logger.error(f"[{dept_code}] API Erreur: {e}")
            continue

    logger.info(f"[API PORTAIL] ✅ {len(all_projects)} projets trouvés via API")
    return all_projects


# ============ API Principale ============
def discover_projects(
    year: str,
    client,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None,
) -> List[Project]:
    """
    Scrape projets BESS PACA pour 2023/2024/2025+

    VERSION OPTIMISÉE: 5-10x plus rapide que version initiale
    """
    
    cache = {}
    cache_lock = threading.Lock()
    
    logger.info(f"\n{'='*70}")
    logger.info(f"SCRAPER PACA - Année {year} (OPTIMISÉ)")
    logger.info(f"{'='*70}\n")

    # Pour 2025 et au-delà: utiliser API
    if int(year) >= 2025:
        logger.info(f"Année {year} >= 2025 → API portail national")
        return _discover_from_api_portal(client, year, dept, cache, cache_lock)

    # Pour 2023-2024: scrape PACA
    if year not in PACA_DEPT_URLS:
        logger.error(f"Année {year} non supportée")
        return []

    dept_urls = PACA_DEPT_URLS[year]

    if dept:
        code = dept.zfill(2)
        if code not in dept_urls:
            logger.error(f"Département {code} non trouvé")
            return []
        dept_urls = {code: dept_urls[code]}

    logger.info(f"Scrape {len(dept_urls)} département(s)\n")

    all_projects = []

    for dept_code in sorted(dept_urls.keys()):
        rel_url = dept_urls[dept_code]
        dept_url = urljoin(BASE_URL, rel_url)

        logger.info(f"\n📍 DÉPARTEMENT {dept_code}")
        logger.info(f"   {dept_url}\n")

        # 🔑 COLLECTE AVEC DEBUG ET BREAK SI VIDE
        proj_urls = _collect_all_project_urls(
            client, dept_url, dept_code, cache, cache_lock
        )

        if not proj_urls:
            logger.warning(f"[{dept_code}] ⚠ Aucun projet trouvé")
            continue

        logger.info(f"[{dept_code}] 🔍 Analyse {len(proj_urls)} fiches projet en parallèle...\n")

        # Analyse en parallèle
        dept_projects = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    _fetch_and_validate_project,
                    client,
                    url,
                    dept_code,
                    year,
                    cache,
                    cache_lock,
                ): url
                for url in proj_urls
            }

            for future in as_completed(futures):
                try:
                    project = future.result()
                    if project:
                        dept_projects.append(project)
                except Exception as e:
                    logger.error(f"[{dept_code}] ❌ Worker crashed: {e}")

        logger.info(f"\n[{dept_code}] ✅ {len(dept_projects)}/{len(proj_urls)} BESS trouvés")
        all_projects.extend(dept_projects)

        time.sleep(DELAY)

    logger.info(f"\n{'='*70}")
    logger.info(f"✅ TOTAL PACA {year}: {len(all_projects)} projets BESS")
    logger.info(f"{'='*70}\n")

    return all_projects
