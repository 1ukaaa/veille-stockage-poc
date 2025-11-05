# regions/grand_est.py
"""
Scraper DREAL Grand Est - VERSION PRODUCTION HYBRIDE CORRIGÉE
Architecture:
- Site régional: ANNÉE → DÉPARTEMENTS → PROJETS (2023-2024)
- API nationale: 2025+
"""
import re
import time
import hashlib
import logging
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse
from selectolax.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed

from models import Project

logger = logging.getLogger(__name__)

# Configuration
DELAY = 0.4
MAX_WORKERS = 5
MAX_PAGES = 50

# Site régional (2023-2024)
REGIONAL_SEED = "https://www.grand-est.developpement-durable.gouv.fr/avis-et-decisions-de-l-ae-r6433.html"

# API nationale (2025+)
NATIONAL_API_BASE = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/Get"
NATIONAL_PORTAL_BASE = "https://evaluation-environnementale.ecologie.gouv.fr"

# Départements Grand Est (10)
GRANDEST_DEPTS = ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"]


# ============ Utils ============

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _get_html_cached(client, url: str, cache: Dict) -> Tuple[str, str]:
    """Récupère HTML avec cache mémoire"""
    if url in cache:
        logger.debug(f"CACHE HIT: {url}")
        return cache[url]
    
    logger.info(f"GET {url}")
    html, final_url = client.get_text(url)
    time.sleep(DELAY)
    
    cache[url] = (html, final_url)
    return html, final_url


def _extract_title_from_tree(tree: HTMLParser) -> str:
    """Extrait titre H1"""
    h1 = tree.css_first("h1")
    return (h1.text() if h1 else "").strip()


def _is_bess_title(title: str) -> bool:
    """Validation BESS"""
    t = title.lower()
    
    has_stock = "stockag" in t
    has_batt = (
        "batter" in t or "bess" in t or
        re.search(r"\b2925(-?2)?\b", t) is not None or
        ("électricité" in t and "stockag" in t) or
        ("energie" in t and "stockag" in t)
    )
    is_pure_solar = (
        "photovolta" in t and "batter" not in t and
        "bess" not in t and "électricité" not in t
    )
    
    return has_stock and has_batt and not is_pure_solar


def _belongs_to_dept(title: str, url: str, dept_code: str) -> bool:
    """Vérifie appartenance département"""
    code = dept_code.zfill(2)
    if re.search(rf"\(\s*{code}\s*\)", title or ""):
        return True
    if re.search(rf"-{code}-", url or ""):
        return True
    return False


def _years_in_text(text: str) -> List[int]:
    """Extrait années depuis texte"""
    return [int(y) for y in re.findall(r"\b(20\d{2})\b", text or "")]


def _dept_from_text(text: str) -> str:
    """Extrait code département depuis texte"""
    # Pattern (XX)
    m = re.search(r"\((\d{2})\)", text)
    if m:
        return m.group(1)
    
    # Pattern "XX-Nom"
    m = re.search(r"(\d{2})-[A-Za-zÀ-ÿ\s-]+", text)
    if m:
        return m.group(1)
    
    # Noms départements Grand Est
    dept_map = {
        "ardennes": "08", "aube": "10", "marne": "51",
        "haute-marne": "52", "meurthe-et-moselle": "54",
        "meuse": "55", "moselle": "57", "bas-rhin": "67",
        "haut-rhin": "68", "vosges": "88"
    }
    
    text_lower = text.lower()
    for name, code in dept_map.items():
        if name in text_lower:
            return code
    
    return ""


# ============ API Portail National (2025) ============

def _call_national_api(
    year_target: int,
    dept_filter: Optional[str]
) -> List[Project]:
    """
    Appelle API gatew-evaluation-environnementale.developpement-durable.gouv.fr
    pour Grand Est
    """
    logger.info(f"[API NATIONALE] Recherche année={year_target}")
    
    import httpx
    
    projects = []
    
    try:
        params = {
            "start": 0,
            "length": 100,
            "descending_order_id": "true",
            "place": "Grand Est",
            "searchAll": "stockage"
        }
        
        client = httpx.Client(timeout=30.0, follow_redirects=True)
        response = client.get(NATIONAL_API_BASE, params=params)
        response.raise_for_status()
        
        data = response.json()
        client.close()
        
        items = data.get('data', [])
        total_count = data.get('totalCount', 0)
        
        logger.info(f"[API] {len(items)}/{total_count} items reçus")
        
        for item in items:
            project_title = item.get('projectTitle', '')
            department = item.get('department', '')
            municipality = item.get('municipality', '')
            reference_number = item.get('referenceNumber', '')
            published_date = item.get('publishedDate', '')
            document_id = item.get('documentId', '')
            
            combined = f"{project_title} {department} {municipality}"
            
            # Vérifier BESS
            if not _is_bess_title(combined):
                continue
            
            # Vérifier année
            years = _years_in_text(published_date or combined)
            if year_target not in years:
                continue
            
            # Extraire département
            dept_code = _dept_from_text(department or municipality or project_title)
            
            # Vérifier département Grand Est
            if dept_code and dept_code not in GRANDEST_DEPTS:
                continue
            
            # Filtre département utilisateur
            if dept_filter and dept_code != dept_filter.zfill(2):
                continue
            
            # Construire URL projet
            url = ""
            if document_id:
                url = f"{NATIONAL_PORTAL_BASE}/#/public/view-document/{document_id}"
            
            project = Project(
                project_id=_sha1(url or reference_number or project_title),
                region="grand-est",
                dept=dept_code or "",
                year=str(year_target),
                project_title=project_title,
                project_url=url
            )
            
            projects.append(project)
            logger.info(f"[API] ✓ {project_title[:60]}...")
        
    except Exception as e:
        logger.error(f"[API NATIONALE] Erreur: {e}", exc_info=True)
    
    logger.info(f"[API NATIONALE] Résultat: {len(projects)} projets")
    return projects


# ============ Site Régional (2023-2024) ============

def _extract_year_accordions(html: str, base_url: str) -> List[Dict]:
    """
    Extrait accordéons ANNÉES depuis page racine
    Pattern: "Décisions cas par cas projets en 2024"
    """
    tree = HTMLParser(html)
    out = []
    
    for section in tree.css("section.rubrique_avec_sous-rubriques-accordion"):
        button = section.css_first("button.rubrique_avec_sous-rubriques-accordion__btn")
        
        if not button:
            continue
        
        text_elem = button.css_first("p.fr-tile__title")
        
        if not text_elem:
            continue
        
        full_text = (text_elem.text() or "").strip()
        
        # Chercher année
        year_match = re.search(r"(\d{4})", full_text)
        if not year_match:
            continue
        
        found_year = year_match.group(1)
        accordion_id = button.attributes.get("aria-controls", "")
        
        if not accordion_id:
            continue
        
        out.append({
            "year": found_year,
            "label": full_text,
            "accordion_id": accordion_id,
            "html_source": html
        })
    
    return out


def _extract_dept_links_from_year_accordion(root_html: str, base_url: str, accordion_id: str) -> List[Dict]:
    """
    Extrait liens départements depuis collapse div année
    Pattern: <a href="ardennes-08-r7472.html">Ardennes (08)</a>
    """
    tree = HTMLParser(root_html)
    
    collapse_div = tree.css_first(f"#{accordion_id}")
    if not collapse_div:
        logger.warning(f"Collapse div {accordion_id} non trouvé")
        return []
    
    found = []
    
    for a in collapse_div.css("a.lien-sous-rubrique"):
        href = a.attributes.get("href", "")
        label = (a.text() or "").strip()
        
        if not href:
            continue
        
        # Extraire code département depuis label
        dept_match = re.search(r"\((\d{2})\)", label)
        if not dept_match:
            continue
        
        dept_code = dept_match.group(1)
        full_url = urljoin(base_url, href)
        
        found.append({
            "code": dept_code,
            "label": label,
            "url": full_url
        })
    
    return found


def _extract_project_links_from_list(list_html: str, base_url: str) -> List[str]:
    """Extrait liens fiches depuis page département-année"""
    tree = HTMLParser(list_html)
    out = []
    
    main_content = tree.css_first("main")
    if not main_content:
        main_content = tree.css_first("article")
    if not main_content:
        main_content = tree
    
    liste_articles = main_content.css_first("div.liste-articles")
    
    if liste_articles:
        for card in liste_articles.css("div.item-liste-articles"):
            h2 = card.css_first("h2.fr-card__title")
            if not h2:
                continue
            
            a = h2.css_first("a")
            if not a:
                continue
            
            href = a.attributes.get("href", "")
            if href:
                out.append(urljoin(base_url, href))
    else:
        for a in main_content.css("a"):
            href = a.attributes.get("href", "")
            text = (a.text() or "").strip().lower()
            
            if not href or not re.search(r"-a\d+\.html$", href):
                continue
            
            if any(kw in text or kw in href.lower() for kw in 
                   ['accessibilite', 'mentions', 'votre-avis', 'contact', 'plan', 'flux']):
                continue
            
            out.append(urljoin(base_url, href))
    
    return sorted(set(out))


def _find_next_page(list_html: str, base_url: str) -> Optional[str]:
    """Trouve lien page suivante"""
    tree = HTMLParser(list_html)
    for a in tree.css("a"):
        text = (a.text() or "").strip().lower()
        if any(kw in text for kw in ["page suivante", "suivant", "next"]):
            href = a.attributes.get("href", "")
            if href:
                return urljoin(base_url, href)
    return None


def _collect_project_urls_from_dept_page(client, dept_url: str, dept_code: str, cache: Dict, max_pages: int = 50) -> List[str]:
    """Collecte URLs fiches avec pagination"""
    all_urls = []
    next_url = dept_url
    seen = set()
    page_idx = 1
    
    while next_url and next_url not in seen and len(seen) < max_pages:
        seen.add(next_url)
        
        try:
            list_html, list_final = _get_html_cached(client, next_url, cache)
        except Exception as e:
            logger.error(f"[{dept_code}] Page list FAIL {next_url}: {e}")
            break
        
        proj_urls = _extract_project_links_from_list(list_html, list_final)
        logger.info(f"[{dept_code}] Page {page_idx}: {len(proj_urls)} fiches")
        all_urls.extend(proj_urls)
        
        next_url = _find_next_page(list_html, list_final)
        page_idx += 1
    
    logger.info(f"[{dept_code}] Total URLs collectées: {len(all_urls)}")
    return sorted(set(all_urls))


def _fetch_project_worker(args: tuple) -> Optional[Project]:
    """Worker parallèle"""
    client, proj_url, dept_code, year, cache = args
    
    try:
        proj_html, proj_final = _get_html_cached(client, proj_url, cache)
        tree = HTMLParser(proj_html)
        title = _extract_title_from_tree(tree)
        
        if not title or not _is_bess_title(title):
            return None
        
        if not _belongs_to_dept(title, proj_final, dept_code):
            return None
        
        logger.info(f"[{dept_code}] ✓ TROUVÉ: {title[:70]}...")
        
        return Project(
            project_id=_sha1(proj_final),
            region="grand-est",
            dept=dept_code,
            year=year,
            project_title=title,
            project_url=proj_final
        )
    
    except Exception as e:
        logger.error(f"[{dept_code}] Project fetch FAIL {proj_url}: {e}")
        return None


def _collect_year_regional(
    client,
    root_html: str,
    root_url: str,
    year: str,
    dept_filter: Optional[str],
    cache: Dict,
    max_pages: int = 50
) -> List[Project]:
    """Collecte année complète (site régional)"""
    projects = []
    
    # Trouver accordéon année
    year_accordions = _extract_year_accordions(root_html, root_url)
    year_entry = next((y for y in year_accordions if y["year"] == year), None)
    
    if not year_entry:
        logger.warning(f"Année {year} non trouvée sur page racine")
        return projects
    
    logger.info(f"✓ Année {year} trouvée: {year_entry['label']}")
    
    # Extraire liens départements
    dept_links = _extract_dept_links_from_year_accordion(
        root_html, root_url, year_entry["accordion_id"]
    )
    
    if not dept_links:
        logger.warning(f"Aucun département trouvé pour année {year}")
        return projects
    
    logger.info(f"✓ {len(dept_links)} départements trouvés")
    
    # Filtrer département si spécifié
    if dept_filter:
        code = dept_filter.zfill(2)
        dept_links = [d for d in dept_links if d["code"] == code]
        
        if not dept_links:
            logger.error(f"Département {code} non trouvé pour année {year}")
            return projects
        
        logger.info(f"Mode DÉPARTEMENT UNIQUE: {code}")
    else:
        logger.info(f"Mode TOUS DÉPARTEMENTS ({len(dept_links)} depts)")
    
    # Collecter projets par département
    for dept_link in sorted(dept_links, key=lambda x: x["code"]):
        dept_code = dept_link["code"]
        dept_url = dept_link["url"]
        
        logger.info(f"\n{'='*70}\nDépartement: {dept_code} - {dept_link['label']}\n{'='*70}")
        
        # Collecter URLs projets
        all_project_urls = _collect_project_urls_from_dept_page(
            client, dept_url, dept_code, cache, max_pages
        )
        
        if not all_project_urls:
            logger.warning(f"[{dept_code}] Aucune fiche trouvée")
            continue
        
        logger.info(f"[{dept_code}] Total {len(all_project_urls)} fiches à analyser")
        
        # Fetch parallèle
        args_list = [(client, url, dept_code, year, cache) for url in all_project_urls]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_project_worker, args): args[1] for args in args_list}
            
            for future in as_completed(futures):
                project = future.result()
                if project:
                    projects.append(project)
        
        logger.info(f"[{dept_code}] ✓ {len([p for p in projects if p.dept == dept_code])} projets BESS trouvés")
    
    return projects


def _scrape_regional_site(
    client,
    year_target: int,
    dept_filter: Optional[str],
    cache: Dict
) -> List[Project]:
    """
    Scrape site régional DREAL Grand Est
    Contient uniquement projets 2023-2024
    """
    logger.info(f"[SITE RÉGIONAL] année={year_target}")
    
    # Pour 2025+, projets uniquement sur portail national
    if year_target >= 2025:
        logger.info("[SITE RÉGIONAL] Année ≥2025 → projets sur portail national uniquement")
        return []
    
    year_str = str(year_target)
    
    try:
        root_html, root_url = _get_html_cached(client, REGIONAL_SEED, cache)
    except Exception as e:
        logger.error(f"Impossible de charger page racine: {e}")
        return []
    
    projects = _collect_year_regional(
        client, root_html, root_url, year_str, dept_filter, cache, MAX_PAGES
    )
    
    logger.info(f"[SITE RÉGIONAL] Résultat: {len(projects)} projets")
    return projects


# ============ API Plugin Principale ============

def discover_projects(
    year: str,
    client,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None
) -> List[Project]:
    """
    API principale scraper Grand Est
    
    Stratégie:
    - 2023-2024: Site régional DREAL Grand Est (ANNÉE → DÉPARTEMENTS → PROJETS)
    - 2025: API portail national
    - Fusion avec dédoublonnage
    """
    cache = {}
    
    try:
        year_target = int(year)
    except:
        logger.error(f"Année invalide: {year}")
        return []
    
    logger.info(
        f"\n{'='*70}\n"
        f"SCRAPER GRAND EST (VERSION HYBRIDE)\n"
        f"Année: {year_target}\n"
        f"Département: {dept or 'TOUS'}\n"
        f"{'='*70}"
    )
    
    # Site régional (2023-2024)
    regional = []
    if year_target <= 2024:
        regional = _scrape_regional_site(client, year_target, dept, cache)
    
    # API nationale (2025+)
    national = []
    if year_target >= 2025:
        national = _call_national_api(year_target, dept)
    
    # Fusion avec dédoublonnage
    seen_urls = set()
    all_projects = []
    
    for proj in regional + national:
        if proj.project_url not in seen_urls:
            seen_urls.add(proj.project_url)
            all_projects.append(proj)
    
    logger.info(
        f"\n{'='*70}\n"
        f"RÉSUMÉ GRAND EST\n"
        f"Année: {year_target}\n"
        f"Projets BESS trouvés: {len(all_projects)}\n"
        f"  - Site régional: {len(regional)}\n"
        f"  - Portail national: {len(national)}\n"
        f"{'='*70}\n"
    )
    
    return all_projects
