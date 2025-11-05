"""
Scraper DREAL Auvergne-Rhône-Alpes - VERSION OPTIMISÉE
Combine crawl robuste + parallélisme 15 workers + délai adaptatif
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

# ============ Configuration OPTIMISÉE ============
DELAY = 0.2  # 🚀 Via AdaptiveHTTPClient (baisse si succès)
MAX_WORKERS = 15  # 🚀 Augmenté: 5 → 15 workers
DEFAULT_SEED = "https://www.auvergne-rhone-alpes.developpement-durable.gouv.fr/projets-r3463.html"

# Hints année AURA (optimisation: accès direct)
YEAR_PAGE_HINTS = {
    "2025": "https://www.auvergne-rhone-alpes.developpement-durable.gouv.fr/2025-r6319.html",
    "2024": "https://www.auvergne-rhone-alpes.developpement-durable.gouv.fr/2024-r5914.html",
    "2023": "https://www.auvergne-rhone-alpes.developpement-durable.gouv.fr/2023-r5533.html",
}


# ============ Utils ============

def _sha1(text: str) -> str:
    """Génère SHA1 d'une chaîne"""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _get_html_cached(client, url: str, cache: Dict) -> Tuple[str, str]:
    """Récupère HTML avec cache mémoire"""
    if url in cache:
        logger.debug(f"CACHE HIT: {url}")
        return cache[url]
    
    logger.info(f"GET {url}")
    html, final_url = client.get_text(url)
    
    cache[url] = (html, final_url)
    return html, final_url


def _extract_title_from_tree(tree: HTMLParser) -> str:
    """Extrait titre H1"""
    h1 = tree.css_first("h1")
    return (h1.text() if h1 else "").strip()


def _is_bess_title(title: str) -> bool:
    """Validation BESS OPTIMALE"""
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


# ============ Découverte Page Racine ============

def _extract_department_links(html: str, base_url: str) -> List[Dict]:
    """Extrait liens vers pages départements"""
    tree = HTMLParser(html)
    out = []
    
    for a in tree.css("a"):
        href = a.attributes.get("href", "")
        label = (a.text() or "").strip()
        if not href:
            continue
        
        # Pattern 1: URL directe par département
        if re.search(r"/[\w\-\u00C0-\u017F]+-\d{2}-r\d+\.html(?:\?[^\s\"']+)?$", href):
            code_match = re.search(r"-(\d{2})-r\d+\.html", href)
            out.append({
                "url": urljoin(base_url, href),
                "label": label,
                "code": code_match.group(1) if code_match else None
            })
        # Pattern 2: Lien texte avec code département
        elif re.search(r"\(\s*\d{2}\s*\)", label):
            code_match = re.search(r"\((\d{2})\)", label)
            out.append({
                "url": urljoin(base_url, href),
                "label": label,
                "code": code_match.group(1).strip() if code_match else None
            })
    
    return out


def _crawl_find_dept_index(client, start_url: str, cache: Dict, max_pages: int = 50) -> Tuple[Optional[str], Optional[str], List[Dict]]:
    """Crawl dynamique pour trouver page \"Par département\""""
    domain = urlparse(start_url).netloc
    queue = [start_url]
    seen = set()
    
    while queue and len(seen) < max_pages:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        
        try:
            html, final_url = _get_html_cached(client, url, cache)
        except Exception as e:
            logger.warning(f"[crawl] skip {url}: {e}")
            continue
        
        links = _extract_department_links(html, final_url)
        
        # Heuristique: >= 5 départements trouvés = bonne page
        if len([x for x in links if x.get("code")]) >= 5:
            logger.info(f"✓ Page 'Par département' trouvée: {final_url} ({len(links)} liens)")
            return final_url, html, links
        
        # Ajouter liens pertinents au queue
        tree = HTMLParser(html)
        for a in tree.css("a"):
            href = a.attributes.get("href", "")
            if not href:
                continue
            
            full_url = urljoin(final_url, href)
            parsed = urlparse(full_url)
            
            if parsed.netloc == domain and full_url.startswith("http"):
                if any(keyword in full_url.lower() for keyword in [
                    "projet", "departement", "par-departement", "cas", "evaluation"
                ]):
                    queue.append(full_url)
    
    logger.error("Page 'Par département' introuvable")
    return None, None, []


# ============ Année -> Segments A-Z ============

def _slice_year_section(full_html: str, year: str) -> str:
    """Extrait section HTML pour année donnée"""
    m_start = re.search(rf"(^|\W){re.escape(year)}(\W|$)", full_html)
    if not m_start:
        return ""
    
    start = m_start.start()
    m_end = re.search(r"(?:^|\W)20\d{2}(?:\W|$)", full_html[start + len(year):])
    end = len(full_html) if not m_end else start + len(year) + m_end.start()
    
    return full_html[start:end]


def _extract_alpha_links_from_year_section(dept_html: str, base_url: str, year: str) -> List[str]:
    """Extrait liens A-Z depuis section année de page département"""
    section = _slice_year_section(dept_html, year)
    if not section:
        return []
    
    tree = HTMLParser(section)
    targets = {"A-G", "H-M", "N-S", "T-Z"}
    found = []
    
    for a in tree.css("a"):
        label = (a.text() or "").strip().upper()
        if label in targets:
            href = a.attributes.get("href", "")
            if href:
                found.append(urljoin(base_url, href))
    
    return sorted(set(found))


def _normalize_alpha_label(label: str) -> str:
    """Normalise labels alphabétiques"""
    s = (label or "").strip().lower()
    s = s.replace("–", "-").replace("—", "-").replace("à", "-")
    s = re.sub(r"\s+", "", s)
    s = s.replace(".", "")
    
    mapping = {
        "a-g": "A-G", "ag": "A-G",
        "h-m": "H-M", "hm": "H-M",
        "n-s": "N-S", "ns": "N-S",
        "t-z": "T-Z", "tz": "T-Z"
    }
    return mapping.get(s, "")


def _extract_alpha_links_from_yearpage(year_html: str, year_url: str) -> List[str]:
    """Extrait liens A-Z depuis page annuelle régionale"""
    tree = HTMLParser(year_html)
    found = {}
    
    for a in tree.css("a"):
        normalized = _normalize_alpha_label(a.text())
        if not normalized:
            continue
        
        href = a.attributes.get("href", "")
        if href and normalized not in found:
            found[normalized] = urljoin(year_url, href)
    
    result = [found[seg] for seg in ["A-G", "H-M", "N-S", "T-Z"] if seg in found]
    return result


def _find_year_page_url_in(html: str, base_url: str, year: str) -> Optional[str]:
    """Cherche URL page année dans HTML"""
    tree = HTMLParser(html)
    yr = str(year)
    
    for a in tree.css("a"):
        href = a.attributes.get("href", "")
        text = a.text() or ""
        if not href:
            continue
        
        if yr in text or re.search(rf"(?:^|/){yr}-r\d+\.html", href, re.I):
            return urljoin(base_url, href)
    
    match = re.search(rf'href=["\'](/?{yr}-r\d+\.html)(?:[#?][^\\"\']*)?["\']', html, re.I)
    if match:
        return urljoin(base_url, match.group(1))
    
    return None


def _find_year_page_url(client, year: str, cache: Dict, seed: str = DEFAULT_SEED) -> Optional[str]:
    """Trouve URL page annuelle avec fallback hints"""
    try:
        html, final_url = _get_html_cached(client, seed, cache)
        found = _find_year_page_url_in(html, final_url, year)
        if found:
            return found
    except Exception as e:
        logger.warning(f"Seed FAIL {seed}: {e}")
    
    # Fallback: YEAR_PAGE_HINTS
    hint = YEAR_PAGE_HINTS.get(str(year))
    if hint:
        logger.info(f"YEAR_HINT {year} -> {hint}")
        return hint
    
    return None


# ============ Navigation Listes ============

def _find_next_page(list_html: str, base_url: str) -> Optional[str]:
    """Trouve lien 'page suivante'"""
    tree = HTMLParser(list_html)
    for a in tree.css("a"):
        text = (a.text() or "").strip().lower()
        if "page suivante" in text or "suivant" in text:
            href = a.attributes.get("href", "")
            if href:
                return urljoin(base_url, href)
    return None


def _extract_project_links_from_list(list_html: str, base_url: str) -> List[str]:
    """Extrait liens fiches projet depuis page liste"""
    tree = HTMLParser(list_html)
    out = []
    
    for a in tree.css("a"):
        href = a.attributes.get("href", "")
        if re.search(r"-a\d+\.html(?:\?[^\s\"']+)?$", href):
            out.append(urljoin(base_url, href))
    
    return sorted(set(out))


# ============ Collecte URLs Fiches ============

def _collect_project_urls_from_alpha(client, alpha_urls: List[str], dept_code: str, cache: Dict, max_pages: int = 50) -> List[str]:
    """Collecte toutes URLs fiches depuis segments alphabétiques"""
    all_urls = []
    
    for alpha_url in alpha_urls:
        next_url = alpha_url
        seen = set()
        page_idx = 1
        
        while next_url and next_url not in seen and len(seen) < max_pages:
            seen.add(next_url)
            
            try:
                list_html, list_final = _get_html_cached(client, next_url, cache)
            except Exception as e:
                logger.error(f"[{dept_code}] Alpha page FAIL {next_url}: {e}")
                break
            
            proj_urls = _extract_project_links_from_list(list_html, list_final)
            logger.info(f"[{dept_code}] {alpha_url.split('/')[-1]} page {page_idx}: {len(proj_urls)} fiches")
            all_urls.extend(proj_urls)
            
            next_url = _find_next_page(list_html, list_final)
            page_idx += 1
    
    return sorted(set(all_urls))


# ============ Téléchargement Parallèle - 🚀 15 WORKERS ============

def _fetch_project_worker(args: tuple) -> Optional[Project]:
    """Worker parallèle pour téléchargement et validation fiche"""
    client, proj_url, dept_code, year, cache, fallback_yearpage = args
    
    try:
        proj_html, proj_final = _get_html_cached(client, proj_url, cache)
        tree = HTMLParser(proj_html)
        title = _extract_title_from_tree(tree)
        
        if not _is_bess_title(title):
            logger.debug(f"[{dept_code}] NON-BESS: {title}")
            return None
        
        if fallback_yearpage and not _belongs_to_dept(title, proj_final, dept_code):
            logger.debug(f"[{dept_code}] MAUVAIS-DEPT: {title}")
            return None
        
        logger.info(f"[{dept_code}] ✓ TROUVÉ: {title}")
        
        return Project(
            project_id=_sha1(proj_final),
            region="auvergne-rhone-alpes",
            dept=dept_code,
            year=year,
            project_title=title,
            project_url=proj_final
        )
    
    except Exception as e:
        logger.error(f"[{dept_code}] Project fetch FAIL {proj_url}: {e}")
        return None


# ============ Collecte Département HYBRIDE ============

def _collect_department_hybrid(
    client,
    dept_url: str,
    dept_code: str,
    year: str,
    cache: Dict,
    max_pages: int = 50
) -> List[Project]:
    """Collecte département avec stratégie hybride optimale"""
    projects = []
    
    try:
        dept_html, dept_final = _get_html_cached(client, dept_url, cache)
    except Exception as e:
        logger.error(f"[{dept_code}] Dept page FAIL {dept_url}: {e}")
        return projects
    
    alpha_urls = _extract_alpha_links_from_year_section(dept_html, dept_final, year)
    fallback_yearpage = False
    
    if not alpha_urls:
        logger.warning(f"[{dept_code}] Aucun lien A-Z {year} -> fallback page annuelle")
        
        year_page = _find_year_page_url_in(dept_html, dept_final, year)
        
        if not year_page:
            year_page = _find_year_page_url(client, year, cache, seed=DEFAULT_SEED)
        
        if year_page:
            try:
                year_html, year_final = _get_html_cached(client, year_page, cache)
                alpha_urls = _extract_alpha_links_from_yearpage(year_html, year_final)
                fallback_yearpage = True
                logger.info(f"[{dept_code}] ✓ {len(alpha_urls)} segments A-Z trouvés via page annuelle")
            except Exception as e:
                logger.error(f"[{dept_code}] Year page FAIL {year_page}: {e}")
        else:
            logger.error(f"[{dept_code}] Page annuelle {year} introuvable")
            return projects
    
    if not alpha_urls:
        logger.warning(f"[{dept_code}] Aucune URL alphabétique pour {year}")
        return projects
    
    logger.info(f"[{dept_code}] {len(alpha_urls)} segments alphabétiques à explorer")
    
    all_project_urls = _collect_project_urls_from_alpha(
        client, alpha_urls, dept_code, cache, max_pages
    )
    
    if not all_project_urls:
        logger.warning(f"[{dept_code}] Aucune fiche trouvée")
        return projects
    
    logger.info(f"[{dept_code}] Total: {len(all_project_urls)} fiches à analyser")
    
    # 🚀 Parallélisation MAX_WORKERS = 15
    logger.info(f"[{dept_code}] Téléchargement parallèle ({MAX_WORKERS} workers)...")
    
    args_list = [
        (client, url, dept_code, year, cache, fallback_yearpage)
        for url in all_project_urls
    ]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_project_worker, args): args[1] for args in args_list}
        
        for future in as_completed(futures):
            project = future.result()
            if project:
                projects.append(project)
    
    logger.info(f"[{dept_code}] ✓ {len(projects)} projets BESS trouvés")
    return projects


# ============ API Plugin ============

def discover_projects(
    year: str,
    client,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None
) -> List[Project]:
    """API principale scraper AURA - VERSION OPTIMISÉE
    
    Args:
        year: Année à scraper (ex: "2024")
        client: HTTPClient ou AdaptiveHTTPClient
        dept: Code département optionnel (ex: "01")
        seed_url: URL de départ personnalisée
    
    Returns:
        List[Project]: Projets BESS trouvés
    """
    cache = {}
    start_url = seed_url or DEFAULT_SEED
    max_pages = 50
    
    logger.info(
        f"\n{'='*70}\n"
        f"SCRAPER AURA (OPTIMISÉ)\n"
        f"Année: {year}\n"
        f"Département: {dept or 'TOUS'}\n"
        f"Workers: {MAX_WORKERS} | Délai: {DELAY}s adaptatif\n"
        f"{'='*70}"
    )
    
    # Étape 1: Trouver page "Par département"
    logger.info("Recherche page 'Par département'...")
    dept_index_url, dept_index_html, dept_links = _crawl_find_dept_index(
        client, start_url, cache, max_pages=max_pages
    )
    
    if not dept_index_url:
        logger.error("Impossible de trouver la page 'Par département'")
        return []
    
    depts = [d for d in dept_links if d.get("code")]
    logger.info(f"✓ {len(depts)} départements disponibles")
    
    # Étape 2a: Mode département spécifique
    if dept:
        code = dept.zfill(2)
        target = next((d for d in depts if d["code"] == code), None)
        
        if not target:
            logger.error(f"Département {code} non trouvé")
            return []
        
        logger.info(f"Mode département unique: {code}")
        return _collect_department_hybrid(
            client, target["url"], code, year, cache, max_pages
        )
    
    # Étape 2b: Mode tous départements
    logger.info(f"Mode TOUS départements ({len(depts)} depts)")
    all_projects = []
    
    for d in sorted(depts, key=lambda x: x["code"]):
        logger.info(f"\n{'='*60}\nDépartement: {d['code']}\n{'='*60}")
        
        projects = _collect_department_hybrid(
            client, d["url"], d["code"], year, cache, max_pages
        )
        all_projects.extend(projects)
    
    logger.info(f"\n{'='*60}\nTOTAL AURA: {len(all_projects)} projets BESS\n{'='*60}")
    
    return all_projects
