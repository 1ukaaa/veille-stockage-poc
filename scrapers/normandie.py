"""
Scraper DREAL Normandie - VERSION PRODUCTION COMPLÈTE ET CORRIGÉE
Architecture: Page racine → accordéons depts → années (avec navigation années archives) → fiches projets
Logique AURA adaptée + handling spécial pour années archivées (2023-2016)
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
DEFAULT_SEED = "https://www.normandie.developpement-durable.gouv.fr/les-decisions-apres-examen-au-cas-par-cas-des-r326.html"


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
    """Validation BESS - Identique AURA"""
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
    """
    Extrait liens département depuis accordéons
    Pattern: <button aria-controls="accordion-listerubXXX">
               <p class="fr-tile__title">Calvados (14)</p>
             </button>
    """
    tree = HTMLParser(html)
    out = []
    
    for button in tree.css("button.rubrique_avec_sous-rubriques-accordion__btn"):
        text_elem = button.css_first("p.fr-tile__title")
        
        if not text_elem:
            continue
        
        full_text = (text_elem.text() or "").strip()
        
        # Pattern: "Calvados (14)"
        match = re.search(r"([A-Za-zÀ-ÿ\s\-]+)\s*\(\s*(\d{2})\s*\)", full_text)
        if not match:
            continue
        
        dept_name = match.group(1).strip()
        dept_code = match.group(2).strip()
        accordion_id = button.attributes.get("aria-controls", "")
        
        if not accordion_id:
            continue
        
        out.append({
            "url": base_url,
            "label": full_text,
            "code": dept_code,
            "name": dept_name,
            "accordion_id": accordion_id,
            "html_source": html
        })
    
    return out


def _crawl_find_dept_index(client, start_url: str, cache: Dict, max_pages: int = 50) -> Tuple[Optional[str], Optional[str], List[Dict]]:
    """
    Crawl dynamique AURA-style pour trouver page avec accordéons depts
    """
    try:
        html, final_url = _get_html_cached(client, start_url, cache)
    except Exception as e:
        logger.error(f"Crawl FAIL {start_url}: {e}")
        return None, None, []
    
    links = _extract_department_links(html, final_url)
    
    # Heuristique: page avec >= 5 départements
    if len([x for x in links if x.get("code")]) >= 5:
        logger.info(f"✓ Page racine avec depts trouvée: {final_url} ({len(links)} liens)")
        return final_url, html, links
    
    # Fallback: crawler
    logger.warning(f"Seulement {len(links)} depts trouvés, crawl interne...")
    
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
        
        if len([x for x in links if x.get("code")]) >= 5:
            logger.info(f"✓ Page depts trouvée après crawl: {final_url}")
            return final_url, html, links
        
        tree = HTMLParser(html)
        for a in tree.css("a"):
            href = a.attributes.get("href", "")
            if not href:
                continue
            
            full_url = urljoin(final_url, href)
            parsed = urlparse(full_url)
            
            if parsed.netloc == domain and full_url.startswith("http"):
                queue.append(full_url)
    
    logger.error("Impossible de trouver page avec >=5 depts")
    return None, None, []


# ============ Découverte Années (CORRIGÉ avec handling archives) ============

def _find_year_links_in_accordion(root_html: str, base_url: str, accordion_id: str) -> List[Dict]:
    """
    Cherche liens années dans le collapse div du département
    IMPORTANT: Gère 2 cas:
    - Années uniques (2025, 2024): URL directe vers fiches
    - Années archives (2023-2016): URL intermédiaire qui demande navigation supplémentaire
    """
    tree = HTMLParser(root_html)
    
    # Chercher le div avec cet ID
    collapse_div = tree.css_first(f"#{accordion_id}")
    if not collapse_div:
        logger.warning(f"Collapse div {accordion_id} non trouvé")
        return []
    
    found = []
    
    # Extraire les <a> dans ce collapse
    for a in collapse_div.css("a"):
        href = a.attributes.get("href", "")
        label = (a.text() or "").strip()
        
        if not href:
            continue
        
        # Chercher année dans le label
        year_match = re.search(r"(\d{4})", label)
        if not year_match:
            continue
        
        found_year = year_match.group(1)
        full_url = urljoin(base_url, href)
        
        found.append({
            "year": found_year,
            "label": label,
            "url": full_url,
            "href": href
        })
    
    return found


def _resolve_year_url(client, year_entry: Dict, target_year: str, cache: Dict) -> Optional[str]:
    """
    CRITIQUE: Résout l'URL réelle de l'année demandée
    
    Gère 2 cas:
    1. URL directe: "annee-2024-r1594.html" → utilisée directement
    2. URL archivée: "acces-aux-decisions-...-2023-a-2016-r1702.html"
       → DOIT naviguer DANS cette page pour trouver "2023-r1522.html"
    """
    url = year_entry["url"]
    href = year_entry["href"]
    found_year = year_entry["year"]
    
    # CAS 1: URL directe par année (2025, 2024) - utiliser directement
    if f"annee-{target_year}" in href or f"{target_year}-r" in href:
        logger.debug(f"Année {target_year}: URL directe trouvée")
        return url
    
    # CAS 2: URL archivée (contient "acces-aux") - NAVIGATION SUPPLÉMENTAIRE
    if "acces-aux" in href:
        logger.info(f"Année {target_year}: URL archivée détectée, navigation supplémentaire...")
        
        try:
            archive_html, archive_final = _get_html_cached(client, url, cache)
        except Exception as e:
            logger.error(f"Impossible d'accéder page archive {url}: {e}")
            return None
        
        # Chercher le lien de l'année dans cette page
        # Pattern: <a href="2023-r1522.html">...</a>
        tree = HTMLParser(archive_html)
        
        for a in tree.css("a"):
            href_inner = a.attributes.get("href", "")
            text_inner = (a.text() or "").strip()
            
            # Chercher l'année exacte dans le href
            if re.search(rf"^{target_year}-r\d+\.html$", href_inner):
                real_url = urljoin(archive_final, href_inner)
                logger.info(f"Année {target_year}: URL réelle trouvée: {href_inner}")
                return real_url
        
        logger.warning(f"Année {target_year} non trouvée dans page archive")
        return None
    
    # DÉFAUT: retourner l'URL trouvée
    return url


# ============ Extraction Fiches ============

def _extract_project_links_from_list(list_html: str, base_url: str) -> List[str]:
    """
    Extrait liens fiches depuis page année/département
    NORMANDIE PATTERN:
    <div class="liste-articles">
      <div class="item-liste-articles fr-card">
        <h2 class="fr-card__title">
          <a href="creation-d-un-forage-...-a5307.html">...</a>
        </h2>
      </div>
    </div>
    
    Filtre: UNIQUEMENT liens du contenu principal, pas navigation
    """
    tree = HTMLParser(list_html)
    out = []
    
    # Chercher le conteneur principal
    main_content = tree.css_first("main")
    if not main_content:
        main_content = tree.css_first("article")
    if not main_content:
        main_content = tree
    
    # Chercher liste-articles
    liste_articles = main_content.css_first("div.liste-articles")
    
    if liste_articles:
        # Extraire liens des cartes de projets
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
        # Fallback: chercher liens -aXXXX.html dans main, sans pages nav
        for a in main_content.css("a"):
            href = a.attributes.get("href", "")
            text = (a.text() or "").strip().lower()
            
            if not href or not re.search(r"-a\d+\.html$", href):
                continue
            
            # Exclure navigation/footer
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


def _collect_project_urls_from_year(client, year_url: str, dept_code: str, cache: Dict, max_pages: int = 50) -> List[str]:
    """Collecte URLs fiches avec pagination"""
    all_urls = []
    next_url = year_url
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


# ============ Téléchargement Parallèle ============

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
            region="normandie",
            dept=dept_code,
            year=year,
            project_title=title,
            project_url=proj_final
        )
    
    except Exception as e:
        logger.error(f"[{dept_code}] Project fetch FAIL {proj_url}: {e}")
        return None


# ============ Collecte Département ============

def _collect_department_hybrid(
    client,
    root_html: str,
    root_url: str,
    dept_code: str,
    dept_name: str,
    accordion_id: str,
    year: str,
    cache: Dict,
    max_pages: int = 50
) -> List[Project]:
    """Collecte département complet avec résolution années archivées"""
    projects = []
    
    # Étape 1: Chercher années dans accordéon
    year_links = _find_year_links_in_accordion(root_html, root_url, accordion_id)
    
    if not year_links:
        logger.warning(f"[{dept_code}] Aucune année trouvée")
        return projects
    
    logger.info(f"[{dept_code}] Années trouvées: {[y['year'] for y in year_links]}")
    
    # Chercher l'année exacte
    year_entry = next((y for y in year_links if y["year"] == year), None)
    
    if not year_entry:
        logger.warning(f"[{dept_code}] Année {year} non trouvée")
        return projects
    
    # Étape 2: RÉSOUDRE l'URL réelle (gère cas archives)
    year_url = _resolve_year_url(client, year_entry, year, cache)
    
    if not year_url:
        logger.warning(f"[{dept_code}] Impossible de résoudre URL année {year}")
        return projects
    
    logger.info(f"[{dept_code}] URL année {year} résolue: OK")
    
    # Étape 3: Collecter URLs fiches
    all_project_urls = _collect_project_urls_from_year(client, year_url, dept_code, cache, max_pages)
    
    if not all_project_urls:
        logger.warning(f"[{dept_code}] Aucune fiche trouvée")
        return projects
    
    logger.info(f"[{dept_code}] Total {len(all_project_urls)} fiches à analyser")
    
    # Étape 4: Téléchargement parallèle
    args_list = [(client, url, dept_code, year, cache) for url in all_project_urls]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_project_worker, args): args[1] for args in args_list}
        
        for future in as_completed(futures):
            project = future.result()
            if project:
                projects.append(project)
    
    logger.info(f"[{dept_code}] ✓ {len(projects)} projets BESS trouvés")
    return projects


# ============ API Plugin Principal ============

def discover_projects(
    year: str,
    client,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None
) -> List[Project]:
    """
    API principale scraper Normandie - Logique AURA complète et corrigée
    
    Architecture:
    1. Crawl page racine avec accordéons depts
    2. Extraction années de chaque dept (accordéon)
    3. RÉSOLUTION années archivées (navigation supplémentaire)
    4. Collecte fiches avec pagination
    5. Validation BESS + appartenance dept
    6. Téléchargement parallèle
    """
    cache = {}
    start_url = seed_url or DEFAULT_SEED
    max_pages = 50
    
    logger.info(
        f"\n{'='*70}\n"
        f"SCRAPER NORMANDIE (VERSION CORRIGÉE)\n"
        f"Année: {year}\n"
        f"Département: {dept or 'TOUS'}\n"
        f"{'='*70}"
    )
    
    # Étape 1: Crawl find page racine
    logger.info("Crawl recherche page avec accordéons depts...")
    
    dept_page_url, dept_page_html, dept_links = _crawl_find_dept_index(
        client, start_url, cache, max_pages=max_pages
    )
    
    if not dept_page_url or not dept_page_html:
        logger.error("Impossible de trouver page avec depts")
        return []
    
    depts = [d for d in dept_links if d.get("code")]
    logger.info(f"✓ {len(depts)} départements découverts")
    
    # Étape 2: Mode département spécifique
    if dept:
        code = dept.zfill(2)
        target = next((d for d in depts if d["code"] == code), None)
        
        if not target:
            logger.error(f"Département {code} non trouvé")
            return []
        
        logger.info(f"Mode DÉPARTEMENT UNIQUE: {code}")
        
        return _collect_department_hybrid(
            client,
            dept_page_html,
            dept_page_url,
            code,
            target["name"],
            target["accordion_id"],
            year,
            cache,
            max_pages
        )
    
    # Étape 3: Mode TOUS
    logger.info(f"Mode TOUS DÉPARTEMENTS ({len(depts)} depts)")
    all_projects = []
    
    for d in sorted(depts, key=lambda x: x["code"]):
        logger.info(f"\n{'='*70}\nDépartement: {d['code']} - {d['name']}\n{'='*70}")
        
        projects = _collect_department_hybrid(
            client,
            dept_page_html,
            dept_page_url,
            d["code"],
            d["name"],
            d["accordion_id"],
            year,
            cache,
            max_pages
        )
        all_projects.extend(projects)
    
    logger.info(f"\n{'='*70}\nRÉSUMÉ NORMANDIE - Année {year}\nTotal: {len(all_projects)} projets BESS\n{'='*70}\n")
    
    return all_projects
