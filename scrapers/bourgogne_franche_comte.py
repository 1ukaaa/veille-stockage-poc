# regions/bourgogne_franche_comte.py
"""
Scraper DREAL Bourgogne-Franche-Comté - VERSION FINALE PRODUCTION
Utilise l'API gatew-evaluation-environnementale.developpement-durable.gouv.fr
"""
import re
import time
import hashlib
import logging
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin
from selectolax.parser import HTMLParser

from models import Project

logger = logging.getLogger(__name__)

DELAY = 0.6
MAX_PAGES = 150

# Site régional (projets avant 26/11/2024)
REGIONAL_SEED = "https://www.bourgogne-franche-comte.developpement-durable.gouv.fr/decisions-cas-par-cas-projet-dossiers-deposes-r669.html"

# API nationale (projets après 26/11/2024)
NATIONAL_API_BASE = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/Get"
NATIONAL_PORTAL_BASE = "https://evaluation-environnementale.ecologie.gouv.fr"

BFC_DEPTS = ["21", "25", "39", "58", "70", "71", "89", "90"]

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _sleep():
    time.sleep(DELAY)

def _get_html(client, url: str) -> Tuple[str, str]:
    logger.info(f"GET {url}")
    html, final_url = client.get_text(url)
    _sleep()
    return html, final_url

def _is_bess_text(text: str) -> bool:
    """Détection BESS optimisée"""
    if not text:
        return False
    t = text.lower()
    has_storage = "stockag" in t
    has_battery = any(k in t for k in ["batter", "bess", "accumulateur", "lithium", "électricité", "énergie"])
    is_excluded = any(k in t for k in ["carburant", "déchet", "gaz naturel"])
    return has_storage and has_battery and not is_excluded

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
    
    # Noms départements
    dept_map = {
        "sens": "89", "yonne": "89", "ligny": "89",
        "drée": "21", "dree": "21", "vieillmoulin": "21", "côte-d'or": "21",
        "gy": "70", "haute-saône": "70", "haute-saone": "70",
        "tonnerre": "89", "doubs": "25", "jura": "39",
        "nièvre": "58", "saône-et-loire": "71", "belfort": "90"
    }
    
    text_lower = text.lower()
    for name, code in dept_map.items():
        if name in text_lower:
            return code
    
    return ""

def _years_in_text(text: str) -> List[int]:
    """Extrait années depuis texte"""
    return [int(y) for y in re.findall(r"\b(20\d{2})\b", text or "")]

# ============ API Portail National ============

def _call_national_api(
    year_target: int,
    dept_filter: Optional[str]
) -> List[Project]:
    """
    Appelle API gatew-evaluation-environnementale.developpement-durable.gouv.fr
    
    Structure JSON découverte:
    {
      "projectTitle": "SENS (89) - Projet de stockage...",
      "department": "89-Yonne",
      "municipality": "Sens (89)",
      "referenceNumber": "007184/KK P",
      "publishedDate": "2025-10-20T15:54:21.96251",
      "documentId": 12248
    }
    """
    logger.info(f"[API NATIONALE] Recherche année={year_target}")
    
    import httpx
    
    projects = []
    
    try:
        # Paramètres API
        params = {
            "start": 0,
            "length": 100,
            "descending_order_id": "true",
            "place": "Bourgogne-Franche-Comté",
            "searchAll": "stockage"
        }
        
        # Appel API
        client = httpx.Client(timeout=30.0, follow_redirects=True)
        response = client.get(NATIONAL_API_BASE, params=params)
        response.raise_for_status()
        
        data = response.json()
        client.close()
        
        items = data.get('data', [])
        total_count = data.get('totalCount', 0)
        
        logger.info(f"[API] {len(items)}/{total_count} items reçus")
        
        for item in items:
            # Extraction champs API
            project_title = item.get('projectTitle', '')
            department = item.get('department', '')
            municipality = item.get('municipality', '')
            reference_number = item.get('referenceNumber', '')
            published_date = item.get('publishedDate', '')
            document_id = item.get('documentId', '')
            
            # Texte combiné
            combined = f"{project_title} {department} {municipality}"
            
            # Vérifier BESS
            if not _is_bess_text(combined):
                continue
            
            # Vérifier année
            years = _years_in_text(published_date or combined)
            if year_target not in years:
                continue
            
            # Extraire département
            dept_code = _dept_from_text(department or municipality or project_title)
            
            # Vérifier département BFC
            if dept_code and dept_code not in BFC_DEPTS:
                continue
            
            # Filtre département utilisateur
            if dept_filter and dept_code != dept_filter.zfill(2):
                continue
            
            # Construire URL projet
            url = ""
            if document_id:
                url = f"{NATIONAL_PORTAL_BASE}/#/public/portal-review/{document_id}"
            
            # Créer projet
            project = Project(
                project_id=_sha1(url or reference_number or project_title),
                region="bourgogne-franche-comte",
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

# ============ Site Régional (avant 26/11/2024) ============

def _closest_item_block(a_node):
    n = a_node
    for _ in range(4):
        if n is None:
            break
        if n.tag in ("li", "article", "div"):
            return n
        n = n.parent
    return a_node.parent or a_node

def _extract_entries(list_html: str, base_url: str) -> List[Dict]:
    tree = HTMLParser(list_html)
    out = []
    seen = set()
    
    for a in tree.css("a"):
        href = a.attributes.get("href", "")
        if not href or not re.search(r"-a\d+\.html", href):
            continue
        
        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)
        
        link_text = (a.text() or "").strip()
        block = _closest_item_block(a)
        item_text = block.text(separator=" ").strip() if block else link_text
        
        out.append({"url": url, "link_text": link_text, "item_text": item_text})
    
    return out

def _page_year_band(entries: List[Dict]) -> Tuple[Optional[int], Optional[int]]:
    years = []
    for e in entries:
        years.extend(_years_in_text(e["item_text"]))
    return (min(years), max(years)) if years else (None, None)

def _find_next(html: str, base: str) -> Optional[str]:
    tree = HTMLParser(html)
    for a in tree.css("a"):
        if "suivant" in (a.text() or "").lower():
            href = a.attributes.get("href")
            if href:
                return urljoin(base, href)
    return None

def _best_title(tree: HTMLParser) -> str:
    h1 = tree.css_first("h1")
    return h1.text().strip() if (h1 and h1.text().strip()) else ""

def _is_bess_page(html: str) -> Tuple[bool, str]:
    tree = HTMLParser(html)
    title = _best_title(tree)
    body = tree.text(separator=" ")[:4000]
    return (_is_bess_text(title) or _is_bess_text(body)), title

def _scrape_regional_site(
    client,
    year_target: int,
    dept_filter: Optional[str],
    options: Dict
) -> List[Project]:
    """
    Scrape site régional DREAL BFC
    Contient uniquement projets déposés AVANT le 26 novembre 2024
    """
    logger.info(f"[SITE RÉGIONAL] année={year_target}")
    
    # Pour 2025+, projets uniquement sur portail national
    if year_target >= 2025:
        logger.info("[SITE RÉGIONAL] Année ≥2025 → projets sur portail national uniquement")
        return []
    
    max_pages = int(options.get("max_pages", MAX_PAGES))
    url = REGIONAL_SEED
    seen = set()
    page_idx = 0
    projects = []
    
    while url and url not in seen and page_idx < max_pages:
        seen.add(url)
        page_idx += 1
        
        try:
            html, final_url = _get_html(client, url)
        except Exception as e:
            logger.error(f"[page {page_idx}] Erreur: {e}")
            break
        
        entries = _extract_entries(html, final_url)
        y_min, y_max = _page_year_band(entries)
        
        logger.info(f"[page {page_idx}] années={y_min}-{y_max} entrées={len(entries)}")
        
        # Navigation optimisée
        if y_min and y_min > year_target:
            url = _find_next(html, final_url)
            continue
        
        if y_max and y_max < year_target:
            logger.info(f"[STOP] Plage temporelle dépassée")
            break
        
        # Filtrer candidats BESS
        candidates = [
            e for e in entries 
            if _is_bess_text(e["item_text"]) and 
            (year_target in _years_in_text(e["item_text"]) or not _years_in_text(e["item_text"]))
        ]
        
        logger.info(f"[page {page_idx}] Candidats BESS: {len(candidates)}")
        
        # Analyser fiches
        for entry in candidates:
            try:
                fiche_html, fiche_url = _get_html(client, entry["url"])
            except:
                continue
            
            is_bess, title = _is_bess_page(fiche_html)
            if not is_bess:
                continue
            
            dept_code = _dept_from_text(title + " " + fiche_url)
            
            if dept_filter and dept_code != dept_filter.zfill(2):
                continue
            
            project = Project(
                project_id=_sha1(fiche_url),
                region="bourgogne-franche-comte",
                dept=dept_code,
                year=str(year_target),
                project_title=title,
                project_url=fiche_url
            )
            
            projects.append(project)
            logger.info(f"[RÉGIONAL] ✓ {title[:60]}...")
        
        url = _find_next(html, final_url)
        if not url:
            break
    
    logger.info(f"[SITE RÉGIONAL] Résultat: {len(projects)} projets")
    return projects

# ============ API Plugin Principale ============

def discover_projects(
    year: str,
    client,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None,
    options: Optional[Dict] = None
) -> List[Project]:
    """
    API principale scraper Bourgogne-Franche-Comté
    
    Stratégie:
    - ≤2024: Site régional DREAL BFC
    - ≥2024: API portail national (projets après 26/11/2024)
    - Fusion avec dédoublonnage
    """
    try:
        year_target = int(year)
    except:
        logger.error(f"Année invalide: {year}")
        return []
    
    opts = options or {}
    
    logger.info(
        f"\n{'='*60}\n"
        f"SCRAPER BOURGOGNE-FRANCHE-COMTÉ\n"
        f"Année: {year_target}\n"
        f"Département: {dept or 'TOUS'}\n"
        f"{'='*60}"
    )
    
    # Site régional (≤2024)
    regional = _scrape_regional_site(client, year_target, dept, opts)
    
    # API nationale (≥2024)
    national = []
    if year_target >= 2024:
        national = _call_national_api(year_target, dept)
    
    # Fusion avec dédoublonnage
    seen_urls = set()
    all_projects = []
    
    for proj in regional + national:
        if proj.project_url not in seen_urls:
            seen_urls.add(proj.project_url)
            all_projects.append(proj)
    
    logger.info(
        f"\n{'='*60}\n"
        f"RÉSUMÉ BOURGOGNE-FRANCHE-COMTÉ\n"
        f"Année: {year_target}\n"
        f"Projets BESS trouvés: {len(all_projects)}\n"
        f"  - Site régional: {len(regional)}\n"
        f"  - Portail national: {len(national)}\n"
        f"{'='*60}\n"
    )
    
    return all_projects
