"""
Scraper DREAL Centre-Val de Loire - VERSION PRODUCTION
- 2023-2024: Pages trimestrielles statiques (structure bloc Dossier → PDF)
- 2025: Pages statiques + API Portail National (Q4 et après)
Interface: discover_projects(year, client, dept, seed_url, region)
"""
import re
import time
import hashlib
import logging
import json
import threading
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

from selectolax.parser import HTMLParser
from models import Project

logger = logging.getLogger(__name__)

DELAY = 0.4
MAX_WORKERS = 3
BASE_URL = "https://www.centre-val-de-loire.developpement-durable.gouv.fr"

# URLs trimestrelles statiques 2023-2024
QUARTERLY_SEEDS = {
    # 2023
    "2023-Q1": f"{BASE_URL}/dossiers-instruits-1er-trimestre-2023-a4613.html",
    "2023-Q2": f"{BASE_URL}/dossiers-instruits-2eme-trimestre-2023-a4614.html",
    "2023-Q3": f"{BASE_URL}/dossiers-instruits-3eme-trimestre-2023-a4615.html",
    "2023-Q4": f"{BASE_URL}/dossiers-instruits-4eme-trimestre-2023-a4616.html",
    # 2024
    "2024-Q1": f"{BASE_URL}/dossiers-instruits-1er-trimestre-2024-a4775.html",
    "2024-Q2": f"{BASE_URL}/dossiers-instruits-2eme-trimestre-2024-a4802.html",
    "2024-Q3": f"{BASE_URL}/dossiers-instruits-3eme-trimestre-2024-a4873.html",
    "2024-Q4": f"{BASE_URL}/dossiers-instruits-4eme-trimestre-2024-a4901.html",
    # 2025 (site)
    "2025-Q1": f"{BASE_URL}/dossiers-instruits-au-1er-trimestre-2025-a4930.html",
    "2025-Q2": f"{BASE_URL}/dossiers-instruits-au-2eme-trimestre-2025-a4974.html",
    "2025-Q3": f"{BASE_URL}/dossiers-instruits-au-3eme-trimestre-2025-a5010.html",
}

# API Portail National (pour Q4 2025 et au-delà)
NATIONAL_API_BASE = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/Get"
NATIONAL_PORTAL_BASE = "https://evaluation-environnementale.ecologie.gouv.fr"

CVL_DEPTS = {"18", "28", "36", "37", "41", "45"}

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def _get_html(client, url: str, cache: Dict, lock: threading.Lock) -> Tuple[str, str]:
    with lock:
        cached = cache.get(url)
    if cached:
        return cached
    html, final_url = client.get_text(url)
    time.sleep(DELAY)
    with lock:
        cache[url] = (html, final_url)
    return html, final_url

def _is_bess(text: str) -> bool:
    t = (text or "").lower()
    has_stock = bool(re.search(r"stockag|stock\s*éner|station\s+de\s+stock", t))
    has_batt = (
        "batter" in t
        or "bess" in t
        or "accumulateur" in t
        or bool(re.search(r"\b2925(-?2)?\b", t))
        or ("électricité" in t and "stockag" in t)
        or ("energie" in t and "stockag" in t)
    )
    pure_pv = ("photovolta" in t) and not has_batt
    return (has_stock or has_batt) and not pure_pv

def _extract_dept(text: str) -> Optional[str]:
    m = re.search(r"\((\d{2})\)", text or "")
    return m.group(1) if m else None

def _belongs_to_cvl(text: str) -> bool:
    d = _extract_dept(text or "")
    return d in CVL_DEPTS if d else False

def _strip_anchor_texts(full_text: str, block) -> str:
    cleaned = full_text
    for a in block.css("a"):
        at = (a.text() or "").strip()
        if at:
            cleaned = cleaned.replace(at, " ")
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    return cleaned.strip()

def _normalize_title_from_block(block) -> str:
    full_text = (block.text() or "").strip()
    full_text = re.sub(r"^\s*Dossier\s*[:\-–]*\s*", "", full_text, flags=re.IGNORECASE)
    full_text = _strip_anchor_texts(full_text, block)
    for line in full_text.splitlines():
        s = line.strip()
        if s:
            return s[:300]
    return "Projet BESS"

def _classify_and_collect_links(block, base_url: str) -> Tuple[Optional[str], Optional[str]]:
    url_cerfa = None
    url_decision = None
    for a in block.css("a"):
        href = a.attributes.get("href", "") or ""
        if not href:
            continue
        full = urljoin(base_url, href)
        low = ((a.text() or "") + " " + href).lower()
        if href.lower().endswith(".zip") or "/img/zip/" in href.lower():
            if not url_cerfa:
                url_cerfa = full
        elif href.lower().endswith(".pdf"):
            if any(k in low for k in ["décision", "decision", "arrêt", "arrete", "avis"]):
                if not url_decision:
                    url_decision = full
            else:
                if not url_cerfa:
                    url_cerfa = full
    return url_cerfa, url_decision

def _find_decision_by_idcode(tree: HTMLParser, base_url: str, block_text: str) -> Optional[str]:
    """
    Cherche le PDF portant le même idcode (fNNNNNpNNNN).
    Structure CVL: ZIP en haut (bloc Dossier), PDF en bas après le titre → c'est la décision.
    """
    m = re.search(r"(f\d{5}p\d{4})", block_text, flags=re.IGNORECASE)
    idcode = m.group(1).lower() if m else None
    
    if not idcode:
        return None
    
    # Chercher TOUT PDF portant le même idcode (peu importe le nom/titre)
    for a in tree.css("a[href*='.pdf']"):
        href = (a.attributes.get("href", "") or "").lower()
        if idcode in href:
            return urljoin(base_url, a.attributes.get("href", ""))
    
    return None

def _parse_quarter_page(html: str, base_url: str) -> List[Dict]:
    tree = HTMLParser(html)
    projects = []
    
    blocks = tree.css("div.texteencadre-spip.spip")
    if not blocks:
        blocks = tree.css("p")
    
    for block in blocks:
        full_text = (block.text() or "").strip()
        if not full_text or "Dossier" not in full_text:
            continue
        
        title = _normalize_title_from_block(block)
        dept = _extract_dept(full_text) or "??"
        url_cerfa, url_decision = _classify_and_collect_links(block, base_url)
        
        # Si pas de décision dans le bloc, chercher par idcode dans toute la page
        if not url_decision:
            url_decision = _find_decision_by_idcode(tree, base_url, full_text)
        
        projects.append({
            "title": title,
            "content": full_text,
            "dept": dept,
            "url_cerfa": url_cerfa,
            "url_decision": url_decision,
        })
    
    return projects

def _scrape_quarter(client, url: str, year: str, quarter: str, cache: Dict, lock: threading.Lock) -> List[Project]:
    try:
        html, final_url = _get_html(client, url, cache, lock)
    except Exception as e:
        logger.error(f"[{year} {quarter}] FAIL {url}: {e}")
        return []
    
    rows = _parse_quarter_page(html, final_url)
    out: List[Project] = []
    
    for r in rows:
        title = r["title"]
        content = r["content"]
        dept = r["dept"]
        
        if not _is_bess(content):
            continue
        if not _belongs_to_cvl(content):
            continue
        
        main_url = r["url_cerfa"] or r["url_decision"] or final_url
        pid = _sha1(f"{year}|{quarter}|{dept}|{title}|{main_url}")
        
        p = Project(
            project_id=pid,
            region="centre-val-de-loire",
            dept=dept,
            year=year,
            project_title=title[:200],
            project_url=main_url,
            url_cerfa=r["url_cerfa"],
            url_decision=r["url_decision"],
        )
        out.append(p)
    
    return out

# ============ API PORTAIL NATIONAL ============

def _query_national_api(client, year: str, dept_filter: Optional[str] = None) -> List[Project]:
    """
    Appelle API Portail National pour Centre-Val de Loire
    Cherche projets stockage dans la région
    """
    logger.info(f"[API] Portail {year} dept={dept_filter or 'ALL'}")
    projects: List[Project] = []
    
    try:
        params = {
            "start": 0,
            "length": 500,
            "descending[order][id]": "true",
            "place": "Centre-Val de Loire",
            "searchAll": "stockage"
        }
        
        query_string = urlencode(params)
        full_url = f"{NATIONAL_API_BASE}?{query_string}"
        
        logger.info(f"[API] GET {full_url[:80]}...")
        response_text, _ = client.get_text(full_url)
        time.sleep(DELAY)
        
        data = json.loads(response_text)
        items = data.get('data', [])
        total_count = data.get('totalCount', 0)
        
        logger.info(f"[API] {len(items)}/{total_count} items reçus")
        
        for item in items:
            project_title = item.get('projectTitle', '')
            department = item.get('department', '')
            municipality = item.get('municipality', '')
            document_id = item.get('documentId', '')
            published_date = item.get('publishedDate', '')
            
            combined = f"{project_title} {department} {municipality}"
            
            if not _is_bess(combined):
                continue
            
            # Extraire code département
            m = re.search(r'\((\d{2})\)', combined)
            dept_code = m.group(1) if m else ""
            
            if dept_filter and dept_code != dept_filter.zfill(2):
                continue
            
            if not _belongs_to_cvl(combined):
                continue
            
            url_decision = f"{NATIONAL_PORTAL_BASE}/public/view-document/{document_id}" if document_id else None
            
            pid = _sha1(f"api|{year}|{dept_code}|{project_title}|{document_id}")
            
            p = Project(
                project_id=pid,
                region="centre-val-de-loire",
                dept=dept_code or "??",
                year=year,
                project_title=project_title[:200],
                project_url=url_decision or "",
                url_cerfa=None,
                url_decision=url_decision,
            )
            projects.append(p)
            
            logger.info(f"[API] ✓ {project_title[:60]}")
        
    except Exception as e:
        logger.error(f"[API] ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return projects

# ============ MAIN API ============

def discover_projects(
    year: Optional[str] = None,
    client=None,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None,
    region: str = "centre-val-de-loire",
) -> List[Project]:
    """
    API principale - Centre-Val de Loire
    - 2023-2024: Pages trimestrielles statiques
    - 2025: Pages Q1-Q3 + API pour Q4 et après
    """
    cache: Dict[str, Tuple[str, str]] = {}
    lock = threading.Lock()
    
    targets: List[Tuple[str, str, str]] = []
    use_api = False
    
    if seed_url:
        m = re.search(r"(20[0-9]{2})", seed_url or "")
        inferred_year = m.group(1) if m else None
        y = year or inferred_year or "2025"
        targets = [(y, "Q?", seed_url)]
    else:
        if not year:
            raise ValueError("Merci de passer --year (2023, 2024 ou 2025).")
        
        # Ajouter trimestres statiques
        for key, qurl in QUARTERLY_SEEDS.items():
            if key.startswith(year):
                q = key.split("-")[1]
                targets.append((year, q, qurl))
        
        # Pour 2025, ajouter aussi l'API (pour Q4 et après)
        if year == "2025":
            use_api = True
    
    if not targets and not use_api:
        logger.warning(f"Aucun trimestre configuré pour {year}")
        return []
    
    results: List[Project] = []
    
    # Scraper trimestres statiques
    if targets:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(_scrape_quarter, client, url, y, q, cache, lock) for (y, q, url) in targets]
            for fut in as_completed(futures):
                try:
                    results.extend(fut.result() or [])
                except Exception as e:
                    logger.error(f"Worker error: {e}")
    
    # Ajouter API pour 2025 (Q4 et au-delà)
    if use_api:
        api_projects = _query_national_api(client, year, dept)
        results.extend(api_projects)
    
    # Filtrer par département si demandé
    if dept:
        code = dept.zfill(2)
        results = [p for p in results if p.dept == code]
    
    return results
