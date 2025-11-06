#!/usr/bin/env python3
"""
Scraper DREAL Pays de la Loire - VERSION FINALE
- 2023: Site PDL (a6134-a6138)
- 2024: Site PDL (a6437-a6441) + API fin 2024
- 2025: API Portail National uniquement
- PAS DE CACHE (plus de problèmes de timeout/conflits)
"""

import re
import time
import hashlib
import logging
from typing import List, Optional, Dict
from urllib.parse import urljoin
from pathlib import Path
from models import Project

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIG
# ============================================================================

DELAY = 0.5
BASEURL = "https://www.pays-de-la-loire.developpement-durable.gouv.fr"

# 2023: Pages statiques
PDL_URLS_2023 = {
    "44": f"{BASEURL}/loire-atlantique-a6134.html",
    "49": f"{BASEURL}/maine-et-loire-a6135.html",
    "53": f"{BASEURL}/mayenne-a6136.html",
    "72": f"{BASEURL}/sarthe-a6137.html",
    "85": f"{BASEURL}/vendée-a6138.html",
}

# 2024: Pages statiques DIFFÉRENTES
PDL_URLS_2024 = {
    "44": f"{BASEURL}/loire-atlantique-a6437.html",
    "49": f"{BASEURL}/maine-et-loire-a6438.html",
    "53": f"{BASEURL}/mayenne-a6439.html",
    "72": f"{BASEURL}/sarthe-a6440.html",
    "85": f"{BASEURL}/vendée-a6441.html",
}

# API Portail National
NATIONAL_API_BASE = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/Get"
NATIONAL_PORTAL_BASE = "https://evaluation-environnementale.ecologie.gouv.fr"

PDF_KEY_RE = re.compile(r'(?P<year>\d{4})[-_](?P<num>\d{3,6})', re.IGNORECASE)

PDL_DEPTS = ["44", "49", "53", "72", "85"]

# ============================================================================
# UTILS
# ============================================================================

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode('utf-8')).hexdigest()

def is_bess_project(title: str) -> bool:
    """Filtre BESS STRICT"""
    if not title or len(title) < 15:
        return False
    
    combined = title.lower()
    
    has_storage = "stockag" in combined or "accumulateur" in combined
    has_battery = (
        "batter" in combined or "bess" in combined or
        ("énergie" in combined and "électricité" in combined) or
        ("systèmes de stockage d'électricité par batteries" in combined)
    )
    
    excluded = [
        "carburant", "fuel", "gaz", "déchet", "bois", "forêt",
        "carrière", "pierre", "eau", "forage", "puits",
        "agriculture", "volaille", "élevage", "serre", "parking"
    ]
    is_excluded = any(k in combined for k in excluded)
    
    return (has_storage and has_battery) and not is_excluded

def classify_pdf(url_or_name: str) -> str:
    """Classification PDF ULTRA-TOLÉRANTE"""
    s = url_or_name.lower()
    
    if any(k in s for k in ["cerfa", "formulaire", "formuliare", "complet", "formulaireinitial"]):
        return "cerfa"
    
    if any(k in s for k in [
        "decision", "décision", "decison", "signee", "signée",
        "decisionsignee", "decision_signee", "_alm", "projetdecision",
        "projetdarrete", "arrete", "projet_d", "projet_decision"
    ]):
        return "decision"
    
    return "other"

def extract_project_key_from_pdfs(urls: List[str], region: str, dept: str, title: str) -> str:
    """Extrait clé unique YYYY-NNNN"""
    for url in urls:
        if not url:
            continue
        match = PDF_KEY_RE.search(url)
        if match:
            year = match.group('year')
            num = match.group('num')
            return f"{region}-{dept}-{year}-{num}"
    
    return f"{region}-{dept}-{sha1(title)[:10]}"

def collect_pdfs_around(html: str, start_pos: int, baseurl: str, window: int = 8000) -> List[str]:
    """Collecte PDFs dans fenêtre 8KB"""
    if start_pos + window > len(html):
        segment = html[start_pos:]
    else:
        segment = html[start_pos:start_pos + window]
    
    urls = re.findall(r'href="([^"]+\.pdf)"', segment, re.IGNORECASE)
    
    seen = set()
    result = []
    for u in urls:
        full_url = urljoin(baseurl, u)
        if full_url not in seen:
            seen.add(full_url)
            result.append(full_url)
    
    return result

# ============================================================================
# PARSING - SITE PDL
# ============================================================================

def extract_projects_from_pdl(html: str, baseurl: str, region: str, dept: str) -> List[Dict]:
    """Parse HTML site PDL"""
    projects = []
    seen_keys = set()
    
    pattern = r'<strong>\s*([^<]{20,300})\s*</strong>'
    
    for match in re.finditer(pattern, html, re.IGNORECASE):
        title = match.group(1).strip()
        
        if not is_bess_project(title):
            continue
        
        logger.info(f"    [BESS] {title[:70]}")
        
        start_pos = match.start()
        pdf_urls = collect_pdfs_around(html, start_pos, baseurl, window=8000)
        
        if not pdf_urls:
            continue
        
        url_cerfa = ""
        url_decision = ""
        
        for pdf_url in pdf_urls:
            pdf_type = classify_pdf(pdf_url)
            if pdf_type == 'cerfa' and not url_cerfa:
                url_cerfa = pdf_url
            elif pdf_type == 'decision' and not url_decision:
                url_decision = pdf_url
        
        if not url_cerfa and not url_decision:
            continue
        
        project_key = extract_project_key_from_pdfs(
            [url_cerfa, url_decision], region, dept, title
        )
        
        if project_key not in seen_keys:
            projects.append({
                'project_key': project_key,
                'title': title,
                'url_cerfa': url_cerfa,
                'url_decision': url_decision,
                'year': project_key.split('-')[2] if '-' in project_key else "2025",
                'dept': dept,
                'region': region,
            })
            seen_keys.add(project_key)
            
            cerfa_mark = "✓" if url_cerfa else "✗"
            decision_mark = "✓" if url_decision else "✗"
            logger.info(f"      ✓ {project_key} [{cerfa_mark}C][{decision_mark}D]")
    
    unique_dict = {p['project_key']: p for p in projects}
    return list(unique_dict.values())

# ============================================================================
# SCRAPING - SITE PDL
# ============================================================================

def fetch_page(client, url: str) -> str:
    logger.info(f"  GET {url}")
    html, _ = client.get_text(url)
    time.sleep(DELAY)
    return html

def scrape_pdl_site(client, region: str, year: str, dept: str, url: str) -> List[Dict]:
    """Scrape site PDL SANS CACHE"""
    
    logger.info(f"  Scraping {year}/{dept}")
    
    try:
        html = fetch_page(client, url)
        projects = extract_projects_from_pdl(html, url, region, dept)
        
        return projects
    
    except Exception as e:
        logger.error(f"  ERROR {year}/{dept}: {e}")
        return []

# ============================================================================
# API PORTAIL NATIONAL
# ============================================================================

def query_national_api(client, region: str, year_target: str, dept_filter: Optional[str] = None) -> List[Dict]:
    """
    Appelle API Portail National
    CORRECTION: Construire URL avec paramètres manuellement
    """
    
    logger.info(f"  API Portail {year_target} dept={dept_filter or 'ALL'}")
    
    projects = []
    
    try:
        # Construire URL avec paramètres
        from urllib.parse import urlencode
        
        params = {
            "start": 0,
            "length": 200,
            "descending[order][id]": "true",
            "place": "Pays de la Loire",
            "searchAll": "stockage"
        }
        
        query_string = urlencode(params)
        full_url = f"{NATIONAL_API_BASE}?{query_string}"
        
        logger.info(f"  GET {full_url[:100]}...")
        
        # Appel API sans params
        response_text, _ = client.get_text(full_url)
        
        import json
        data = json.loads(response_text)
        
        items = data.get('data', [])
        total_count = data.get('totalCount', 0)
        
        logger.info(f"  API: {len(items)}/{total_count} items reçus")
        
        for item in items:
            project_title = item.get('projectTitle', '')
            department = item.get('department', '')
            municipality = item.get('municipality', '')
            reference_number = item.get('referenceNumber', '')
            published_date = item.get('publishedDate', '')
            document_id = item.get('documentId', '')
            
            combined = f"{project_title} {department} {municipality}"
            
            if not is_bess_project(combined):
                continue
            
            # Filtrer par département
            dept_code = re.search(r'\b(\d{2})\b', department or municipality or "")
            if dept_code:
                dept_code = dept_code.group(1)
            else:
                dept_code = ""
            
            if dept_filter and dept_code != dept_filter.zfill(2):
                continue
            
            # URL projet
            url = f"{NATIONAL_PORTAL_BASE}/public/view-document/{document_id}" if document_id else ""
            
            projects.append({
                'project_key': f"pays-de-la-loire-{dept_code}-{year_target}-{document_id}",
                'title': project_title,
                'url_cerfa': None,
                'url_decision': url,
                'year': year_target,
                'dept': dept_code,
                'region': "pays-de-la-loire",
            })
            
            logger.info(f"    ✓ API: {project_title[:60]}")
        
    except Exception as e:
        logger.error(f"  API ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return projects


# ============================================================================
# MAIN ROUTING
# ============================================================================

def discover_projects(year: Optional[str] = None, client=None, dept: Optional[str] = None, 
                     seed_url: Optional[str] = None, region: str = "pays-de-la-loire") -> List[Project]:
    """
    API principale - ROUTING FINAL SANS CACHE
    - 2023: Site PDL (a6134-a6138)
    - 2024: Site PDL (a6437-a6441) + API fin 2024
    - 2025: API uniquement
    """
    
    logger.info(f"""
╔{'='*68}╗
║ SCRAPER PAYS DE LA LOIRE - FINAL SANS CACHE                        ║
║ Année: {str(year or '2023-2025'):44} ║
║ Département: {str(dept or 'TOUS (44,49,53,72,85)'):40} ║
║ Stratégie:                                                          ║
║   - 2023: Site PDL (a6134-a6138)                                   ║
║   - 2024: Site PDL (a6437-a6441) + API fin 2024                   ║
║   - 2025: API Portail National uniquement                          ║
║   - PAS DE CACHE (évite conflits 2023/2024)                       ║
╚{'='*68}╝
    """)
    
    years_to_scrape = [year] if year else ["2023", "2024", "2025"]
    depts_to_scrape = [dept] if dept else PDL_DEPTS
    
    all_entries = {}
    
    for y in years_to_scrape:
        logger.info(f"\n{'='*50}")
        logger.info(f"ANNÉE {y}")
        logger.info(f"{'='*50}")
        
        if y == "2023":
            # 2023: Site PDL
            for d in depts_to_scrape:
                if d in PDL_URLS_2023:
                    url = PDL_URLS_2023[d]
                    entries = scrape_pdl_site(client, region, y, d, url)
                    for entry in entries:
                        all_entries[entry['project_key']] = entry
        
        elif y == "2024":
            # 2024: Site PDL (a6437-a6441)
            for d in depts_to_scrape:
                if d in PDL_URLS_2024:
                    url = PDL_URLS_2024[d]
                    entries = scrape_pdl_site(client, region, y, d, url)
                    for entry in entries:
                        all_entries[entry['project_key']] = entry
            
            # 2024: API pour fin 2024
            logger.info(f"\n  --- Fin 2024 (API) ---")
            api_entries = query_national_api(client, region, y, dept)
            for entry in api_entries:
                all_entries[entry['project_key']] = entry
        
        elif y == "2025":
            # 2025: API uniquement
            api_entries = query_national_api(client, region, y, dept)
            for entry in api_entries:
                all_entries[entry['project_key']] = entry
        
        else:
            logger.warning(f"  Year {y} not configured")
    
    # Convertir en Project objects
    final_projects = []
    
    for key, entry in all_entries.items():
        if entry.get('url_cerfa') or entry.get('url_decision'):
            project = Project(
                project_id=sha1(key),
                region=entry.get('region', region),
                dept=entry.get('dept', '?'),
                year=entry.get('year', '2025'),
                project_title=entry['title'][:200],
                project_url=entry.get('url_cerfa') or entry.get('url_decision') or "",
                url_cerfa=entry.get('url_cerfa'),
                url_decision=entry.get('url_decision')
            )
            
            final_projects.append(project)
    
    logger.info(f"""
╔{'='*68}╗
║ RÉSUMÉ FINAL                                                       ║
║ Total: {len(final_projects):<54} ║
║ Avec CERFA: {sum(1 for p in final_projects if p.url_cerfa):<45} ║
║ Avec Décision: {sum(1 for p in final_projects if p.url_decision):<40} ║
╚{'='*68}╝
    """)
    
    return final_projects
