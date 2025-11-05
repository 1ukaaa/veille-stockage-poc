#!/usr/bin/env python3
"""
Scraper DREAL Hauts-de-France - SPIP Listing avec Matching Formulaire/Décision
Architecture: URL listing avec pagination → parse items → group by project_key → extract documents

Logique spéciale:
- Page listing SPIP avec tous les documents mélangés (CERFA + DÉCISIONS)
- Pré-filtre serveur par "stockage" (tous types) → validation BESS client-side
- Matching par clé numérique extraite du nom PDF (année-ID ou variation)
- Support cross-année (CERFA 2023-12 avec DÉCISION 2024-01 = année projet = 2024)
- ANNÉE EXTRAITE DEPUIS L'URL PDF (pas la date HTML)
- Filtre par année + département (console)
- Orphelins conservés
- Regroupement: 1 ligne par projet_key avec CERFA + DÉCISION URLs
- ANNÉE RÉELLE: Priorité DÉCISION > CERFA
"""
import re
import time
import hashlib
import logging
import csv
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from selectolax.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed

from models import Project

logger = logging.getLogger(__name__)

# ============ Configuration ============
DELAY = 0.5  # Respectueux
MAX_WORKERS = 3
BASE_URL = "https://www.hauts-de-france.developpement-durable.gouv.fr"
LISTING_ENDPOINT = "/?-Consultation-des-avis-examens-au-cas-par-cas-et-decisions-"

# Filtre par défaut pour stockage
SEARCH_QUERY = "stockage"

# Items par page (SPIP standard)
ITEMS_PER_PAGE = 10


# ============ Utils ============

def _sha1(text: str) -> str:
    """Hash SHA1 pour project_id"""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _extract_project_key(filename: str) -> Optional[str]:
    """
    Extrait clé de groupage du nom fichier PDF
    NORMALISE les séparateurs: tiret ET underscore deviennent tiret
    
    Patterns supportés:
    - 2024-8433-cerfa.pdf → "2024-8433"
    - 2024_8433-cerfa.pdf → "2024-8433" (normalisé!)
    - cerfa_2024-3019.pdf → "2024-3019"
    - 2024-8433-decision.pdf → "2024-8433"
    - 005382-cerfa.pdf → "005382"
    """
    # Pattern principal: YYYY-NNNN ou YYYY_NNNN (tiret ou underscore)
    match = re.search(r'(\d{4})[_-](\d{4})', filename)
    if match:
        # Retourner NORMALISÉ avec tiret
        return f"{match.group(1)}-{match.group(2)}"
    
    # Pattern: NNNNNN sans année
    match = re.search(r'_(\d{6})\.', filename)
    if match:
        return match.group(1)
    
    # Fallback: tout nombre > 3 chiffres
    match = re.search(r'(\d{4,})', filename)
    if match:
        return match.group(1)
    
    return None


def _extract_year_from_url(url: str) -> str:
    """
    Extrait l'année DEPUIS L'URL du PDF (fiable!)
    
    Patterns:
    - https://.../2024-8112-decision.pdf → "2024"
    - https://.../2023_7235_cerfa.pdf → "2023"
    - https://.../005382-cerfa.pdf → "2025" (pas d'année, fallback)
    """
    # Récupère le nom fichier
    filename = url.split("/")[-1].lower()
    
    # Pattern YYYY-NNNN ou YYYY_NNNN au début
    match = re.search(r'^(\d{4})[_-]', filename)
    if match:
        return match.group(1)
    
    # Fallback 2025
    return "2025"


def _classify_document(title: str) -> str:
    """
    Classifie type de document
    
    Returns: "cerfa", "decision", "autre"
    """
    t = title.lower()
    
    if "formulaire" in t or "cerfa" in t or "demande" in t:
        return "cerfa"
    elif "décision" in t:
        return "decision"
    else:
        return "autre"


def _is_bess_project(title: str, communes: str = "") -> bool:
    """Valide si projet est vraiment BESS"""
    combined = f"{title} {communes}".lower()
    
    # Si contient "stockage" + ("électricité" ou "énergie"), c'est BESS
    has_storage = "stockag" in combined
    has_electricity_or_energy = ("électricité" in combined) or ("énergie" in combined)
    
    # Critères d'inclusion
    is_bess = has_storage and has_electricity_or_energy
    
    if not is_bess:
        logger.debug(f"[BESS-REJECT] No 'stockage + électricité/énergie': {title[:60]}")
        return False
    
    # Critères d'exclusion (carburant, gaz, etc.)
    excluded_types = [
        "carburant", "fuel", "gasoil", "essence",
        "gaz naturel", "propane", "butane",
        "déchet", "décheterie"
    ]
    
    is_excluded = any(k in combined for k in excluded_types)
    
    if is_excluded:
        logger.debug(f"[BESS-EXCLUDE] Found excluded type: {title[:60]}")
        return False
    
    logger.debug(f"[BESS-OK] {title[:60]}")
    return True



def _belongs_to_dept(title: str, dept_code: str) -> bool:
    """Vérifie si document appartient au département"""
    if not dept_code:
        return True
    
    code = dept_code.zfill(2)
    pattern = rf'\(\s*{code}\s*\)'
    return bool(re.search(pattern, title or ""))


# ============ Parsing HTML Listing ============

def _parse_listing_items(html: str, base_url: str) -> List[Dict]:
    """
    Parse items depuis HTML listing SPIP
    
    ⭐ CHANGEMENT CLÉS:
    - N'utilise PAS la date HTML "Publié le" pour l'année
    - Extrait l'année depuis L'URL du PDF (plus fiable)
    """
    tree = HTMLParser(html)
    items = []
    
    articles_container = tree.css_first("div.articles-list")
    if not articles_container:
        articles_container = tree
    
    for h4 in articles_container.css("h4"):
        try:
            title_link = h4.css_first("a")
            if not title_link:
                continue
            
            title = (title_link.text() or "").strip()
            pdf_url = title_link.attributes.get("href", "")
            
            if not title or not pdf_url:
                continue
            
            full_pdf_url = urljoin(base_url, pdf_url)
            
            # ⭐ EXTRAIRE L'ANNÉE DEPUIS L'URL DU PDF
            year = _extract_year_from_url(full_pdf_url)
            logger.debug(f"[YEAR] URL={pdf_url} → year={year}")
            
            # Date (pour info/debug seulement, pas pour l'année)
            date_span = None
            parent = h4.parent
            if parent:
                date_span = parent.css_first("span.date")
            
            if not date_span:
                if parent and parent.parent:
                    date_span = parent.parent.css_first("span.date")
            
            date_text = (date_span.text() if date_span else "").strip()
            date_text = re.sub(r'^Publié le\s*', '', date_text)
            
            # Communes
            communes_text = ""
            communes_div = None
            if parent:
                communes_div = parent.css_first("div.liste_communes")
            
            if communes_div:
                communes_text = (communes_div.text() or "").strip()
            
            # ===== FILTRE BESS =====
            if not _is_bess_project(title, communes_text):
                logger.info(f"[SKIP] Non-BESS project: {title[:60]}...")
                continue
            
            filename = pdf_url.split("/")[-1]
            project_key = _extract_project_key(filename)
            
            if not project_key:
                logger.warning(f"Impossible d'extraire clé: {filename}")
                continue
            
            doc_type = _classify_document(title)
            
            items.append({
                "project_key": project_key,
                "title": title,
                "pdf_url": full_pdf_url,
                "filename": filename,
                "date_text": date_text,
                "year": year,  # ⭐ Extraite depuis L'URL PDF
                "doc_type": doc_type,
                "communes": communes_text
            })
        
        except Exception as e:
            logger.warning(f"Parse item error: {e}")
            continue
    
    return items


# ============ Pagination ============

def _build_listing_url(page_offset: int, search_query: str = SEARCH_QUERY) -> str:
    """Construit URL listing avec pagination"""
    url = f"{BASE_URL}{LISTING_ENDPOINT}&recherche={search_query}&debut_articles={page_offset}#pagination_articles"
    return url


def _fetch_listing_page(client, page_offset: int, search_query: str = SEARCH_QUERY) -> Tuple[str, str]:
    """Récupère une page du listing"""
    url = _build_listing_url(page_offset, search_query)
    logger.info(f"GET page offset={page_offset}: {url}")
    html, final_url = client.get_text(url)
    time.sleep(DELAY)
    return html, final_url


# ============ Collecte Complète ============

def _collect_all_items(client, search_query: str = SEARCH_QUERY, max_pages: int = 50) -> List[Dict]:
    """Collecte TOUS les items de la pagination"""
    all_items = []
    page_offset = 0
    page_idx = 1
    
    logger.info(f"Collecte listing: recherche='{search_query}'")
    
    while page_idx <= max_pages:
        try:
            html, final_url = _fetch_listing_page(client, page_offset, search_query)
            items = _parse_listing_items(html, BASE_URL)
            
            logger.info(f"Page {page_idx} (offset={page_offset}): {len(items)} items (après filtre BESS)")
            
            if not items:
                logger.info(f"Page vide, fin pagination")
                break
            
            all_items.extend(items)
            page_offset += ITEMS_PER_PAGE
            page_idx += 1
        
        except Exception as e:
            logger.error(f"Fetch page {page_idx} FAIL: {e}")
            break
    
    logger.info(f"Total items collectés (après filtre BESS): {len(all_items)}")
    return all_items


# ============ Matching & Groupage ============

def _group_items_by_project_key(items: List[Dict]) -> Dict[str, List[Dict]]:
    """Groupe items par clé projet"""
    grouped = defaultdict(list)
    
    for item in items:
        key = item["project_key"]
        grouped[key].append(item)
    
    logger.info(f"Groupés en {len(grouped)} projets uniques")
    return dict(grouped)


def _build_project_entry(project_key: str, docs: List[Dict]) -> Dict:
    """
    Construit projet regroupé avec CERFA + DÉCISION
    
    LOGIQUE ANNÉE CRITIQUE:
    - L'année du projet = année la PLUS RÉCENTE
    - DÉCISION > CERFA (la décision valide le projet)
    
    Exemples:
    - CERFA 2023 + DÉCISION 2024 → année projet = 2024
    - CERFA 2024 + DÉCISION 2025 → année projet = 2025
    - CERFA seul 2023 → année projet = 2023
    """
    
    docs_by_type = defaultdict(list)
    all_communes = set()
    
    for doc in docs:
        doc_type = doc["doc_type"]
        docs_by_type[doc_type].append(doc)
        
        if doc.get("communes"):
            all_communes.add(doc["communes"])
    
    # Déduire status
    has_cerfa = len(docs_by_type["cerfa"]) > 0
    has_decision = len(docs_by_type["decision"]) > 0
    
    if has_cerfa and has_decision:
        status = "paired"
    elif has_cerfa:
        status = "orphan_cerfa"
    elif has_decision:
        status = "orphan_decision"
    else:
        status = "orphan_autre"
    
    # ===== ANNÉE RÉELLE: DÉCISION > CERFA =====
    final_year = "2025"  # Fallback
    
    if docs_by_type["decision"]:
        # ⭐ DÉCISION = priorité 1 (elle valide le projet)
        final_year = docs_by_type["decision"][0]["year"]
        logger.debug(f"[{project_key}] Year from DECISION: {final_year}")
    elif docs_by_type["cerfa"]:
        # ⭐ CERFA = priorité 2 (si pas de décision)
        final_year = docs_by_type["cerfa"][0]["year"]
        logger.debug(f"[{project_key}] Year from CERFA: {final_year}")
    else:
        logger.debug(f"[{project_key}] No year source, using fallback 2025")
    
    entry = {
        "project_key": project_key,
        "url_cerfa": docs_by_type["cerfa"][0]["pdf_url"] if docs_by_type["cerfa"] else None,
        "url_decision": docs_by_type["decision"][0]["pdf_url"] if docs_by_type["decision"] else None,
        "title_cerfa": docs_by_type["cerfa"][0]["title"] if docs_by_type["cerfa"] else None,
        "title_decision": docs_by_type["decision"][0]["title"] if docs_by_type["decision"] else None,
        "year": final_year,  # ⭐ ANNÉE RÉELLE (extraite depuis URL)
        "communes": " | ".join(sorted(all_communes)),
        "status": status
    }
    
    return entry


# ============ Filtrage ============

def _filter_by_year_dept(
    projects: Dict[str, Dict],
    year_filter: Optional[str] = None,
    dept_filter: Optional[str] = None
) -> Dict[str, Dict]:
    """
    Filtre projets par année et département
    """
    filtered = {}
    
    for project_key, entry in projects.items():
        # Filtre année
        if year_filter:
            if entry["year"] != year_filter:
                logger.debug(f"[{project_key}] rejected: year {entry['year']} != {year_filter}")
                continue
        else:
            # Filtre par défaut: garder si année en 2023-2025
            if entry["year"] not in ["2023", "2024", "2025"]:
                logger.debug(f"[{project_key}] rejected: year {entry['year']} not in 2023-2025")
                continue
        
        # Filtre département
        if dept_filter:
            text_to_search = f"{entry['communes']} {entry['title_cerfa'] or ''} {entry['title_decision'] or ''}".lower()
            
            if not _belongs_to_dept(text_to_search, dept_filter):
                logger.debug(f"[{project_key}] rejected: dept {dept_filter} not found")
                continue
        
        filtered[project_key] = entry
    
    logger.info(f"Après filtres année/dept: {len(filtered)} projets")
    return filtered

def _clean_project_title(title: str) -> str:
    """
    Nettoie le titre en enlevant les préfixes génériques
    
    De: "Formulaire de demande d'examen au cas par cas, relatif à une unité de stockage..."
    Vers: "Une unité de stockage..."
    """
    if not title:
        return ""
    
    # Enlever "Formulaire de demande d'examen au cas par cas, relatif à"
    title = re.sub(
        r"^Formulaire\s+de\s+demande\s+d.examen\s+au\s+cas\s+par\s+cas[,]?\s*(relatif à|concernant|portant sur)?\s*",
        "",
        title,
        flags=re.IGNORECASE
    ).strip()
    
    return title if title else "BESS Project"


# ============ Conversion Project Model ============

def _build_project_object(entry: Dict, region: str = "hauts-de-france") -> Optional[Project]:
    """
    Crée objet Project UNIQUE par projet (regroupé)
    
    URL principale: CERFA si dispo, sinon DÉCISION
    """
    
    # Titre principal (prioriser CERFA pour le titre)
    raw_title = entry["title_cerfa"] or entry["title_decision"] or ""
    title = _clean_project_title(raw_title)
    
    # URL principale
    main_url = entry["url_cerfa"] or entry["url_decision"]
    
    if not main_url:
        logger.warning(f"No URL found for {entry['project_key']}")
        return None
    
    # Département
    dept_match = re.search(r'\((\d{2})\)', entry["communes"] + (entry["title_cerfa"] or ""))
    dept = dept_match.group(1) if dept_match else "?"
    
    return Project(
        project_id=_sha1(entry["project_key"]),
        region=region,
        dept=dept,
        year=entry["year"],  # ⭐ ANNÉE RÉELLE (extraite depuis URL)
        project_title=title[:200],
        project_url=main_url,
        url_cerfa=entry["url_cerfa"],
        url_decision=entry["url_decision"]
    )


# ============ Export CSV avec MULTI-URLs ============

def export_projects_csv_with_urls(
    entries_dict: Dict[str, Dict],
    output_path: Path
):
    """
    Exporte projets EN LIGNE UNIQUE avec CERFA + DÉCISION URLs
    """
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            "project_key",
            "status",
            "url_cerfa",
            "url_decision",
            "title_cerfa",
            "title_decision",
            "year",
            "communes"
        ])
        
        # Rows: 1 ligne par projet (regroupé)
        for project_key, entry in entries_dict.items():
            writer.writerow([
                entry["project_key"],
                entry["status"],
                entry["url_cerfa"] or "",
                entry["url_decision"] or "",
                entry["title_cerfa"] or "",
                entry["title_decision"] or "",
                entry["year"],
                entry["communes"]
            ])
    
    logger.info(f"CSV exporté (regroupé par projet): {output_path}")


# ============ API Principale ============

def discover_projects(
    year: Optional[str] = None,
    client = None,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None,
    search_query: str = SEARCH_QUERY
) -> List[Project]:
    """
    API principale Hauts-de-France
    
    Retourne 1 Project par projet_key (regroupé CERFA + DÉCISION)
    Année: Extraite depuis L'URL PDF (DÉCISION > CERFA)
    """
    
    logger.info(
        f"\n{'='*70}\n"
        f"SCRAPER HAUTS-DE-FRANCE (SPIP LISTING)\n"
        f"Année: {year or 'filtre défaut (2023-2025)'}\n"
        f"Département: {dept or 'TOUS'}\n"
        f"Recherche: '{search_query}'\n"
        f"Filtre BESS: ACTIVÉ\n"
        f"Année réelle: Extraite depuis URL PDF (DÉCISION > CERFA)\n"
        f"Regroupement: 1 ligne/projet avec CERFA + DÉCISION\n"
        f"{'='*70}\n"
    )
    
    # Étape 1: Collecte tous les items
    all_items = _collect_all_items(client, search_query, max_pages=50)
    
    if not all_items:
        logger.error("Aucun item trouvé après filtre BESS")
        return []
    
    logger.info(f"Total items bruts: {len(all_items)}")
    
    # Étape 2: Groupage par clé
    grouped = _group_items_by_project_key(all_items)
    
    # Étape 3: Construction projets (1 par clé, regroupés)
    projects_dict = {}
    for project_key, docs in grouped.items():
        entry = _build_project_entry(project_key, docs)
        projects_dict[project_key] = entry
    
    logger.info(f"Projets avant filtres année/dept: {len(projects_dict)}")
    
    # Étape 4: Filtrage année/dept
    filtered_dict = _filter_by_year_dept(projects_dict, year, dept)
    
    # Étape 5: Conversion Project objects
    projects = []
    for entry in filtered_dict.values():
        proj = _build_project_object(entry)
        if proj:
            projects.append(proj)
    
    # Résumé
    logger.info(
        f"\n{'='*70}\n"
        f"RÉSUMÉ HAUTS-DE-FRANCE\n"
        f"Total projets BESS (regroupés): {len(projects)}\n"
    )
    
    # Comptage statut
    status_count = defaultdict(int)
    for entry in filtered_dict.values():
        status_count[entry["status"]] += 1
    
    for status, count in sorted(status_count.items()):
        logger.info(f"  - {status}: {count}")
    
    logger.info(f"{'='*70}\n")
    
    return projects
