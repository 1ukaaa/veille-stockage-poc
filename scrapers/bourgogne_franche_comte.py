#!/usr/bin/env python3
"""
Scraper DREAL Île-de-France - VERSION PRODUCTION COMPLÈTE
Architecture: Départements → Années → Tableau Projets (HIÉRARCHIE 3 NIVEAUX)

Structure DREAL IDF:
- Niveau 1: Main page → 8 départements IDF
- Niveau 2: Page département → années (2018-2025)
- Niveau 3: Page année/département → tableau 5 colonnes (NO pagination)

EXCEPTION 2025+:
- Année < 2025: Scrape site régional (https://www.drieat.ile-de-france.developpement-durable.gouv.fr)
- Année ≥ 2025: Scrape site régional + API portail national (https://evaluation-environnementale.ecologie.gouv.fr)

Tableau structure (5 colonnes):
  1. N° formulaire + fichiers CERFA (PDF)
  2. Commune et intitulé projet ← FILTRE BESS ICI
  3. Date réception
  4. Date limite décision
  5. Décision (PDF)

Filtre BESS:
  ✓ "stockag" + ("électricité" OU "énergie")
  ✓ "batter", "bess", "accumulateur"
  ✗ Exclure: carrière, déchet, gaz, carburant

Extraction:
  - url_cerfa: depuis colonne 1
  - url_decision: depuis colonne 5
  - Orphelins: gardés avec URL vide
  - Année: extraite du PDF (priorité DÉCISION > CERFA)
  - Project Key: SHA1(titre + dept + year)

Usage:
    from scrapers.ile_de_france import discover_projects
    
    projects = discover_projects(
        year="2024",      # optionnel
        client=http_client,
        dept="77"         # optionnel
    )
"""
import re
import time
import hashlib
import logging
import httpx
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime

from selectolax.parser import HTMLParser

from models import Project

logger = logging.getLogger(__name__)

# ============ Configuration ============
DELAY = 0.5  # Respectueux du serveur
MAX_WORKERS = 2

BASE_URL = "https://www.drieat.ile-de-france.developpement-durable.gouv.fr"
MAIN_PAGE = "/suivi-des-demandes-d-examen-au-cas-par-cas-pour-le-r659.html"

# API nationale (2025+)
NATIONAL_API_BASE = "https://gatew-evaluation-environnementale.developpement-durable.gouv.fr/api/PublishedDocument/Get"
NATIONAL_PORTAL_BASE = "https://evaluation-environnementale.ecologie.gouv.fr"

# 8 départements Île-de-France
DEPARTMENTS = {
    "75": ("paris-75-r687.html", "Paris (75)"),
    "77": ("seine-et-marne-77-r691.html", "Seine-et-Marne (77)"),
    "78": ("yvelines-78-r692.html", "Yvelines (78)"),
    "91": ("essonne-91-r693.html", "Essonne (91)"),
    "92": ("hauts-de-seine-92-r688.html", "Hauts-de-Seine (92)"),
    "93": ("seine-saint-denis-93-r689.html", "Seine-Saint-Denis (93)"),
    "94": ("val-de-marne-94-r690.html", "Val-de-Marne (94)"),
    "95": ("val-d-oise-95-r694.html", "Val-d'Oise (95)"),
}

# IDF codes pour filtrer API nationale
IDF_DEPTS = ["75", "77", "78", "91", "92", "93", "94", "95"]


# ============ Utils ============

def _sha1(text: str) -> str:
    """Hash SHA1 pour project_id"""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _is_bess_project(title: str) -> bool:
    """
    Valide si projet est vraiment BESS (stockage d'énergie électrique)
    
    Critère INCLURE:
      - "stockag" + ("électricité" OU "énergie")
      - "batter" + "stockag"
      - "bess"
      - "accumulateur"
      - Code NAF 2925
    
    Critère EXCLURE:
      - carrière, déchet, gaz, carburant, fioul
    """
    combined = title.lower()
    
    # Critères d'inclusion BESS
    has_storage = "stockag" in combined or "stock énerg" in combined
    has_battery = (
        "batter" in combined or "bess" in combined or
        re.search(r"\b2925(-?2)?\b", combined) is not None or
        "accumulateur" in combined or
        ("énergie" in combined and "stockag" in combined) or
        ("électricité" in combined and "stockag" in combined)
    )
    
    is_bess = has_storage or has_battery
    
    if not is_bess:
        logger.debug(f"[BESS-REJECT] {title[:60]}...")
        return False
    
    # Critères d'exclusion (NOT BESS)
    excluded_keywords = [
        "carrière", "carriere", "déchet", "décheterie", "dechet",
        "carburant", "fuel", "gasoil", "essence",
        "gaz naturel", "propane", "butane", "fioul"
    ]
    
    is_excluded = any(k in combined for k in excluded_keywords)
    
    if is_excluded:
        logger.debug(f"[BESS-EXCLUDE] {title[:60]}...")
        return False
    
    logger.debug(f"[BESS-OK] {title[:60]}...")
    return True


def _extract_year_from_url(url: str) -> str:
    """Extrait l'année depuis l'URL du PDF (YYYY-XXXX pattern)"""
    if not url:
        return "2025"
    
    filename = url.split("/")[-1].lower()
    match = re.search(r"^(\d{4})[_-]", filename)
    if match:
        return match.group(1)
    
    return "2025"


def _extract_text_from_html(html_str: str) -> str:
    """Extrait texte depuis HTML (retire les tags)"""
    text = re.sub(r"<[^>]+>", "", html_str)
    return " ".join(text.split()).strip()


def _extract_first_href_from_html(html_str: str) -> Optional[str]:
    """
    Extrait le premier href depuis du HTML
    Utilise regex car plus fiable que selectolax pour les HTML mal formés
    """
    if not html_str:
        return None
    
    # Cherche href="..." dans le HTML
    match = re.search(r'href="([^"]+)"', html_str)
    return match.group(1) if match else None


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
    
    return ""


def _years_in_text(text: str) -> List[int]:
    """Extrait années depuis texte"""
    return [int(y) for y in re.findall(r"\b(20\d{2})\b", text or "")]


# ============ API Portail National (2025+) ============

def _call_national_api(
    year_target: int,
    dept_filter: Optional[str]
) -> List[Project]:
    """
    Appelle API gatew-evaluation-environnementale.developpement-durable.gouv.fr
    pour récupérer projets 2025+ (depuis 26/11/2024)
    
    Structure JSON API:
    {
      "projectTitle": "Île-de-France - Projet de stockage...",
      "department": "77-Seine-et-Marne",
      "municipality": "Vernou (77)",
      "referenceNumber": "007184/KK P",
      "publishedDate": "2025-10-20T15:54:21.96251",
      "documentId": 12248
    }
    """
    logger.info(f"[API NATIONALE] Recherche année={year_target}")
    
    projects = []
    
    try:
        # Paramètres API
        params = {
            "start": 0,
            "length": 200,
            "descending_order_id": "true",
            "place": "Île-de-France",
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
            if not _is_bess_project(combined):
                logger.debug(f"[API] NON-BESS: {project_title[:60]}")
                continue
            
            # Vérifier année
            years = _years_in_text(published_date or combined)
            if year_target not in years:
                logger.debug(f"[API] Année {years} ≠ {year_target}")
                continue
            
            # Extraire département
            dept_code = _dept_from_text(department or municipality or project_title)
            
            # Vérifier département IDF
            if dept_code and dept_code not in IDF_DEPTS:
                logger.debug(f"[API] Dept {dept_code} ∉ IDF")
                continue
            
            # Filtre département utilisateur
            if dept_filter and dept_code != dept_filter.zfill(2):
                continue
            
            # Construire URL projet
            url = ""
            if document_id:
                url = f"{NATIONAL_PORTAL_BASE}/#/public/view-document/{document_id}"
            
            # Créer projet
            project = Project(
                project_id=_sha1(url or reference_number or project_title),
                region="ile-de-france",
                dept=dept_code or "",
                year=str(year_target),
                project_title=project_title[:200],
                project_url=url,
                url_cerfa=None,  # API nationale n'a pas CERFA séparé
                url_decision=url if url else None
            )
            
            projects.append(project)
            logger.info(f"[API] ✓ {project_title[:60]}...")
        
    except Exception as e:
        logger.error(f"[API NATIONALE] Erreur: {e}", exc_info=True)
    
    logger.info(f"[API NATIONALE] Résultat: {len(projects)} projets")
    return projects


# ============ NIVEAU 1: Extraction des départements ============

def _fetch_main_page(client) -> Tuple[str, str]:
    """Récupère page principale avec listing des 8 départements"""
    url = urljoin(BASE_URL, MAIN_PAGE)
    logger.info(f"[NIV 1] GET départements: {url}")
    html, final_url = client.get_text(url)
    time.sleep(DELAY)
    return html, final_url


def _extract_departments_from_html(html: str, base_url: str) -> Dict[str, str]:
    """
    Parse page principale et extrait URLs des 8 départements IDF
    
    Returns: {dept_code: full_url}
    """
    tree = HTMLParser(html)
    departments = {}
    
    # Cherche tous les liens avec pattern "XXXX-NN-rNNN.html"
    for link in tree.css("a"):
        href = link.attributes.get("href", "")
        text = link.text() or ""
        
        if not href or not text:
            continue
        
        # Cherche numéro de département: "Paris (75)"
        dept_match = re.search(r"\((\d{2})\)", text)
        if not dept_match:
            continue
        
        dept_code = dept_match.group(1)
        
        # Valider c'est un lien département (pas une rubrique générique)
        if not re.search(r"-r\d{3,4}\.html$", href):
            continue
        
        full_url = urljoin(base_url, href)
        departments[dept_code] = full_url
        logger.debug(f"  Dept {dept_code}: {href}")
    
    logger.info(f"  ✓ {len(departments)} départements trouvés")
    return departments


# ============ NIVEAU 2: Extraction des années par département ============

def _fetch_department_page(client, dept_url: str) -> Tuple[str, str]:
    """Récupère page département avec listing des années"""
    logger.info(f"[NIV 2] GET années: {dept_url}")
    html, final_url = client.get_text(dept_url)
    time.sleep(DELAY)
    return html, final_url


def _extract_years_from_html(html: str) -> Dict[str, str]:
    """
    Parse page département et extrait URLs des années
    
    Pattern: "2025-cas-par-cas-77-a13125.html" → {"2025": full_url}
    
    Returns: {year: full_url}
    """
    tree = HTMLParser(html)
    years = {}
    
    # Cherche liens "YYYY-cas-par-cas-NN-aXXXX.html"
    for link in tree.css("a"):
        href = link.attributes.get("href", "")
        text = link.text() or ""
        
        if not href or not text:
            continue
        
        # Pattern: YYYY-cas-par-cas-NN-aXXXX.html
        year_match = re.search(r"^(\d{4})-cas-par-cas", href)
        if not year_match:
            continue
        
        year = year_match.group(1)
        years[year] = href  # Conserver URL relative (urljoin fait dans le caller)
        logger.debug(f"  Year {year}: {href}")
    
    logger.info(f"  ✓ {len(years)} années trouvées")
    return years


# ============ NIVEAU 3: Parsing du tableau des projets ============

def _fetch_year_page(client, year_url: str) -> Tuple[str, str]:
    """Récupère page année/département avec tableau des projets"""
    logger.info(f"[NIV 3] GET tableau: {year_url}")
    html, final_url = client.get_text(year_url)
    time.sleep(DELAY)
    return html, final_url


def _parse_projects_table(html: str, base_url: str, year: str, dept: str) -> List[Dict]:
    """
    Parse le tableau 5 colonnes et extrait projets BESS
    
    Structure tableau:
      <table>
        <tr><td>CERFA PDF</td><td>Commune et intitulé</td><td>Date réc</td><td>Date lim</td><td>Décision PDF</td></tr>
    
    Args:
      html: contenu HTML de la page année/département
      base_url: URL de base pour urljoin
      year: année (ex: "2024")
      dept: code département (ex: "77")
    
    Returns:
      Liste de dictionnaires projets BESS
    """
    projects = []
    
    # Cherche le tableau
    tree = HTMLParser(html)
    table = tree.css_first("table.spip, table")
    
    if not table:
        logger.warning(f"  ⚠️  Aucun tableau trouvé")
        return []
    
    rows = table.css("tr")
    logger.info(f"  Lignes du tableau: {len(rows)}")
    
    # Skip header (première ligne)
    data_rows = rows[1:] if rows else []
    
    for idx, row in enumerate(data_rows):
        try:
            cells = row.css("td")
            
            if len(cells) < 5:
                logger.debug(f"    Row {idx}: {len(cells)} colonnes, skip")
                continue
            
            # Extraire le HTML brut des cellules (plus fiable)
            # car cells[X].html peut avoir des problèmes de parsing
            cell_cerfa_html = cells[0].html if hasattr(cells[0], 'html') else str(cells[0])
            cell_title_html = cells[1].html if hasattr(cells[1], 'html') else str(cells[1])
            cell_date_reception_html = cells[2].html if hasattr(cells[2], 'html') else str(cells[2])
            cell_date_limite_html = cells[3].html if hasattr(cells[3], 'html') else str(cells[3])
            cell_decision_html = cells[4].html if hasattr(cells[4], 'html') else str(cells[4])
            
            # Extraction titre
            title_text = _extract_text_from_html(cell_title_html).strip()
            
            if not title_text:
                logger.debug(f"    Row {idx}: titre vide, skip")
                continue
            
            # ===== FILTRE BESS =====
            if not _is_bess_project(title_text):
                logger.debug(f"    Row {idx}: ❌ NON-BESS")
                continue
            
            logger.info(f"    Row {idx}: ✅ BESS - {title_text[:70]}")
            
            # Extraction URLs (CORRIGÉ: utiliser regex sur HTML brut)
            url_cerfa = _extract_first_href_from_html(cell_cerfa_html)
            if url_cerfa:
                url_cerfa = urljoin(base_url, url_cerfa)
            
            url_decision = _extract_first_href_from_html(cell_decision_html)
            if url_decision:
                url_decision = urljoin(base_url, url_decision)
            
            # Dates
            date_reception = _extract_text_from_html(cell_date_reception_html)
            date_limite = _extract_text_from_html(cell_date_limite_html)
            
            # Année depuis DÉCISION > CERFA (priorité)
            final_year = year
            if url_decision:
                final_year = _extract_year_from_url(url_decision)
            elif url_cerfa:
                final_year = _extract_year_from_url(url_cerfa)
            
            # Project key pour groupage
            project_key = _sha1(f"{title_text}-{dept}-{final_year}")
            
            projects.append({
                "project_key": project_key,
                "title": title_text,
                "url_cerfa": url_cerfa,
                "url_decision": url_decision,
                "date_reception": date_reception,
                "date_limite": date_limite,
                "year": final_year,
                "dept": dept
            })
            
        except Exception as e:
            logger.warning(f"    Row {idx} parse error: {e}")
            continue
    
    logger.info(f"  Projets BESS trouvés: {len(projects)}")
    return projects


# ============ Conversion vers Project objects ============

def _build_project_object(proj_dict: Dict) -> Project:
    """Crée objet Project depuis dict interne"""
    
    # URL principale: CERFA si dispo, sinon DÉCISION
    main_url = proj_dict["url_cerfa"] or proj_dict["url_decision"] or ""
    
    return Project(
        project_id=proj_dict["project_key"],
        region="ile-de-france",
        dept=proj_dict["dept"],
        year=proj_dict["year"],
        project_title=proj_dict["title"][:200],
        project_url=main_url,
        url_cerfa=proj_dict["url_cerfa"],
        url_decision=proj_dict["url_decision"]
    )


# ============ API Principale ============

def discover_projects(
    year: Optional[str] = None,
    client=None,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None
) -> List[Project]:
    """
    API principale Île-de-France - HIÉRARCHIE 3 NIVEAUX + API NATIONALE 2025+
    
    ARCHITECTURE:
      1. Récupère page principale → extrait 8 départements
      2. Pour chaque département:
         a. Récupère page département → extrait années (2018-2025)
         b. Pour chaque année:
            i. SI année < 2025: scrape tableau régional + filtre BESS
            ii. SI année ≥ 2025: appelle API portail national
    
    RETOURNE:
      Liste d'objets Project avec clés:
        - project_id: SHA1(titre + dept + year)
        - region: "ile-de-france"
        - dept: Code département (ex: "77")
        - year: Année (extraite du PDF ou API)
        - project_title: Commune et intitulé
        - project_url: URL CERFA ou DÉCISION
        - url_cerfa: URL CERFA (ou None)
        - url_decision: URL DÉCISION (ou None)
    
    PARAMÈTRES OPTIONNELS:
      - year: filtrer par année (ex: "2024")
      - dept: filtrer par département (ex: "77")
      - client: HTTPClient depuis utils.py
      - seed_url: NON UTILISÉ pour cette région
    """
    
    logger.info(
        f"\n{'='*70}\n"
        f"SCRAPER ÎLE-DE-FRANCE (HIÉRARCHIE 3 NIVEAUX + API 2025+)\n"
        f"Année filtre: {year or 'TOUTES'}\n"
        f"Dept filtre: {dept or 'TOUS (8 depts)'}\n"
        f"Filtre BESS: ACTIVÉ\n"
        f"URL formulaires CERFA + Décisions\n"
        f"Regroupement: 1 ligne/projet (titre + dept + year)\n"
        f"Année < 2025: Site régional | Année ≥ 2025: API nationale\n"
        f"{'='*70}\n"
    )
    
    all_projects = []
    
    try:
        # ============ NIVEAU 1: Récupère départements ============
        
        main_html, main_url = _fetch_main_page(client)
        depts_extracted = _extract_departments_from_html(main_html, BASE_URL)
        
        if not depts_extracted:
            logger.error("❌ Aucun département trouvé")
            return []
        
        # Filtre département si demandé
        depts_to_process = {}
        if dept:
            if dept in depts_extracted:
                depts_to_process[dept] = depts_extracted[dept]
                logger.info(f"  ✓ Filtre appliqué: dept {dept}")
            else:
                logger.error(f"  ❌ Département {dept} non trouvé")
                return []
        else:
            depts_to_process = depts_extracted
        
        logger.info(f"  Départements à traiter: {list(depts_to_process.keys())}\n")
        
        # ============ NIVEAU 2 & 3: Traite chaque département ============
        
        for dept_code, dept_url in depts_to_process.items():
            try:
                logger.info(f"{'─'*70}\n[DÉPARTEMENT {dept_code}]\n")
                
                # Récupère page département
                dept_html, dept_final_url = _fetch_department_page(client, dept_url)
                years_extracted = _extract_years_from_html(dept_html)
                
                if not years_extracted:
                    logger.warning(f"  ⚠️  Aucune année trouvée")
                    continue
                
                # Filtre année si demandé
                years_to_process = {}
                if year:
                    if year in years_extracted:
                        years_to_process[year] = years_extracted[year]
                        logger.info(f"  ✓ Filtre appliqué: year {year}\n")
                else:
                    years_to_process = years_extracted
                
                # Traite chaque année
                for year_str, year_rel_url in years_to_process.items():
                    try:
                        logger.info(f"[ANNÉE {year_str}]")
                        year_int = int(year_str)
                        
                        projects = []
                        
                        # STRATÉGIE: année < 2025 → site régional | année ≥ 2025 → API nationale
                        if year_int < 2025:
                            # Scrape site régional
                            year_url = urljoin(dept_final_url, year_rel_url)
                            year_html, year_final_url = _fetch_year_page(client, year_url)
                            
                            projects = _parse_projects_table(
                                year_html,
                                year_final_url,
                                year_str,
                                dept_code
                            )
                        else:
                            # Appelle API nationale pour 2025+
                            logger.info(f"  [API NATIONALE] Année {year_str} (≥2025)")
                            national_projects = _call_national_api(year_int, dept_code)
                            
                            # Convertir en dict interne pour cohérence
                            for proj in national_projects:
                                projects.append({
                                    "project_key": proj.project_id,
                                    "title": proj.project_title,
                                    "url_cerfa": proj.url_cerfa,
                                    "url_decision": proj.url_decision,
                                    "date_reception": "",
                                    "date_limite": "",
                                    "year": proj.year,
                                    "dept": proj.dept
                                })
                        
                        all_projects.extend(projects)
                        logger.info("")
                        
                    except Exception as e:
                        logger.error(f"  ❌ Year {year_str} FAIL: {e}")
                        continue
            
            except Exception as e:
                logger.error(f"  ❌ Dept {dept_code} FAIL: {e}")
                continue
        
        # ============ Conversion vers Project objects ============
        
        project_objects = []
        
        # Dédupliquer par project_key
        unique_projects = {}
        for proj in all_projects:
            key = proj["project_key"]
            if key not in unique_projects:
                unique_projects[key] = proj
        
        logger.info(f"\n{'='*70}\nDéduplication: {len(all_projects)} → {len(unique_projects)} uniques\n")
        
        for proj_dict in unique_projects.values():
            try:
                proj_obj = _build_project_object(proj_dict)
                project_objects.append(proj_obj)
            except Exception as e:
                logger.warning(f"Project object creation FAIL: {e}")
                continue
        
        # Résumé final
        logger.info(
            f"{'='*70}\n"
            f"RÉSUMÉ ÎLE-DE-FRANCE\n"
            f"Total projets BESS: {len(project_objects)}\n"
            f"{'='*70}\n"
        )
        
        return project_objects
    
    except Exception as e:
        logger.error(f"❌ Scraper FAIL: {e}")
        return []
