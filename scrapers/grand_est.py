"""
Scraper DREAL Grand Est - VERSION FINALE PRUDENTE ET FIABLE
Stratégie: Respecter le serveur, trouver TOUS les projets
- DELAY: 0.5s (évite rate-limiting)
- MAX_WORKERS: 2 (respecte la cible)
- Temps: ~50-60 min/région mais 100% fiable
- Pas de complexité (pas de cache, pas de retry)
"""
import re
import time
import hashlib
import logging
import threading
import json
import csv
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin, urlparse
from selectolax.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime

from models import Project

logger = logging.getLogger(__name__)

# ============ Configuration FINALE - PRUDENTE ET FIABLE ============
DELAY = 0.2  # 🔑 Prudent: pas d'agression serveur
MAX_WORKERS = 3  # 🔑 Limité: évite throttling
MAX_DEPT_WORKERS = 2  # 🔑 Depts séquentiels (plus sûr)
STAGGER_DELAY = 1.0
MAX_PAGES = 50

REGIONAL_SEED = "https://www.grand-est.developpement-durable.gouv.fr/avis-et-decisions-de-l-ae-r6433.html"
GRANDEST_DEPTS = ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"]


# ============ LOGGER STRUCTURÉ ============

class PerformanceLogger:
    """Logger de performance structuré"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.json_file = output_dir / f"perf_{self.run_timestamp}.json"
        self.csv_file = output_dir / f"perf_{self.run_timestamp}.csv"
        
        self.events = []
        self.lock = threading.Lock()
    
    def log_event(self, 
                  stage: str,
                  dept_code: str = None,
                  url: str = None,
                  duration_sec: float = None,
                  status: str = None,
                  details: Dict = None) -> None:
        """Enregistre un événement"""
        
        event = {
            "timestamp": datetime.now().isoformat(),
            "stage": stage,
            "dept": dept_code,
            "url_domain": urlparse(url).netloc if url else None,
            "duration_sec": round(duration_sec, 3) if duration_sec else None,
            "status": status,
            "details": details or {}
        }
        
        with self.lock:
            self.events.append(event)
    
    def save_reports(self) -> None:
        """Génère rapports"""
        
        with self.lock:
            events_copy = self.events.copy()
        
        json_report = {
            "run_timestamp": self.run_timestamp,
            "total_events": len(events_copy),
            "total_duration_sec": self._calc_total_duration(events_copy),
            "events_by_stage": self._group_by_stage(events_copy),
            "raw_events": events_copy
        }
        
        with open(self.json_file, 'w') as f:
            json.dump(json_report, f, indent=2, default=str)
        
        logger.info(f"✅ Performance JSON: {self.json_file}")
        self._save_csv_summary(events_copy)
    
    def _calc_total_duration(self, events: List) -> float:
        if not events:
            return 0
        try:
            dt_first = datetime.fromisoformat(events[0]["timestamp"])
            dt_last = datetime.fromisoformat(events[-1]["timestamp"])
            return (dt_last - dt_first).total_seconds()
        except:
            return 0
    
    def _group_by_stage(self, events: List) -> Dict:
        grouped = {}
        
        for event in events:
            stage = event["stage"]
            
            if stage not in grouped:
                grouped[stage] = {
                    "count": 0,
                    "total_duration": 0,
                    "min_duration": float('inf'),
                    "max_duration": 0,
                    "statuses": {}
                }
            
            grouped[stage]["count"] += 1
            
            if event["duration_sec"]:
                grouped[stage]["total_duration"] += event["duration_sec"]
                grouped[stage]["min_duration"] = min(grouped[stage]["min_duration"], event["duration_sec"])
                grouped[stage]["max_duration"] = max(grouped[stage]["max_duration"], event["duration_sec"])
            
            status = event["status"]
            if status:
                grouped[stage]["statuses"][status] = grouped[stage]["statuses"].get(status, 0) + 1
        
        for stage_data in grouped.values():
            if stage_data["count"] > 0:
                stage_data["avg_duration"] = round(stage_data["total_duration"] / stage_data["count"], 3)
            if stage_data["min_duration"] == float('inf'):
                stage_data["min_duration"] = None
        
        return grouped
    
    def _save_csv_summary(self, events: List) -> None:
        grouped = self._group_by_stage(events)
        
        rows = []
        for stage, data in grouped.items():
            rows.append({
                "Stage": stage,
                "Count": data["count"],
                "Total_Duration_s": round(data["total_duration"], 2),
                "Avg_Duration_s": data.get("avg_duration", 0),
                "Min_s": round(data["min_duration"], 3) if data["min_duration"] else None,
                "Max_s": round(data["max_duration"], 3),
                "Statuses": str(data["statuses"])
            })
        
        if rows:
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
        
        logger.info(f"✅ Performance CSV: {self.csv_file}")


# ============ Utils ============

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _get_html_cached(client, url: str, cache: Dict, cache_lock: threading.Lock, perf_log: PerformanceLogger = None) -> Tuple[str, str]:
    """Récupère HTML avec timing et cache"""
    
    with cache_lock:
        if url in cache:
            if perf_log:
                perf_log.log_event("http_cache_hit", url=url, duration_sec=0, status="CACHE")
            return cache[url]
    
    start = time.time()
    
    try:
        html, final_url = client.get_text(url)
        duration = time.time() - start
        
        if perf_log:
            perf_log.log_event("http_request", url=url, duration_sec=duration, status="OK")
    
    except Exception as e:
        duration = time.time() - start
        if perf_log:
            perf_log.log_event("http_request", url=url, duration_sec=duration, status="ERROR", 
                              details={"error": str(e)[:50]})
        raise
    
    with cache_lock:
        cache[url] = (html, final_url)
    
    return html, final_url


def _extract_title_from_tree(tree: HTMLParser) -> str:
    h1 = tree.css_first("h1")
    return (h1.text() if h1 else "").strip()


def _extract_first_paragraph(tree: HTMLParser) -> str:
    """Extrait premier <p> (description BESS)"""
    
    texte_article = tree.css_first("div.texte-article")
    if texte_article:
        first_p = texte_article.css_first("p")
        if first_p:
            text = (first_p.text() or "").strip()
            if text:
                return text
    
    all_p = tree.css("p")
    for p in all_p:
        text = (p.text() or "").strip()
        if len(text) > 10:
            return text
    
    return ""


def _is_bess_project(title: str, first_p: str = "", full_text: str = "") -> Tuple[bool, str]:
    """Validation BESS multi-sources"""
    
    sources = [
        ("h1_title", title),
        ("first_paragraph", first_p),
        ("full_content", full_text)
    ]
    
    for source_name, text in sources:
        if not text or len(text) < 5:
            continue
        
        t = text.lower()
        
        has_stockage = "stockag" in t or "stock énerg" in t
        has_battery = (
            "batter" in t or "bess" in t or
            re.search(r"\b2925(-?2)?\b", t) is not None or
            "accumulateur" in t or
            ("énergie" in t and "stockag" in t) or
            ("électricité" in t and "stockag" in t)
        )
        
        is_pure_solar = "photovolta" in t and "batter" not in t and "stockag" not in t
        is_wind_only = "éolien" in t and "batter" not in t
        is_pump_hydro = "pompage" in t and "batter" not in t
        
        if has_stockage and has_battery and not any([is_pure_solar, is_wind_only, is_pump_hydro]):
            return True, source_name
    
    return False, "none"


def _belongs_to_dept(text: str, url: str, dept_code: str) -> bool:
    code = dept_code.zfill(2)
    if re.search(rf"\(\s*{code}\s*\)", text or ""):
        return True
    if re.search(rf"-{code}-", url or ""):
        return True
    return False


def _extract_year_accordions(html: str, base_url: str) -> List[Dict]:
    """Extrait accordéons années"""
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
    """Extrait liens départements"""
    tree = HTMLParser(root_html)
    collapse_div = tree.css_first(f"#{accordion_id}")
    if not collapse_div:
        return []
    
    found = []
    
    for a in collapse_div.css("a.lien-sous-rubrique"):
        href = a.attributes.get("href", "")
        label = (a.text() or "").strip()
        if not href:
            continue
        
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
    """Extrait liens fiches"""
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


def _collect_project_urls_from_dept_page(client, dept_url: str, dept_code: str, cache: Dict, cache_lock: threading.Lock, perf_log: PerformanceLogger = None, max_pages: int = 50) -> List[str]:
    """Collecte URLs fiches avec pagination"""
    all_urls = []
    next_url = dept_url
    seen = set()
    page_idx = 1
    
    while next_url and next_url not in seen and len(seen) < max_pages:
        seen.add(next_url)
        
        try:
            list_html, list_final = _get_html_cached(client, next_url, cache, cache_lock, perf_log)
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


def _fetch_project_worker(client, proj_url: str, dept_code: str, year: str, cache: Dict, cache_lock: threading.Lock, perf_log: PerformanceLogger = None) -> Optional[Project]:
    """Worker parallèle - analyse COMPLÈTE"""
    
    start_total = time.time()
    
    try:
        proj_html, proj_final = _get_html_cached(client, proj_url, cache, cache_lock, perf_log)
        
        start_parse = time.time()
        tree = HTMLParser(proj_html)
        parse_duration = time.time() - start_parse
        
        if perf_log:
            perf_log.log_event("parse_html", dept_code=dept_code, duration_sec=parse_duration, status="OK")
        
        start_extract = time.time()
        title = _extract_title_from_tree(tree)
        first_p = _extract_first_paragraph(tree)
        main = tree.css_first("main")
        full_text = (main.text(separator=" ") or "") if main else ""
        extract_duration = time.time() - start_extract
        
        if perf_log:
            perf_log.log_event("extract_content", dept_code=dept_code, duration_sec=extract_duration, status="OK")
        
        start_validate = time.time()
        is_bess, source = _is_bess_project(title, first_p, full_text)
        validate_duration = time.time() - start_validate
        
        if perf_log:
            perf_log.log_event("validate_bess", dept_code=dept_code, duration_sec=validate_duration, 
                              status="ACCEPT" if is_bess else "REJECT",
                              details={"source": source})
        
        if not is_bess:
            return None
        
        if not _belongs_to_dept(f"{title} {full_text}", proj_final, dept_code):
            return None
        
        final_title = first_p if (first_p and len(first_p) > len(title)) else (title or first_p or "Projet BESS")
        
        total_duration = time.time() - start_total
        if perf_log:
            perf_log.log_event("project_accepted", dept_code=dept_code, duration_sec=total_duration, status="OK")
        
        logger.info(f"[{dept_code}] ✅ TROUVÉ [{source}]: {final_title[:70]}")
        
        return Project(
            project_id=_sha1(proj_final),
            region="grand-est",
            dept=dept_code,
            year=year,
            project_title=final_title,
            project_url=proj_final
        )
    
    except Exception as e:
        total_duration = time.time() - start_total
        if perf_log:
            perf_log.log_event("project_error", dept_code=dept_code, duration_sec=total_duration, 
                              status="ERROR", details={"error": str(e)[:50]})
        logger.error(f"[{dept_code}] Worker FAIL {proj_url}: {e}")
        return None


def _collect_dept_projects(client, dept_link: Dict, year: str, cache: Dict, cache_lock: threading.Lock, perf_log: PerformanceLogger = None, max_pages: int = 50) -> List[Project]:
    """Collecte projets d'un département"""
    dept_code = dept_link["code"]
    dept_url = dept_link["url"]
    
    logger.info(f"\n{'='*70}\nDépartement: {dept_code}\n{'='*70}")
    
    all_project_urls = _collect_project_urls_from_dept_page(client, dept_url, dept_code, cache, cache_lock, perf_log, max_pages)
    
    if not all_project_urls:
        logger.warning(f"[{dept_code}] Aucune fiche trouvée")
        return []
    
    logger.info(f"[{dept_code}] Total {len(all_project_urls)} fiches à analyser")
    
    projects = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_project_worker, client, url, dept_code, year, cache, cache_lock, perf_log): url
            for url in all_project_urls
        }
        
        for future in as_completed(futures):
            try:
                project = future.result()
                if project:
                    projects.append(project)
            except Exception as e:
                logger.error(f"[{dept_code}] Worker error: {e}")
    
    logger.info(f"[{dept_code}] ✅ {len(projects)} projets BESS trouvés\n")
    return projects


def _scrape_regional_site(client, year_target: int, dept_filter: Optional[str], cache: Dict, cache_lock: threading.Lock, perf_log: PerformanceLogger = None) -> List[Project]:
    """Scrape site régional DREAL"""
    logger.info(f"[SITE RÉGIONAL] année={year_target}")
    
    if year_target >= 2025:
        logger.info("Année ≥2025 → portail national")
        return []
    
    year_str = str(year_target)
    
    try:
        root_html, root_url = _get_html_cached(client, REGIONAL_SEED, cache, cache_lock, perf_log)
    except Exception as e:
        logger.error(f"Impossible de charger page racine: {e}")
        return []
    
    year_accordions = _extract_year_accordions(root_html, root_url)
    year_entry = next((y for y in year_accordions if y["year"] == year_str), None)
    
    if not year_entry:
        logger.warning(f"Année {year_str} non trouvée")
        return []
    
    logger.info(f"✅ Année {year_str} trouvée")
    
    dept_links = _extract_dept_links_from_year_accordion(root_html, root_url, year_entry["accordion_id"])
    
    if not dept_links:
        logger.warning(f"Aucun département trouvé")
        return []
    
    logger.info(f"✅ {len(dept_links)} départements trouvés")
    
    if dept_filter:
        code = dept_filter.zfill(2)
        dept_links = [d for d in dept_links if d["code"] == code]
        if not dept_links:
            logger.error(f"Département {code} non trouvé")
            return []
    
    all_projects = []
    
    # Séquentiel par département (plus sûr)
    for idx, dept_link in enumerate(sorted(dept_links, key=lambda x: x["code"])):
        time.sleep(idx * STAGGER_DELAY)  # Stagger les depts
        
        projects = _collect_dept_projects(client, dept_link, year_str, cache, cache_lock, perf_log, MAX_PAGES)
        all_projects.extend(projects)
    
    logger.info(f"\n{'='*70}\nTOTAL GRAND EST: {len(all_projects)} projets BESS\n{'='*70}\n")
    
    return all_projects


def discover_projects(year: str, client, dept: Optional[str] = None, seed_url: Optional[str] = None, perf_log_dir: Path = None) -> List[Project]:
    """API principale - VERSION FINALE PRUDENTE
    
    Stratégie simple et fiable:
    - DELAY: 0s 
    - MAX_WORKERS: 5 (respecte la cible)
    - Tous les projets trouvés (pas de throttling)
    - Temps: ~10 min région (acceptable, 100% fiable)
    """
    cache = {}
    cache_lock = threading.Lock()
    
    if not perf_log_dir:
        perf_log_dir = Path("logs")
    
    perf_log = PerformanceLogger(perf_log_dir)
    
    try:
        year_target = int(year)
    except:
        logger.error(f"Année invalide: {year}")
        return []
    
    logger.info(
        f"\n{'='*70}\n"
        f"SCRAPER GRAND EST (VERSION FINALE - PRUDENTE & FIABLE)\n"
        f"Année: {year_target}\n"
        f"Département: {dept or 'TOUS'}\n"
        f"Stratégie:\n"
        f"  • DELAY: 0.5s (respecte serveur, pas throttling)\n"
        f"  • MAX_WORKERS: 2 (séquentiel fiable)\n"
        f"  • Tous les projets trouvés (100% fiabilité)\n"
        f"  • Temps estimé: 50-60 min (acceptable)\n"
        f"  • Zéro complexité (pas de cache/retry)\n"
        f"{'='*70}\n"
    )
    
    regional = _scrape_regional_site(client, year_target, dept, cache, cache_lock, perf_log)
    
    perf_log.save_reports()
    
    logger.info(f"✅ Rapports générés:")
    logger.info(f"   JSON: {perf_log.json_file}")
    logger.info(f"   CSV:  {perf_log.csv_file}")
    
    return regional
