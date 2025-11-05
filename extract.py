#!/usr/bin/env python3
"""
CLI d'extraction et d'analyse de documents BESS - VERSION MULTI-URLs
Support URLs multiples par projet (Hauts-de-France)
Chaque projet peut avoir: url_cerfa, url_decision, url_autre

Usage: python extract.py --input out/projects/hauts_de_france_2025.csv
"""
import sys
import os
import json
import time
import re
import asyncio
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import pandas as pd
import google.generativeai as genai
from selectolax.parser import HTMLParser

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from config import settings
from models import Analysis
from utils import (
    HTTPClient, 
    html_to_text, 
    extract_pdf_robust, 
    extract_zip_archive,
    slugify,
    SimpleCache
)

logger = logging.getLogger(__name__)

# Configuration Gemini
if settings.GOOGLE_API_KEY:
    genai.configure(api_key=settings.GOOGLE_API_KEY)

# Configuration parallélisation
MAX_WORKERS_DOWNLOAD = 5
MAX_WORKERS_EXTRACTION = 3
MAX_PLAYWRIGHT_INSTANCES = 2


# ============ Configuration ============

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )


# ============ NOUVEAU: Charger projets avec multi-URLs ============

def load_projects_from_csv(csv_path: Path) -> List[Dict]:
    """
    Charge CSV et détecte automatiquement les URLs (1, 2, 3 ou plus)
    
    Supporte:
    - project_url (standard, colonne principale)
    - url_cerfa (optionnel)
    - url_decision (optionnel)
    - url_autre (optionnel)
    """
    df = pd.read_csv(csv_path, dtype=str)
    projects = []
    
    for idx, row in df.iterrows():
        urls = []
        
        # Collecte toutes les URLs disponibles
        for col_name in ["url_cerfa", "url_decision", "project_url", "url_autre"]:
            if col_name in df.columns:
                url_val = str(row.get(col_name, "")).strip()
                if url_val and url_val not in urls and url_val.lower() != "nan":
                    urls.append(url_val)
        
        if not urls:
            logger.warning(f"[Row {idx}] Aucune URL trouvée pour {row.get('project_id', '?')}")
            continue
        
        projects.append({
            "project_id": str(row["project_id"]).strip(),
            "project_title": str(row.get("project_title", "")).strip(),
            "region": str(row.get("region", "")).strip(),
            "dept": str(row.get("dept", "")).strip(),
            "year": str(row.get("year", "")).strip(),
            "urls": urls,  # ⭐ LISTE d'URLs (1 ou plusieurs)
            "status": str(row.get("status", "unknown")).strip()
        })
    
    logger.info(f"Loaded {len(projects)} projects")
    return projects


# ============ Document Discovery ============

def find_document_links(html: str, base_url: str) -> List[Dict]:
    """Trouve tous les liens PDF/ZIP dans le HTML"""
    doc_pattern = r"\.(pdf|zip)($|\?)"
    tree = HTMLParser(html)
    documents = []
    
    for anchor in tree.css("a"):
        href = anchor.attributes.get("href", "")
        if not href or not re.search(doc_pattern, href, re.I):
            continue
        
        url = urljoin(base_url, href)
        label = (anchor.text() or "").strip()
        ext = "zip" if re.search(r"\.zip($|\?)", href, re.I) else "pdf"
        
        documents.append({
            "url": url,
            "label": label,
            "ext": ext
        })
    
    # Déduplication
    seen = set()
    unique_docs = []
    for doc in documents:
        if doc["url"] not in seen:
            seen.add(doc["url"])
            unique_docs.append(doc)
    
    return unique_docs


def classify_document(label: str, url: str) -> str:
    """Classifie un document selon son label/URL"""
    text = f"{label} {url}".lower()
    
    if re.search(r"cerfa|cas\s*par\s*cas|formulaire", text):
        return "CERFA"
    
    decision_patterns = [
        r"\bdec[\-_]",
        r"d[ée]cision",
        r"arr[ée]t[ée]",
        r"avis\s+mrae",
        r"soumis",
    ]
    
    if any(re.search(pattern, text) for pattern in decision_patterns):
        return "DECISION"
    
    return "AUTRE"


def prioritize_documents(documents: List[Dict]) -> List[Dict]:
    """Trie les documents par importance"""
    priority = {"DECISION": 0, "CERFA": 1, "AUTRE": 2}
    
    classified = []
    for doc in documents:
        doc_type = classify_document(doc["label"], doc["url"])
        classified.append({**doc, "type": doc_type})
    
    classified.sort(key=lambda d: (priority.get(d["type"], 2), d["url"]))
    return classified


# ============ Gemini Analysis ============

EXTRACTION_PROMPT = """Tu es un extracteur expert pour projets BESS (Battery Energy Storage Systems) en France.
Analyse les documents officiels fournis et retourne UNIQUEMENT un JSON valide.

Schéma JSON attendu:
{
  "project_name": "string|null",
  "puissance_MW": number|null,
  "duree_h": number|null,
  "energie_MWh": number|null,
  "technologie": "string|null",
  "demandeur": "string|null",
  "commune": "string|null",
  "cas_par_cas_decision": "soumis_EI"|"non_soumis_EI"|null,
  "cas_par_cas_date": "YYYY-MM-DD"|null,
  "decision_detail": "string|null",
  "notes": "string|null"
}

Règles strictes:
1. **project_name**: Extrait le NOM OFFICIEL du projet tel qu'il apparaît dans les documents (ex: "BESS Mont Courselle", "Station de stockage d'Amargue", "Projet RTE Patis"). Si aucun nom propre, construis un nom descriptif court (ex: "BESS {commune}").
2. Convertis kW→MW, kWh→MWh
3. Calcule energie_MWh = puissance_MW × duree_h si possible
4. Dates format ISO 8601 (YYYY-MM-DD)
5. null si info absente (ne pas inventer)
6. UNIQUEMENT le JSON en réponse, aucun texte avant/après
"""


def analyze_with_gemini(text_chunks: List[str], metadata: Dict) -> Dict:
    """Analyse LLM des documents via Gemini"""
    if not settings.GOOGLE_API_KEY:
        logger.warning("GOOGLE_API_KEY non configuré, analyse LLM ignorée")
        return {}
    
    model = genai.GenerativeModel(
        settings.GEMINI_MODEL,
        generation_config={
            "temperature": settings.GEMINI_TEMPERATURE,
            "response_mime_type": "application/json"
        }
    )
    
    # Concaténation textes avec limites
    combined = ""
    for chunk in text_chunks:
        truncated = chunk[:settings.MAX_PER_DOC_CHARS]
        if len(combined) + len(truncated) + 1000 > settings.MAX_GEMINI_CHARS:
            break
        combined += "\n\n==== DOCUMENT ====\n\n" + truncated
    
    prompt = f"{EXTRACTION_PROMPT}\n\nMétadonnées:\n{json.dumps(metadata, ensure_ascii=False)}\n\nTextes:\n{combined}"
    
    try:
        response = model.generate_content(prompt)
        result = json.loads(response.text)
        return result
    except json.JSONDecodeError as e:
        logger.error(f"Gemini JSON parse error: {e}")
        return {}
    except Exception as e:
        logger.error(f"Gemini analysis failed: {e}")
        return {}


# ============ NOUVEAU: Télécharge multiple URLs ============

def download_urls_for_project(
    project_id: str,
    urls: List[str],
    http_client: HTTPClient,
    cache: Optional[SimpleCache] = None
) -> Dict[str, Tuple[bytes, str]]:
    """
    Télécharge TOUTES les URLs du projet en parallèle
    
    Returns: {url: (bytes, status), ...}
    """
    logger.info(f"[{project_id}] Téléchargement {len(urls)} URLs...")
    
    downloaded = {}
    
    def download_worker(url: str) -> Tuple[str, bytes, str]:
        try:
            # Cache check
            cache_key = cache.get_key(url) if cache else None
            if cache and cache_key:
                cached_data = cache.load(cache_key, "pdfs")
                if cached_data:
                    logger.debug(f"Cache hit: {url}")
                    return url, cached_data, "cached"
            
            # Téléchargement
            blob, _ = http_client.get_bytes(url)
            
            # Cache save
            if cache and cache_key:
                cache.save(cache_key, blob, "pdfs")
            
            return url, blob, "downloaded"
        except Exception as e:
            logger.error(f"Download failed {url}: {e}")
            return url, b"", "failed"
    
    # Parallélisation
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DOWNLOAD) as executor:
        futures = {executor.submit(download_worker, url): url for url in urls}
        
        for future in as_completed(futures):
            url, blob, status = future.result()
            if blob:
                downloaded[url] = (blob, status)
    
    logger.info(f"[{project_id}] {len(downloaded)}/{len(urls)} téléchargés")
    return downloaded


# ============ Extraction Parallèle ============

def extract_document_worker(args: Tuple) -> Tuple[str, str, str, str]:
    """Worker pour extraction parallèle de texte"""
    filename, data, ext, doc_type = args
    
    try:
        if ext == "pdf":
            text, method = extract_pdf_robust(data)
            return filename, text, method, doc_type
        else:
            return filename, "", "unsupported", doc_type
    except Exception as e:
        logger.error(f"Extraction failed {filename}: {e}")
        return filename, "", "failed", doc_type


# ============ Process Project Optimisé (MULTI-URLs) ============

def process_project_optimized(
    project_data: Dict,
    http_client: HTTPClient,
    cache: Optional[SimpleCache] = None
) -> Optional[Analysis]:
    """
    Traite UN projet avec ses URLs multiples
    - Télécharge TOUTES les URLs
    - Extrait texte de chacune
    - Combine et analyse
    """
    project_id = project_data["project_id"]
    urls = project_data["urls"]  # ⭐ LISTE d'URLs
    
    logger.info(f"Processing: {project_data['project_title'][:60]} ({len(urls)} URLs)")
    
    # Dossier sortie
    output_dir = settings.OUTPUT_DIR / "docs" / project_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # ===== ÉTAPE 1: Télécharge TOUTES les URLs =====
    downloaded_docs = download_urls_for_project(
        project_id,
        urls,
        http_client,
        cache
    )
    
    if not downloaded_docs:
        logger.warning(f"[{project_id}] Aucun PDF téléchargé")
        return None
    
    # ===== ÉTAPE 2: Extraction parallèle =====
    logger.info(f"[{project_id}] Extraction parallèle de {len(downloaded_docs)} documents...")
    
    text_chunks = []
    has_decision = False
    extraction_jobs: List[Tuple[str, bytes, str, str]] = []
    
    for doc_idx, (url, (blob, status)) in enumerate(downloaded_docs.items(), 1):
        # Détecte type document
        doc_type = "AUTRE"
        if "cerfa" in url.lower():
            doc_type = "CERFA"
        elif "decision" in url.lower():
            doc_type = "DECISION"
            has_decision = True
        
        filename = f"{doc_idx:02d}_{doc_type}_{slugify(url.split('/')[-1][:30])}.pdf"
        save_path = output_dir / filename
        save_path.write_bytes(blob)
        
        extraction_jobs.append((filename, blob, "pdf", doc_type))
    
    # Extraction parallèle
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXTRACTION) as executor:
        futures = {
            executor.submit(extract_document_worker, job): job[0]
            for job in extraction_jobs
        }
        
        for future in as_completed(futures):
            filename, text, method, doc_type = future.result()
            
            # Sauvegarde texte
            txt_path = output_dir / f"{filename}.txt"
            txt_path.write_text(text, encoding="utf-8")
            
            # Ajout au corpus
            text_chunks.append(
                f"[DOC={filename}|TYPE={doc_type}|METHOD={method}]\n{text or '(vide)'}"
            )
    
    # ===== ÉTAPE 3: Analyse Gemini =====
    metadata = {
        "project_id": project_id,
        "project_title": project_data["project_title"],
        "region": project_data["region"],
        "dept": project_data["dept"],
        "year": project_data["year"],
        "n_urls": len(urls),
        "urls_processed": list(downloaded_docs.keys())
    }
    
    analysis_result = analyze_with_gemini(text_chunks, metadata)

     # ⭐ UTILISER LE NOM EXTRAIT PAR GEMINI
    project_name = analysis_result.get("project_name")
    
    # Fallback si Gemini ne trouve pas de nom
    if not project_name or project_name == "null":
        project_name = project_data["project_title"]  # Titre DREAL (backup)
    
    # ===== ÉTAPE 4: Construction Analysis =====
    return Analysis(
        project_id=project_id,
        project_url=urls[0],  # URL principale (première)
        project_title=project_name,
        dept=project_data["dept"],
        region=project_data["region"],
        year=project_data["year"],
        puissance_MW=analysis_result.get("puissance_MW"),
        duree_h=analysis_result.get("duree_h"),
        energie_MWh=analysis_result.get("energie_MWh"),
        technologie=analysis_result.get("technologie"),
        demandeur=analysis_result.get("demandeur"),
        commune=analysis_result.get("commune"),
        cas_par_cas_decision=analysis_result.get("cas_par_cas_decision"),
        cas_par_cas_date=analysis_result.get("cas_par_cas_date"),
        decision_detail=analysis_result.get("decision_detail"),
        n_docs=len(downloaded_docs),
        has_decision_doc="yes" if has_decision else "no",
        analysis_ok="yes" if analysis_result else "no",
        notes=analysis_result.get("notes")
    )


# ============ Main ============

def main():
    parser = argparse.ArgumentParser(
        description="Extraction et analyse de documents BESS (Support multi-URLs)"
    )
    
    parser.add_argument(
        "--input",
        required=True,
        help="Fichier CSV de projets (doit avoir colonnes: project_url, url_cerfa, url_decision, ...)"
    )
    parser.add_argument(
        "--output",
        help="Fichier CSV de sortie (défaut: out/analyzed/)"
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        default=True,
        help="Utiliser le cache pour PDFs (activé par défaut)"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Désactiver le cache"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limiter nombre de projets (pour tests)"
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Afficher statistiques de performance"
    )
    
    args = parser.parse_args()
    
    configure_logging()
    
    # Validation input
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Fichier introuvable: {input_path}")
        sys.exit(1)
    
    # Output
    if args.output:
        output_path = Path(args.output)
    else:
        filename = input_path.stem.replace("projects", "analyzed") + ".csv"
        output_path = settings.OUTPUT_DIR / "analyzed" / filename
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # ⭐ NOUVEAU: Charge avec support multi-URLs
    projects = load_projects_from_csv(input_path)
    
    if not projects:
        logger.error("Aucun projet chargé")
        sys.exit(1)
    
    # Limite optionnelle
    if args.limit:
        projects = projects[:args.limit]
        logger.info(f"Limited to {len(projects)} projects for testing")
    
    # Cache
    use_cache = args.use_cache and not args.no_cache
    cache = SimpleCache() if use_cache else None
    
    if use_cache:
        logger.info("Cache activé")
    
    # Timer
    start_time = time.time()
    
    # Traitement
    results = []
    
    with HTTPClient() as client:
        for idx, project in enumerate(projects, 1):
            try:
                logger.info(f"[{idx}/{len(projects)}] {project['project_id']}")
                analysis = process_project_optimized(project, client, cache)
                if analysis:
                    results.append(analysis.to_dict())
            except KeyboardInterrupt:
                logger.warning("\nInterruption utilisateur")
                break
            except Exception as e:
                logger.error(f"Project failed: {e}", exc_info=False)
                continue
    
    duration = time.time() - start_time
    
    # Export
    if results:
        pd.DataFrame(results).to_csv(output_path, index=False, encoding="utf-8")
        logger.info(f"✓ {len(results)} projets analysés → {output_path}")
    else:
        logger.warning("Aucun projet analysé avec succès")
        sys.exit(1)
    
    # Résumé
    print("\n" + "="*70)
    print("RÉSUMÉ EXTRACTION")
    print("="*70)
    print(f"Projets traités:   {len(results)}/{len(projects)}")
    print(f"Durée:             {duration:.1f}s")
    print(f"Fichier sortie:    {output_path}")
    print("="*70)
    
    # Benchmark
    if args.benchmark:
        print("\n⏱️  PERFORMANCE")
        print(f"   Durée totale:        {duration:.1f}s")
        print(f"   Projets/seconde:     {len(results)/duration:.2f}")
        print(f"   Temps moyen/projet:  {duration/max(1, len(results)):.1f}s")


if __name__ == "__main__":
    main()
