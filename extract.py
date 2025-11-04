#!/usr/bin/env python3
"""
CLI d'extraction et d'analyse de documents BESS - VERSION OPTIMISÉE
Support Playwright pour portail national (2025+)
Usage: python extract.py --input out/projects/bourgogne_2025.csv
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


# ============ Playwright Attachments (NOUVEAU) ============

async def fetch_attachments_with_playwright(
    project_url: str, 
    document_id: str
) -> List[Dict]:
    """
    Récupère les pièces jointes d'un projet portail national via Playwright.
    Clique sur les boutons de téléchargement et intercepte les URLs ctsFileId.
    
    Args:
        project_url: URL du projet (sera normalisée en view-document)
        document_id: ID du document
    
    Returns:
        Liste de dicts {"url": "...", "label": "...", "ext": "pdf"}
    """
    
    # Normalise l'URL si nécessaire
    if "portal-review" in project_url:
        project_url = project_url.replace("/portal-review/", "/view-document/")
    
    attachments = []
    attachment_urls = set()  # Pour éviter les doublons
    
    logger.info(f"[Playwright] Extraction pièces jointes pour {document_id}")
    
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("[Playwright] Module Playwright non disponible, skipped")
        return []
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Intercepte les téléchargements
            async def on_response(response):
                if response.status == 200 and "ctsFileId" in response.url:
                    attachment_urls.add(response.url)
            
            page.on("response", on_response)
            
            logger.info(f"[Playwright] Navigation vers {project_url}")
            await page.goto(project_url, wait_until="networkidle", timeout=30000)
            
            logger.info("[Playwright] Attente du chargement (5s)...")
            await asyncio.sleep(5)
            
            # Cherche les sections fichiers
            logger.info("[Playwright] Recherche boutons téléchargement...")
            file_sections = await page.query_selector_all("[class*='file']")
            logger.info(f"[Playwright] {len(file_sections)} sections fichiers trouvées")
            
            # Collecte les boutons de chaque section
            download_buttons = []
            for section in file_sections[:20]:  # Max 20 sections
                try:
                    buttons = await section.query_selector_all("button, a")
                    download_buttons.extend(buttons[:3])  # Max 3 par section
                except:
                    pass
            
            logger.info(f"[Playwright] {len(download_buttons)} boutons à cliquer")
            
            # Clique sur chaque bouton et attend la réponse réseau
            clicked = 0
            for btn in download_buttons:
                try:
                    await btn.click()
                    await asyncio.sleep(1.5)  # Attends la requête réseau
                    clicked += 1
                except:
                    pass  # Bouton peut se détacher du DOM
            
            logger.info(f"[Playwright] {clicked} boutons cliqués")
            
            await browser.close()
            
    except asyncio.TimeoutError:
        logger.warning(f"[Playwright] Timeout lors du chargement de {project_url}")
    except Exception as e:
        logger.error(f"[Playwright] Erreur: {e}", exc_info=False)
    
    # Convertit les URLs en objets Document
    for url in sorted(attachment_urls):
        # Extrait le ctsFileId pour un label unique
        match = re.search(r"ctsFileId=(\d+)", url)
        file_id = match.group(1) if match else "unknown"
        
        attachments.append({
            "url": url,
            "label": f"Pièce_jointe_{file_id}",
            "ext": "pdf"
        })
    
    logger.info(f"[Playwright] {len(attachments)} pièces jointes trouvées")
    
    return attachments


# ============ Document Discovery (HTML classique) ============

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
  "puissance_MW": number|null,
  "duree_h": number|null,
  "energie_MWh": number|null,
  "technologie": "string|null",
  "demandeur": "string|null",
  "commune": "string|null",
  "cas_par_cas_decision": "soumis_EI"|"non_soumis_EI"|null,
  "cas_par_cas_date": "YYYY-MM-DD|null",
  "decision_detail": "string|null",
  "notes": "string|null"
}

Règles strictes:
1. Convertis kW→MW, kWh→MWh
2. Calcule energie_MWh = puissance_MW × duree_h si possible
3. Dates format ISO 8601 (YYYY-MM-DD)
4. null si info absente (ne pas inventer)
5. UNIQUEMENT le JSON en réponse, aucun texte avant/après
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


# ============ Téléchargement Parallèle ============

def download_document_worker(args: Tuple) -> Tuple[int, str, bytes, str]:
    """Worker pour téléchargement parallèle de documents"""
    order, http_client, link, cache = args
    doc_url = link["url"]
    
    try:
        # Cache check
        cache_key = cache.get_key(doc_url) if cache else None
        
        if cache and cache_key:
            cached_data = cache.load(cache_key, "pdfs")
            if cached_data:
                logger.debug(f"Cache hit: {doc_url}")
                return order, doc_url, cached_data, "cached"
        
        # Téléchargement
        blob, _ = http_client.get_bytes(doc_url)
        
        # Sauvegarde cache
        if cache and cache_key:
            cache.save(cache_key, blob, "pdfs")
        
        return order, doc_url, blob, "downloaded"
    
    except Exception as e:
        logger.error(f"Download failed {doc_url}: {e}")
        return order, doc_url, b"", "failed"


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


# ============ Process Project Optimisé ============

def process_project_optimized(
    project_data: Dict, 
    http_client: HTTPClient, 
    cache: Optional[SimpleCache] = None
) -> Optional[Analysis]:
    """Traite un projet avec téléchargements et extractions parallèles"""
    project_id = project_data["project_id"]
    url = project_data["project_url"]
    
    logger.info(f"Processing: {project_data['project_title'][:60]}")
    
    # Dossier sortie
    output_dir = settings.OUTPUT_DIR / "docs" / project_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Découverte documents
    doc_links = []
    page_text = ""
    final_url = url
    
    # Routing : Portail National vs Site Régional
    if "evaluation-environnementale.ecologie.gouv.fr" in url:
        # === PORTAIL NATIONAL (2024+) ===
        logger.info(f"[Portail National] Utilisation Playwright pour extraction")
        
        # Extrait documentId
        document_id = url.split("/")[-1].split("?")[0]
        
        # Récupère attachments via Playwright
        try:
            attachments_data = asyncio.run(
                fetch_attachments_with_playwright(url, document_id)
            )
            doc_links = prioritize_documents(attachments_data)
        except Exception as e:
            logger.error(f"[Playwright] Erreur: {e}")
            doc_links = []
    else:
        # === SITE RÉGIONAL (classique) ===
        logger.info(f"[Site Régional] Utilisation parsing HTML")
        
        try:
            html, final_url = http_client.get_text(url)
            (output_dir / "page.html").write_text(html, encoding="utf-8")
            
            page_text = html_to_text(html)
            (output_dir / "page.txt").write_text(page_text, encoding="utf-8")
            
            doc_links = find_document_links(html, final_url)
            doc_links = prioritize_documents(doc_links)
        except Exception as e:
            logger.error(f"Failed to fetch HTML: {e}")
            doc_links = []
    
    # Limite documents
    doc_links = doc_links[:15]
    
    # 2. Téléchargement parallèle
    downloaded_docs = {}
    
    if doc_links:
        logger.info(f"[{project_id}] Téléchargement parallèle de {len(doc_links)} documents...")
        
        download_args = [
            (idx, http_client, link, cache)
            for idx, link in enumerate(doc_links)
        ]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_DOWNLOAD) as executor:
            futures = {
                executor.submit(download_document_worker, args): args[0]
                for args in download_args
            }
            
            for future in as_completed(futures):
                order, doc_url, blob, status = future.result()
                if blob:
                    downloaded_docs[order] = (doc_url, blob, status)
    
    # 3. Extraction parallèle PDFs
    text_chunks = []
    if page_text:
        text_chunks.append(f"[PAGE_HTML]\n{page_text}")
    
    has_decision = False
    
    if downloaded_docs:
        logger.info(f"[{project_id}] Extraction parallèle de {len(downloaded_docs)} documents...")
        
        extraction_jobs: List[Tuple[str, bytes, str, str]] = []
        ordered_downloads: List[Tuple[Dict, bytes]] = []
        
        for idx, link in enumerate(doc_links):
            payload = downloaded_docs.get(idx)
            if not payload:
                continue
            doc_url, blob, status = payload
            ordered_downloads.append((link, blob))
        
        for idx, (link, blob) in enumerate(ordered_downloads, 1):
            doc_url = link["url"]
            doc_type = link.get("type", "AUTRE")
            ext = (link.get("ext") or "").lower()
            base_slug = slugify(os.path.splitext(os.path.basename(doc_url))[0])
            base_prefix = f"{idx:02d}_{doc_type}_{base_slug}"
            
            if ext == "pdf":
                filename = f"{base_prefix}.pdf"
                save_path = output_dir / filename
                save_path.write_bytes(blob)
                
                extraction_jobs.append((filename, blob, "pdf", doc_type))
                
                if doc_type == "DECISION":
                    has_decision = True
            elif ext == "zip":
                zip_filename = f"{base_prefix}.zip"
                (output_dir / zip_filename).write_bytes(blob)
                
                zip_dir = output_dir / f"{base_prefix}_zip"
                extracted = extract_zip_archive(blob, zip_dir)
                
                if not extracted:
                    logger.warning(f"[{project_id}] ZIP vide ou illisible: {doc_url}")
                    continue
                
                for item in extracted[:80]:
                    file_path = Path(item["path"])
                    file_ext = file_path.suffix.lower()
                    try:
                        file_bytes = file_path.read_bytes()
                    except Exception as err:
                        logger.error(f"[{project_id}] Lecture échouée {file_path}: {err}")
                        continue
                    
                    nested_doc_type = classify_document(item.get("original_name", ""), item.get("original_name", ""))
                    effective_type = nested_doc_type or doc_type
                    nested_name = f"{zip_dir.name}/{file_path.name}"
                    
                    if file_ext in settings.PDF_EXTENSIONS:
                        extraction_jobs.append((nested_name, file_bytes, "pdf", effective_type))
                        if effective_type == "DECISION":
                            has_decision = True
                    else:
                        text_chunks.append(
                            f"[DOC={nested_name}|TYPE={effective_type}|METHOD=unsupported]\n(extension {file_ext} non supportée)"
                        )
            else:
                logger.debug(f"[{project_id}] Extension non gérée pour {doc_url}")
        
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
                text_chunks.append(f"[DOC={filename}|TYPE={doc_type}|METHOD={method}]\n{text or '(vide)'}")
    
    # 4. Analyse Gemini
    metadata = {
        "project_id": project_id,
        "project_title": project_data["project_title"],
        "region": project_data["region"],
        "dept": project_data["dept"],
        "year": project_data["year"]
    }
    
    analysis_result = analyze_with_gemini(text_chunks, metadata)
    
    # 5. Construction Analysis
    return Analysis(
        project_id=project_id,
        project_url=final_url,
        project_title=project_data["project_title"],
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
        n_docs=len(doc_links),
        has_decision_doc="yes" if has_decision else "no",
        analysis_ok="yes" if analysis_result else "no",
        notes=analysis_result.get("notes")
    )


# ============ Main ============

def main():
    parser = argparse.ArgumentParser(
        description="Extraction et analyse de documents BESS (VERSION OPTIMISÉE)"
    )
    
    parser.add_argument(
        "--input",
        required=True,
        help="Fichier CSV de projets (depuis discover.py)"
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
    
    # Vérification Playwright
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("⚠️  Playwright non installé. Installation: pip install playwright && playwright install chromium")
    
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
    
    # Chargement projets
    df = pd.read_csv(input_path, dtype=str).fillna("")
    logger.info(f"Loaded {len(df)} projects from {input_path}")
    
    # Limite optionnelle
    if args.limit:
        df = df.head(args.limit)
        logger.info(f"Limited to {len(df)} projects for testing")
    
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
        for idx, row in df.iterrows():
            try:
                analysis = process_project_optimized(row.to_dict(), client, cache)
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
    print(f"Projets traités:   {len(results)}/{len(df)}")
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
