#!/usr/bin/env python3
"""
CLI d'extraction et d'analyse de documents BESS - VERSION OPTIMISÉE
Usage: python extract.py --input out/projects/aura_2024.csv
"""
import sys
import os
import json
import time
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import google.generativeai as genai
from selectolax.parser import HTMLParser

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

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configuration Gemini
if settings.GOOGLE_API_KEY:
    genai.configure(api_key=settings.GOOGLE_API_KEY)

# Configuration parallélisation
MAX_WORKERS_DOWNLOAD = 5  # Téléchargements parallèles
MAX_WORKERS_EXTRACTION = 3  # Extractions PDF parallèles


# ============ Document Discovery ============

def find_document_links(html: str, base_url: str) -> List[Dict]:
    """Trouve tous les liens PDF/ZIP dans le HTML"""
    import re
    from urllib.parse import urljoin
    
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
    import re
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
        logger.debug(f"Response: {response.text[:500]}")
        return {}
    except Exception as e:
        logger.error(f"Gemini analysis failed: {e}")
        return {}


# ============ Téléchargement Parallèle ============

def download_document_worker(args: Tuple) -> Tuple[str, bytes, str]:
    """Worker pour téléchargement parallèle de documents"""
    http_client, doc_url, cache = args
    
    try:
        # Cache check
        cache_key = cache.get_key(doc_url) if cache else None
        
        if cache and cache_key:
            cached_data = cache.load(cache_key, "pdfs")
            if cached_data:
                logger.debug(f"Cache hit: {doc_url}")
                return doc_url, cached_data, "cached"
        
        # Téléchargement
        blob, final_url = http_client.get_bytes(doc_url)
        
        # Sauvegarde cache
        if cache and cache_key:
            cache.save(cache_key, blob, "pdfs")
        
        return doc_url, blob, "downloaded"
    
    except Exception as e:
        logger.error(f"Download failed {doc_url}: {e}")
        return doc_url, b"", "failed"


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
    cache: SimpleCache
) -> Analysis:
    """Traite un projet avec téléchargements et extractions parallèles"""
    project_id = project_data["project_id"]
    url = project_data["project_url"]
    
    logger.info(f"Processing: {project_data['project_title'][:60]}")
    
    # Dossier sortie
    output_dir = settings.OUTPUT_DIR / "docs" / project_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Page HTML
    html = ""
    final_url = url
    try:
        html, final_url = http_client.get_text(url)
        (output_dir / "page.html").write_text(html, encoding="utf-8")
        
        page_text = html_to_text(html)
        (output_dir / "page.txt").write_text(page_text, encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to fetch HTML: {e}")
        page_text = ""
        html = ""
    
    # 2. Découverte documents
    doc_links = find_document_links(html, final_url)
    doc_links = prioritize_documents(doc_links)
    
    # Limite documents
    doc_links = doc_links[:15]
    
    # 3. Téléchargement parallèle
    downloaded_docs = {}
    
    if doc_links:
        logger.info(f"[{project_id}] Téléchargement parallèle de {len(doc_links)} documents...")
        
        download_args = [
            (http_client, link["url"], cache)
            for link in doc_links
        ]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_DOWNLOAD) as executor:
            futures = {
                executor.submit(download_document_worker, args): args[1]
                for args in download_args
            }
            
            for future in as_completed(futures):
                doc_url, blob, status = future.result()
                if blob:
                    downloaded_docs[doc_url] = blob
    
    # 4. Extraction parallèle PDFs
    text_chunks = [f"[PAGE_HTML]\n{page_text}"]
    has_decision = False
    
    if downloaded_docs:
        logger.info(f"[{project_id}] Extraction parallèle de {len(downloaded_docs)} documents téléchargés...")
        
        # Préparation données extraction
        extraction_jobs: List[Tuple[str, bytes, str, str]] = []
        url_to_link = {link["url"]: link for link in doc_links}
        
        for idx, (doc_url, blob) in enumerate(downloaded_docs.items(), 1):
            link = url_to_link.get(doc_url, {})
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
    
    # 5. Analyse Gemini
    metadata = {
        "project_id": project_id,
        "project_title": project_data["project_title"],
        "region": project_data["region"],
        "dept": project_data["dept"],
        "year": project_data["year"]
    }
    
    analysis_result = analyze_with_gemini(text_chunks, metadata)
    
    # 6. Construction Analysis
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
    import re
    main()
