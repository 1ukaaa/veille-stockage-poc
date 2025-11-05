"""
Fonctions utilitaires partagées - VERSION OPTIMISÉE
Inclus: AdaptiveHTTPClient, PersistentCache, cache PDFs, Document AI
"""
import os
import re
import io
import time
import json
import base64
import hashlib
import zipfile
import mimetypes
import logging
import threading
import random
import pickle
from typing import Tuple, Optional, List, Dict
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime

from google.oauth2 import service_account
from google.auth.transport.requests import Request

import httpx
from selectolax.parser import HTMLParser
from pdfminer.high_level import extract_text as pdfminer_extract_text
import fitz  # PyMuPDF

from config import settings

logger = logging.getLogger(__name__)


class HTTPDownloadTooLarge(Exception):
    """Raised when a download exceeds the configured size limit."""


# ============ HTTP Client STANDARD ============

class HTTPClient:
    """Client HTTP avec rate limiting"""
    
    def __init__(self, delay: float = settings.RATE_LIMIT):
        self.delay = delay
        self.client = httpx.Client(
            headers={"User-Agent": settings.USER_AGENT},
            timeout=settings.TIMEOUT,
            follow_redirects=True
        )
        self._request_count = 0
        self._request_lock = threading.Lock()
        self._lock = threading.Lock()
        self._last_request_end = 0.0
    
    def _respect_rate_limit(self):
        if self.delay <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = self.delay - (now - self._last_request_end)
        if wait > 0:
            time.sleep(wait)

    def _record_request(self):
        with self._lock:
            self._request_count += 1
            self._last_request_end = time.monotonic()

    def get_bytes(self, url: str, max_bytes: Optional[int] = None) -> Tuple[bytes, str]:
        """Télécharge un contenu binaire"""
        with self._request_lock:
            self._respect_rate_limit()
            logger.info(f"GET {url}")
            try:
                with self.client.stream("GET", url) as response:
                    response.raise_for_status()
                    data = bytearray()
                    limit = max_bytes or 0
                    for chunk in response.iter_bytes():
                        data.extend(chunk)
                        if limit and len(data) > limit:
                            raise HTTPDownloadTooLarge(f"Download exceeds limit ({limit} bytes)")
                    final_url = str(response.url)
            finally:
                self._record_request()
            return bytes(data), final_url
    
    def get_text(self, url: str) -> Tuple[str, str]:
        """Télécharge un contenu texte"""
        with self._request_lock:
            self._respect_rate_limit()
            logger.info(f"GET {url}")
            try:
                response = self.client.get(url)
                response.raise_for_status()
                text = response.text
                final_url = str(response.url)
            finally:
                self._record_request()
            return text, final_url

    def head(self, url: str) -> httpx.Response:
        """Effectue une requête HEAD (pour métadonnées)"""
        with self._request_lock:
            self._respect_rate_limit()
            logger.debug(f"HEAD {url}")
            try:
                response = self.client.head(url)
                response.raise_for_status()
            finally:
                self._record_request()
            return response
    
    def close(self):
        """Ferme proprement le client"""
        self.client.close()
        logger.info(f"HTTP client closed ({self._request_count} requests)")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# ============ HTTP Client ADAPTATIF - 🚀 NOUVEAU ============

class AdaptiveHTTPClient(HTTPClient):
    """Client HTTP avec réduction intelligente des délais et backoff exponentiel
    
    Stratégie:
    - Démarre avec base_delay faible (0.15s au lieu de 0.4s)
    - Succès → délai réduit progressivement (-2% par requête)
    - Rate limit (429) → délai augmenté exponentiellement (backoff)
    - User-Agent rotation pour éviter blocages
    
    Utilisation:
        # À la place de HTTPClient(0.4)
        client = AdaptiveHTTPClient(base_delay=0.15, max_delay=0.5)
    """
    
    def __init__(self, base_delay: float = 0.15, max_delay: float = 0.5):
        super().__init__(base_delay)
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.consecutive_errors = 0
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        ]
    
    def _rotate_user_agent(self):
        """Rotation User-Agent pour éviter fingerprinting"""
        ua = random.choice(self.user_agents)
        self.client.headers["User-Agent"] = ua
    
    def _respect_rate_limit(self):
        """Override: Ajoute jitter au délai"""
        if self.delay <= 0:
            return
        
        # Jitter: variation aléatoire 85-115%
        jitter = random.uniform(0.85, 1.15) * self.delay
        
        with self._lock:
            now = time.monotonic()
            wait = jitter - (now - self._last_request_end)
        
        if wait > 0:
            time.sleep(wait)
    
    def get_text(self, url: str, max_retries: int = 2) -> Tuple[str, str]:
        """Override avec gestion d'erreur adaptative"""
        
        for attempt in range(max_retries):
            try:
                self._rotate_user_agent()
                
                # Appel parent
                with self._request_lock:
                    self._respect_rate_limit()
                    logger.info(f"GET {url}")
                    response = self.client.get(url)
                    response.raise_for_status()
                    text = response.text
                    final_url = str(response.url)
                    self._record_request()
                
                # Succès : réduire délai progressivement
                self.consecutive_errors = 0
                self.delay = max(0.1, self.delay * 0.98)
                
                return text, final_url
            
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:  # Rate limited
                    self.consecutive_errors += 1
                    wait_time = min(self.max_delay, self.base_delay * (2 ** self.consecutive_errors))
                    logger.warning(
                        f"Rate limited (429) - Attendre {wait_time:.2f}s "
                        f"(tentative {attempt+1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                elif e.response.status_code in (500, 502, 503, 504):
                    # Erreurs serveur - retry
                    if attempt < max_retries - 1:
                        logger.warning(f"Erreur serveur {e.response.status_code} - retry...")
                        time.sleep(1)
                    else:
                        raise
                else:
                    raise
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.debug(f"Retry {attempt+1}/{max_retries}: {e}")
                time.sleep(0.5)
        
        raise RuntimeError("Impossible d'accéder à l'URL après retries")


# ============ Texte ============

def slugify(text: str, max_length: int = 80) -> str:
    """Convertit une chaîne en slug valide"""
    text = re.sub(r"[^\w\-.]+", "-", text.strip(), flags=re.I)
    text = re.sub(r"-+", "-", text).strip("-")
    return (text[:max_length] or "file").lower()


def html_to_text(html: str) -> str:
    """Convertit HTML en texte brut"""
    tree = HTMLParser(html)
    for node in tree.css("script, style"):
        node.decompose()
    return tree.text(separator=" ").strip()


def is_poor_text(text: str) -> bool:
    """Détermine si un texte extrait est de mauvaise qualité"""
    text = (text or "").strip()
    
    if not text or len(text) < settings.POOR_TEXT_MIN_CHARS:
        return True
    
    whitespace_count = sum(1 for c in text if c.isspace())
    whitespace_ratio = whitespace_count / max(1, len(text))
    
    return whitespace_ratio > settings.POOR_TEXT_WHITESPACE_RATIO


# ============ Extraction PDF ============

def extract_text_pdfminer(pdf_bytes: bytes) -> str:
    """Extraction PDF avec pdfminer.six"""
    try:
        return pdfminer_extract_text(io.BytesIO(pdf_bytes)) or ""
    except Exception as e:
        logger.warning(f"pdfminer extraction failed: {e}")
        return ""


def extract_text_pymupdf(pdf_bytes: bytes) -> str:
    """Extraction PDF avec PyMuPDF"""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        texts = [page.get_text("text") or "" for page in doc]
        doc.close()
        return "\n".join(texts)
    except Exception as e:
        logger.warning(f"PyMuPDF extraction failed: {e}")
        return ""


def extract_pdf_robust(pdf_bytes: bytes) -> Tuple[str, str]:
    """Extraction PDF robuste - Returns: (text, method_used)"""
    text = extract_text_pdfminer(pdf_bytes)
    if not is_poor_text(text):
        return text, "pdfminer"
    
    text = extract_text_pymupdf(pdf_bytes)
    if not is_poor_text(text):
        return text, "pymupdf"
    
    if settings.DOCAI_PROCESS_URL:
        text = docai_extract_text(pdf_bytes, mime_type="application/pdf")
        if text.strip():
            return text, "docai"
    
    return "", "failed"

# ============ Extraction ZIP ============

def extract_zip_archive(zip_bytes: bytes, output_dir: Path) -> List[dict]:
    """
    Extrait une archive ZIP et retourne la liste des fichiers
    Returns: List[{filename, path, ext}]
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    extracted_files = []
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist()[:400]:  # Limite sécurité
            if name.endswith("/"):
                continue
            
            original_basename = os.path.basename(name)
            stem, ext = os.path.splitext(original_basename)
            slug = slugify(stem) or "file"
            hash_suffix = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
            safe_name = f"{slug}_{hash_suffix}{ext.lower()}"
            dest_path = output_dir / safe_name
            
            try:
                with zf.open(name) as src:
                    data = src.read()
                with open(dest_path, "wb") as dst:
                    dst.write(data)
                
                ext = dest_path.suffix.lower()
                extracted_files.append({
                    "filename": safe_name,
                    "path": str(dest_path),
                    "ext": ext,
                    "original_name": name
                })
            except Exception as e:
                logger.warning(f"ZIP extraction failed for {name}: {e}")
                continue
    
    return extracted_files



# ============ Cache Simple ============

class SimpleCache:
    """Cache simple basé sur fichiers"""
    
    def __init__(self, cache_dir: Path = settings.CACHE_DIR):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_key(self, url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()
    
    def exists(self, key: str, category: str = "pdfs") -> bool:
        cache_path = self.cache_dir / category / f"{key}.cache"
        return cache_path.exists()
    
    def save(self, key: str, data: bytes, category: str = "pdfs"):
        cache_dir = self.cache_dir / category
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"{key}.cache"
        
        with open(cache_path, "wb") as f:
            f.write(data)
    
    def load(self, key: str, category: str = "pdfs") -> Optional[bytes]:
        cache_path = self.cache_dir / category / f"{key}.cache"
        
        if not cache_path.exists():
            return None
        
        with open(cache_path, "rb") as f:
            return f.read()


# ============ Cache Persistant - 🚀 NOUVEAU ============

class PersistentCache:
    """Cache persistant sur disque (évite re-scraping entre exécutions)
    
    Utilisation:
        cache = PersistentCache()
        if url in cache.cache:
            html, url = cache.cache[url]
        else:
            html, url = client.get_text(url)
            cache.set(url, (html, url))
        cache.save()  # À la fin
    """
    
    def __init__(self, cache_file: Path = None):
        if cache_file is None:
            cache_file = settings.CACHE_DIR / "scraper_cache.pkl"
        
        self.cache_file = cache_file
        self.cache: Dict[str, Tuple[str, str]] = self._load()
    
    def _load(self) -> dict:
        """Charge cache depuis disque"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'rb') as f:
                    cache = pickle.load(f)
                logger.info(f"✓ Cache loaded: {len(cache)} URLs")
                return cache
            except Exception as e:
                logger.warning(f"Cache load failed: {e}")
        return {}
    
    def save(self):
        """Sauvegarde cache sur disque"""
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.cache, f)
            logger.info(f"✓ Cache saved: {len(self.cache)} URLs")
        except Exception as e:
            logger.error(f"Cache save failed: {e}")
    
    def get(self, key: str) -> Optional[Tuple[str, str]]:
        return self.cache.get(key)
    
    def set(self, key: str, value: Tuple[str, str]):
        self.cache[key] = value
    
    def clear(self):
        self.cache = {}
        if self.cache_file.exists():
            self.cache_file.unlink()


# ============ Document AI (inchangé) ============

class ServiceAccountTokenManager:
    """Gestion tokens Service Account"""
    
    def __init__(self):
        if not settings.SERVICEACCOUNTKEY.exists():
            raise FileNotFoundError(
                f"Fichier clé manquant: {settings.SERVICEACCOUNTKEY}"
            )
        
        self.credentials = service_account.Credentials.from_service_account_file(
            str(settings.SERVICEACCOUNTKEY),
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )
        self._access_token = None
        self._expiry_timestamp = 0
    
    def get_access_token(self) -> str:
        now = time.time()
        
        if self._access_token and self._expiry_timestamp - now > 60:
            return self._access_token
        
        self.credentials.refresh(Request())
        self._access_token = self.credentials.token
        self._expiry_timestamp = now + (self.credentials.expiry - datetime.now()).total_seconds()
        
        logger.info(f"Service Account token généré")
        return self._access_token


token_manager = ServiceAccountTokenManager()


def docai_extract_text(file_bytes: bytes, mime_type: str = "application/pdf") -> str:
    """Extraction via Google Document AI"""
    if not settings.DOCAI_PROCESS_URL:
        logger.warning("Document AI not configured")
        return ""
    
    try:
        token = token_manager.get_access_token()
    except Exception as e:
        logger.error(f"OAuth2 token error: {e}")
        return ""
    
    body = {
        "rawDocument": {
            "content": base64.b64encode(file_bytes).decode("ascii"),
            "mimeType": mime_type,
        },
        "skipHumanReview": True
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        with httpx.Client(timeout=settings.TIMEOUT) as client:
            response = client.post(settings.DOCAI_PROCESS_URL, headers=headers, json=body)
            
            if response.status_code >= 400:
                logger.error(f"Document AI HTTP {response.status_code}")
                return ""
            
            data = response.json()
    except Exception as e:
        logger.error(f"Document AI request failed: {e}")
        return ""
    
    document = data.get("document", {})
    return document.get("text", "")

# AJOUTER À LA FIN DE utils.py

def quick_pdf_scan(pdf_bytes: bytes, commune: str, max_pages: int = 2) -> bool:
    """
    Scan rapide des premières pages pour vérifier pertinence
    Évite extraction complète de PDFs de 100+ pages non pertinents
    """
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        quick_text = ""
        for page_num in range(min(max_pages, doc.page_count)):
            quick_text += doc[page_num].get_text("text")
        
        doc.close()
        
        # Normaliser et chercher commune
        from validation import normalize_commune, get_commune_variations
        commune_variants = get_commune_variations(commune)
        quick_text_lower = quick_text.lower()
        
        if any(var in quick_text_lower for var in commune_variants):
            logger.debug(f"✓ Quick scan: commune trouvée dans {max_pages} premières pages")
            return True
        
        logger.warning(f"⚠️  Quick scan: commune '{commune}' absente des {max_pages} premières pages")
        return False
        
    except Exception as e:
        logger.debug(f"Quick scan failed: {e}")
        return True  # En cas d'erreur, continuer extraction complète


class PDFMetadataCache:
    """Cache des métadonnées PDFs (taille, Content-Type) pour éviter HEAD répétés"""
    
    def __init__(self, cache_dir: Path):
        self.cache = SimpleCache(cache_dir)
    
    def get_pdf_size(self, url: str, http_client: 'HTTPClient') -> Optional[float]:
        """Retourne taille en MB depuis cache ou HEAD request"""
        cache_key = self.cache.get_key(url)
        
        # Chercher en cache
        cached = self.cache.load(cache_key, category="pdf_metadata")
        if cached:
            try:
                metadata = json.loads(cached.decode('utf-8'))
                logger.debug(f"📦 Cache hit: {url[:80]}")
                return metadata.get("size_mb")
            except:
                pass
        
        # HEAD request
        try:
            response = http_client.head(url)
            content_length = response.headers.get("Content-Length")
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                
                # Sauvegarder en cache
                self.cache.save(
                    cache_key,
                    json.dumps({"size_mb": size_mb, "timestamp": time.time()}).encode('utf-8'),
                    category="pdf_metadata"
                )
                return size_mb
        except Exception as e:
            logger.debug(f"HEAD failed: {e}")
        
        return None

# ============ QUICK PDF SCAN (NOUVEAU) ============

def quick_pdf_scan(pdf_bytes: bytes, commune: str, max_pages: int = 2) -> bool:
    """
    Scan rapide des premières pages pour vérifier pertinence
    Évite extraction complète de PDFs de 100+ pages non pertinents
    
    Args:
        pdf_bytes: Contenu du PDF en bytes
        commune: Nom de la commune cible
        max_pages: Nombre de pages à scanner (défaut 2)
    
    Returns:
        True si commune trouvée, False sinon
    """
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        quick_text = ""
        for page_num in range(min(max_pages, doc.page_count)):
            quick_text += doc[page_num].get_text("text")
        
        doc.close()
        
        # Normaliser et chercher commune
        from validation import normalize_commune, get_commune_variations
        commune_variants = get_commune_variations(commune)
        quick_text_lower = quick_text.lower()
        
        if any(var in quick_text_lower for var in commune_variants):
            logger.debug(f"    ✓ Quick scan: commune trouvée dans {max_pages} premières pages")
            return True
        
        logger.debug(f"    ⏭️  Quick scan: commune '{commune}' absente des {max_pages} premières pages")
        return False
        
    except Exception as e:
        logger.debug(f"    Quick scan failed: {e}")
        return True  # En cas d'erreur, continuer extraction complète


# ============ PDF METADATA CACHE (NOUVEAU) ============

class PDFMetadataCache:
    """Cache des métadonnées PDFs (taille, Content-Type) pour éviter HEAD répétés"""
    
    def __init__(self, cache_dir: Path):
        self.cache = SimpleCache(cache_dir)
    
    def get_pdf_size(self, url: str, http_client: 'HTTPClient') -> Optional[float]:
        """Retourne taille en MB depuis cache ou HEAD request"""
        cache_key = self.cache.get_key(url)
        
        # Chercher en cache
        cached = self.cache.load(cache_key, category="pdf_metadata")
        if cached:
            try:
                metadata = json.loads(cached.decode('utf-8'))
                logger.debug(f"    📦 Cache hit pour {url[:60]}")
                return metadata.get("size_mb")
            except:
                pass
        
        # HEAD request
        try:
            response = http_client.head(url)
            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    size_mb = int(content_length) / (1024 * 1024)
                    
                    # Sauvegarder en cache
                    self.cache.save(
                        cache_key,
                        json.dumps({"size_mb": size_mb, "timestamp": time.time()}).encode('utf-8'),
                        category="pdf_metadata"
                    )
                    return size_mb
                except ValueError:
                    pass
        except Exception as e:
            logger.debug(f"    HEAD failed: {e}")
        
        return None
