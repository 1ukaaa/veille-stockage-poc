"""
Fonctions utilitaires partagées
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
from typing import Tuple, Optional, List
from pathlib import Path
from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser
from pdfminer.high_level import extract_text as pdfminer_extract_text
import fitz  # PyMuPDF

from config import settings

logger = logging.getLogger(__name__)


# ============ HTTP Client ============

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
    
    def get_bytes(self, url: str) -> Tuple[bytes, str]:
        """Télécharge un contenu binaire"""
        logger.info(f"GET {url}")
        response = self.client.get(url)
        response.raise_for_status()
        self._request_count += 1
        time.sleep(self.delay)
        return response.content, str(response.url)
    
    def get_text(self, url: str) -> Tuple[str, str]:
        """Télécharge un contenu texte"""
        logger.info(f"GET {url}")
        response = self.client.get(url)
        response.raise_for_status()
        self._request_count += 1
        time.sleep(self.delay)
        return response.text, str(response.url)
    
    def close(self):
        """Ferme proprement le client"""
        self.client.close()
        logger.info(f"HTTP client closed ({self._request_count} requests)")
    
    # AJOUT : Context manager methods
    def __enter__(self):
        """Entrée du context manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sortie du context manager"""
        self.close()
        return False

# ============ Texte ============

def slugify(text: str, max_length: int = 80) -> str:
    """Convertit une chaîne en slug valide pour nom de fichier"""
    text = re.sub(r"[^\w\-\.]+", "-", text.strip(), flags=re.I)
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
    """Extraction PDF avec PyMuPDF (fallback)"""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        texts = [page.get_text("text") or "" for page in doc]
        doc.close()
        return "\n".join(texts)
    except Exception as e:
        logger.warning(f"PyMuPDF extraction failed: {e}")
        return ""


def extract_pdf_robust(pdf_bytes: bytes) -> Tuple[str, str]:
    """
    Extraction PDF robuste avec cascade de méthodes
    Returns: (text, method_used)
    """
    # Tentative 1: pdfminer
    text = extract_text_pdfminer(pdf_bytes)
    if not is_poor_text(text):
        return text, "pdfminer"
    
    # Tentative 2: PyMuPDF
    text = extract_text_pymupdf(pdf_bytes)
    if not is_poor_text(text):
        return text, "pymupdf"
    
    # Tentative 3: Document AI (si configuré)
    if settings.DOCAI_PROCESS_URL:
        text = docai_extract_text(pdf_bytes, mime_type="application/pdf")
        if text.strip():
            return text, "docai"
    
    return "", "failed"


# ============ Document AI ============

class OAuth2TokenManager:
    """Gestion tokens OAuth2 avec cache"""
    
    def __init__(self):
        self._access_token: Optional[str] = None
        self._expiry_timestamp: float = 0
    
    def get_access_token(self) -> str:
        """Obtient un token valide"""
        now = time.time()
        
        if self._access_token and (self._expiry_timestamp - now > 60):
            return self._access_token
        
        data = {
            "client_id": settings.OAUTH_CLIENT_ID,
            "client_secret": settings.OAUTH_CLIENT_SECRET,
            "refresh_token": settings.OAUTH_REFRESH_TOKEN,
            "grant_type": "refresh_token",
        }
        
        with httpx.Client(timeout=settings.TIMEOUT) as client:
            response = client.post(settings.OAUTH_TOKEN_URL, data=data)
            response.raise_for_status()
            token_data = response.json()
        
        self._access_token = token_data.get("access_token", "")
        expires_in = token_data.get("expires_in", 3000)
        self._expiry_timestamp = now + int(expires_in)
        
        logger.info("OAuth2 token refreshed")
        return self._access_token


token_manager = OAuth2TokenManager()


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
            
            safe_name = slugify(os.path.basename(name)) or "file"
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


# ============ Cache ============

class SimpleCache:
    """Cache simple basé sur fichiers"""
    
    def __init__(self, cache_dir: Path = settings.CACHE_DIR):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_key(self, url: str) -> str:
        """Génère une clé de cache depuis une URL"""
        return hashlib.sha256(url.encode()).hexdigest()
    
    def exists(self, key: str, category: str = "pdfs") -> bool:
        """Vérifie si un fichier est en cache"""
        cache_path = self.cache_dir / category / f"{key}.cache"
        return cache_path.exists()
    
    def save(self, key: str, data: bytes, category: str = "pdfs"):
        """Sauvegarde en cache"""
        cache_dir = self.cache_dir / category
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path = cache_dir / f"{key}.cache"
        
        with open(cache_path, "wb") as f:
            f.write(data)
    
    def load(self, key: str, category: str = "pdfs") -> Optional[bytes]:
        """Charge depuis le cache"""
        cache_path = self.cache_dir / category / f"{key}.cache"
        
        if not cache_path.exists():
            return None
        
        with open(cache_path, "rb") as f:
            return f.read()

# ============ Cache Persistant (Optionnel) ============

import pickle
from pathlib import Path as PathLib

class PersistentCache:
    """Cache persistant sur disque (évite re-scraping entre exécutions)"""
    
    def __init__(self, cache_file: PathLib = settings.CACHE_DIR / "scraper_cache.pkl"):
        self.cache_file = cache_file
        self.cache = self._load()
    
    def _load(self) -> dict:
        """Charge cache depuis disque"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'rb') as f:
                    cache = pickle.load(f)
                logger.info(f"Cache loaded: {len(cache)} entries")
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
            logger.info(f"Cache saved: {len(self.cache)} entries")
        except Exception as e:
            logger.error(f"Cache save failed: {e}")
    
    def get(self, key: str) -> Optional[tuple]:
        """Récupère depuis cache"""
        return self.cache.get(key)
    
    def set(self, key: str, value: tuple):
        """Ajoute au cache"""
        self.cache[key] = value
    
    def clear(self):
        """Vide le cache"""
        self.cache = {}
        if self.cache_file.exists():
            self.cache_file.unlink()
