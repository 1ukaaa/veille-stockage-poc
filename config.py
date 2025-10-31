"""
Configuration centralisée du projet
"""
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Charger variables d'environnement
load_dotenv()


@dataclass
class Settings:
    """Configuration globale de l'application"""
    
    # Chemins
    BASE_DIR: Path = Path(__file__).parent
    OUTPUT_DIR: Path = BASE_DIR / "out"
    CACHE_DIR: Path = BASE_DIR / "cache"
    
    # API Keys
    GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "")
    OAUTH_CLIENT_ID: str = os.getenv("OAUTH_CLIENT_ID", "")
    OAUTH_CLIENT_SECRET: str = os.getenv("OAUTH_CLIENT_SECRET", "")
    OAUTH_REFRESH_TOKEN: str = os.getenv("OAUTH_REFRESH_TOKEN", "")
    DOCAI_PROCESS_URL: str = os.getenv("DOCAI_PROCESS_URL", "")
    OAUTH_TOKEN_URL: str = "https://oauth2.googleapis.com/token"
    
    # HTTP
    USER_AGENT: str = "Mozilla/5.0 (compatible; bess-scraper/1.0)"
    TIMEOUT: float = 45.0
    RATE_LIMIT: float = 0.8  # secondes entre requêtes
    
    # Extraction
    MAX_GEMINI_CHARS: int = 160_000
    MAX_PER_DOC_CHARS: int = 20_000
    POOR_TEXT_MIN_CHARS: int = 300
    POOR_TEXT_WHITESPACE_RATIO: float = 0.6
    
    # Gemini
    GEMINI_MODEL: str = "gemini-2.5-flash"
    GEMINI_TEMPERATURE: float = 0.1
    
    # Extensions supportées
    PDF_EXTENSIONS: set = None
    IMAGE_EXTENSIONS: set = None
    
    def __post_init__(self):
        if self.PDF_EXTENSIONS is None:
            self.PDF_EXTENSIONS = {".pdf"}
        if self.IMAGE_EXTENSIONS is None:
            self.IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}
        
        # Créer les dossiers s'ils n'existent pas
        self.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        (self.OUTPUT_DIR / "projects").mkdir(exist_ok=True)
        (self.OUTPUT_DIR / "analyzed").mkdir(exist_ok=True)
        (self.OUTPUT_DIR / "enriched").mkdir(exist_ok=True)

    # NOUVEAUX PARAMÈTRES VALIDATION
    URL_SCORE_THRESHOLD: float = 0.25
    MIN_COMMUNE_MENTIONS: int = 1
    QUICK_SCAN_ENABLED: bool = True

    # Page type classification
    ENABLE_PAGE_CLASSIFICATION: bool = True
    
    # Validation stricte PC vs cas par cas
    REJECT_CAS_PAR_CAS_AS_PC: bool = True
    VALIDATE_PC_FORMAT: bool = True

# Instance globale
settings = Settings()
