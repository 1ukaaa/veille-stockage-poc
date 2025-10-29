"""
Modèles de données du projet
"""
from dataclasses import dataclass, field, asdict
from typing import Optional, List
from datetime import datetime


@dataclass
class Project:
    """Projet BESS découvert"""
    project_id: str
    region: str
    dept: str
    year: str
    project_title: str
    project_url: str
    discovered_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self):
        return asdict(self)


@dataclass
class Document:
    """Document associé à un projet"""
    url: str
    label: str
    doc_type: str  # DECISION, CERFA, AUTRE
    ext: str  # pdf, zip
    filename: Optional[str] = None
    text_extracted: Optional[str] = None
    extraction_method: Optional[str] = None  # pdfminer, pymupdf, docai


@dataclass
class Analysis:
    """Résultat d'analyse d'un projet"""
    project_id: str
    project_url: str
    project_title: str
    dept: str
    region: str
    year: str
    
    # Données techniques
    puissance_MW: Optional[float] = None
    duree_h: Optional[float] = None
    energie_MWh: Optional[float] = None
    technologie: Optional[str] = None
    
    # Acteurs
    demandeur: Optional[str] = None
    commune: Optional[str] = None
    
    # Procédures
    cas_par_cas_decision: Optional[str] = None  # soumis_EI, non_soumis_EI
    cas_par_cas_date: Optional[str] = None
    decision_detail: Optional[str] = None
    
    # Métadonnées
    n_docs: int = 0
    has_decision_doc: str = "no"
    analysis_ok: str = "no"
    errors: Optional[str] = None
    notes: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)


@dataclass
class BuildingPermit:
    """Permis de construire recherché"""
    project_id: str
    permit_number: Optional[str] = None
    issue_date: Optional[str] = None
    applicant: Optional[str] = None
    source_url: Optional[str] = None
    confidence: float = 0.0
    search_summary: Optional[str] = None
    validated: bool = False
    
    def to_dict(self):
        return asdict(self)
