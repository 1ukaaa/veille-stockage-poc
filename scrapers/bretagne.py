#!/usr/bin/env python3
"""
🗺️ Scraper DREAL Bretagne - API WFS + Filtrage BESS OPTIMISÉ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Logique de filtrage BESS:
✅ INCLURE:
   - "batterie" (toutes variantes)
   - "stockage d'électricité"
   - "stockage d'énergie" (sauf si combiné avec agricole/froid)
   - "centrale de stockage"
   - "installation de stockage d'énergie"

❌ EXCLURE ABSOLUMENT:
   - Production: hydrogène, houlomoteur, éolien, solaire, photovoltaïque, bioénergie
   - Secteur: agricole, froid, logistique, hangar
   - Matière: bois, pellets, peroxydes, combustibles
   - Véhicule/transport: bateaux, véhicules, pétrole, gaz

Résultat: ~5-6 vrais projets BESS (batterie électrique)
"""

import re
import hashlib
import logging
from typing import List, Optional, Dict

from models import Project

logger = logging.getLogger(__name__)

# ============ Configuration ============

BASE_URL = "https://geobretagne.fr/geoserver"
WORKSPACE = "dreal_b"
LAYER = "ae_casparcas"
WFS_ENDPOINT = f"{BASE_URL}/{WORKSPACE}/wfs"

# Keywords POSITIFS (doivent être présents)
STORAGE_KEYWORDS = [
    "batterie",
    "battery",
    "stockage d'électricité",
    "stockage d'énergie",
    "centrale de stockage",
    "installation de stockage d'énergie",
    "parc de stockage",
]

# Keywords À EXCLURE (invalident le projet)
EXCLUDE_KEYWORDS = [
    # Production d'énergie (pas du stockage)
    "hydrogène", "hydrogen", "houlomoteur", "houle",
    "éolien", "solaire", "photovoltaïque", "pv",
    "bioénergie", "biogaz", "biomasse", "géothermie",
    "production d'électricité", "turbine", "panneau",
    # Secteur
    "agricole", "silo", "lisier", "stabulation", "fumier", "fosse",
    "froid", "chambre froide", "congélation", "freezer",
    "entrepôt", "logistique", "plateforme", "magasin", "hangar", "warehousing",
    # Matière/énergie fossile
    "bois", "pellets", "emballages", "peroxydes", "combustibles",
    "pétrole", "gaz", "essence", "fioul",
    # Transport
    "bateaux", "véhicules", "cars", "train", "autobus",
    "fruits", "légumes", "alcools", "eau potable",
    "mobilhomes", "self-storage", "parking",
]

# ============ Utils ============

def _sha1(text: str) -> str:
    """Hash SHA1 pour project_id"""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _is_bess_project(titre: Optional[str]) -> bool:
    """
    Filtre BESS OPTIMISÉ : Accepte tous les stockage d'énergie
    SAUF les exclusions absolues.
    
    Logique:
    1. Exclut ABSOLUMENT si contient keyword exclu
    2. Inclut si contient keyword de stockage
    3. Cas spécial "poste électrique": valide que si + batterie/stockage
    """
    if not titre or not isinstance(titre, str):
        return False
    
    titre_lower = titre.lower()
    
    # ÉTAPE 1: Exclusions absolues (non-négociables)
    if any(kw in titre_lower for kw in EXCLUDE_KEYWORDS):
        logger.debug(f"[BESS-EXCLUDE] Mot-clé exclu: {titre[:60]}")
        return False
    
    # ÉTAPE 2: Cherche keywords de stockage
    has_storage = any(kw in titre_lower for kw in STORAGE_KEYWORDS)
    
    if not has_storage:
        logger.debug(f"[BESS-REJECT] Pas de keyword stockage: {titre[:60]}")
        return False
    
    # ÉTAPE 3: Cas spécial "poste électrique" seul
    # Si titre SEULEMENT "poste électrique" sans mention de batterie/stockage...
    # → mais on a déjà checké STORAGE_KEYWORDS, donc c'est ok
    
    logger.debug(f"[BESS-OK] ✅ Validé: {titre[:60]}")
    return True


def _extract_year_from_date(date_str: Optional[str]) -> str:
    """Extrait l'année depuis date ISO"""
    if not date_str:
        return "2025"
    
    match = re.search(r"(\d{4})", str(date_str))
    return match.group(1) if match else "2025"


def _extract_dept_from_title(titre: Optional[str]) -> str:
    """Extrait département depuis titre"""
    if not titre:
        return "?"
    
    match = re.search(r"\((\d{2})\)", titre)
    return match.group(1) if match else "?"


# ============ Collecte WFS ============

def _fetch_all_features(http_client) -> List[Dict]:
    """Récupère TOUS les examens au cas par cas Bretagne via WFS."""
    logger.info("Récupération de tous les examens au cas par cas Bretagne...")
    
    import httpx
    
    params = {
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "VERSION": "2.0.0",
        "typeName": f"{WORKSPACE}:{LAYER}",
        "outputFormat": "application/json"
    }
    
    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(WFS_ENDPOINT, params=params)
            resp.raise_for_status()
            
            data = resp.json()
            features = data.get("features", [])
            total = data.get("numberMatched", len(features))
            
            logger.info(f"✅ Récupéré {len(features)} features sur {total}")
            return features
    
    except Exception as e:
        logger.error(f"Erreur WFS GetFeature: {e}", exc_info=True)
        return []


def _filter_bess_projects(features: List[Dict]) -> List[Dict]:
    """Filtre les features BESS."""
    bess_projects = []
    excluded_count = 0
    
    for feat in features:
        props = feat.get("properties", {})
        titre = props.get("titre")
        
        if not titre or not isinstance(titre, str):
            continue
        
        # Filtre BESS optimisé
        if not _is_bess_project(titre):
            excluded_count += 1
            continue
        
        # Valide URLs
        url_decision = props.get("lien_arrete")
        url_formulaire = props.get("lien_formulaire")
        url_dossier = props.get("lien_dossier")
        
        if not (url_decision or url_formulaire or url_dossier):
            logger.warning(f"Pas d'URL pour: {titre[:60]}")
            continue
        
        project_data = {
            "id_cerfa": props.get("id_cerfa", ""),
            "titre": titre,
            "url_decision": url_decision,
            "url_formulaire": url_formulaire,
            "url_dossier": url_dossier,
            "date_arrete": props.get("date_arrete"),
            "year": _extract_year_from_date(props.get("date_arrete")),
            "dept": _extract_dept_from_title(titre),
        }
        
        bess_projects.append(project_data)
    
    logger.info(f"✅ {len(bess_projects)} projets BESS filtrés (exclus: {excluded_count})")
    return bess_projects


def _build_project_object(project_data: Dict, region: str = "bretagne") -> Optional[Project]:
    """Crée objet Project."""
    
    main_url = (
        project_data.get("url_decision") or
        project_data.get("url_formulaire") or
        project_data.get("url_dossier")
    )
    
    if not main_url:
        logger.warning(f"Pas d'URL principal: {project_data.get('titre', 'unknown')[:60]}")
        return None
    
    return Project(
        project_id=_sha1(project_data.get("id_cerfa", project_data["titre"])),
        region=region,
        dept=project_data["dept"],
        year=project_data["year"],
        project_title=project_data["titre"][:200],
        project_url=main_url,
        url_cerfa=project_data.get("url_formulaire"),
        url_decision=project_data.get("url_decision")
    )


# ============ API Principale ============

def discover_projects(
    year: Optional[str] = None,
    client=None,
    dept: Optional[str] = None,
    seed_url: Optional[str] = None
) -> List[Project]:
    """API principale Bretagne."""
    
    logger.info(
        f"\n{'='*70}\n"
        f"SCRAPER BRETAGNE (WFS + Filtrage BESS OPTIMISÉ)\n"
        f"Année: {year or 'TOUS'}\n"
        f"Département: {dept or 'TOUS'}\n"
        f"Source: GéoBretagne WFS (3903 examens au cas par cas)\n"
        f"Filtrage: Stockage d'énergie/batterie (exclut production renewables)\n"
        f"Résultat attendu: ~5-6 vrais projets BESS\n"
        f"{'='*70}\n"
    )
    
    try:
        features = _fetch_all_features(client)
        
        if not features:
            logger.error("Aucune donnée WFS récupérée")
            return []
        
        bess_data = _filter_bess_projects(features)
        
        if not bess_data:
            logger.warning("Aucun projet BESS trouvé après filtrage")
            return []
        
        filtered_data = bess_data
        
        if year:
            filtered_data = [p for p in filtered_data if p["year"] == year]
            logger.info(f"Après filtre année {year}: {len(filtered_data)} projets")
        
        if dept:
            code = dept.zfill(2)
            filtered_data = [p for p in filtered_data if p["dept"] == code]
            logger.info(f"Après filtre département {code}: {len(filtered_data)} projets")
        
        projects = []
        for data in filtered_data:
            proj = _build_project_object(data)
            if proj:
                projects.append(proj)
        
        logger.info(
            f"\n{'='*70}\n"
            f"RÉSUMÉ BRETAGNE\n"
            f"Total projets BESS: {len(projects)}\n"
            f"{'='*70}\n"
        )
        
        return projects
    
    except Exception as e:
        logger.error(f"Erreur découverte: {e}", exc_info=True)
        return []
