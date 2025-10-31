#!/usr/bin/env python3
"""
Système de validation et scoring pour l'enrichissement des permis
Version 1.0 - Octobre 2025

Fonctionnalités:
- Scoring multi-critères d'URLs (0.0 à 1.0)
- Validation contenu PDF après extraction
- Détection de faux positifs (autre commune)
- Fuzzy matching de noms d'entreprises
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# ============ NORMALISATION ============

def normalize_commune(commune: str) -> str:
    """Normalise une commune pour comparaisons flexibles"""
    normalized = commune.lower()
    normalized = normalized.replace('-', '').replace(' ', '')
    normalized = re.sub(r'[àâä]', 'a', normalized)
    normalized = re.sub(r'[éèêë]', 'e', normalized)
    normalized = re.sub(r'[îï]', 'i', normalized)
    normalized = re.sub(r'[ôö]', 'o', normalized)
    normalized = re.sub(r'[ùûü]', 'u', normalized)
    return normalized


def get_commune_variations(commune: str) -> List[str]:
    """Génère variations possibles d'un nom de commune"""
    variations = [
        commune.lower(),
        normalize_commune(commune),
        commune.lower().replace('-', ' '),
        commune.lower().replace(' ', '-')
    ]
    
    # Variations Saint -> St
    if commune.lower().startswith('saint'):
        variations.append(commune.lower().replace('saint', 'st', 1))
        variations.append(commune.lower().replace('saint-', 'st-', 1))
    
    return list(set(variations))


def clean_company_name(company: str) -> str:
    """Nettoie nom d'entreprise pour comparaisons"""
    cleaned = company.lower()
    # Retirer formes juridiques
    cleaned = re.sub(r'\b(sasu|sas|sarl|sa|eurl|sci)\b', '', cleaned, flags=re.I)
    # Retirer mots communs
    cleaned = re.sub(r'\b(centrale|solaire|des|les?|france)\b', '', cleaned, flags=re.I)
    return cleaned.strip()


def extract_commune_names_from_text(text: str) -> List[str]:
    """Extrait noms de communes depuis un texte (basique)"""
    # Pattern simple : mots capitalisés suivis de (XX)
    pattern = r'\b([A-Z][a-zé\-]+(?:\s+[a-zé\-]+)*)\s+\((\d{2,3})\)'
    matches = re.findall(pattern, text)
    return [commune for commune, dept in matches]


# ============ SCORING D'URL ============

@dataclass
class URLScore:
    """Score de pertinence d'une URL"""
    url: str
    score: float  # 0.0 à 1.0
    signals: Dict[str, float] = field(default_factory=dict)
    should_download: bool = False
    priority: int = 5  # 1 (haute) à 5 (basse)
    rejection_reason: Optional[str] = None


def score_url_relevance(
    url: str,
    snippet: str,
    title: str,
    commune: str,
    dept: str,
    demandeur: Optional[str] = None
) -> URLScore:
    """
    Calcule un score de pertinence composite au lieu d'un rejet binaire
    
    Retourne un URLScore avec:
    - score: 0.0 à 1.0
    - should_download: True si >= 0.25
    - priority: 1 (haute) à 5 (basse)
    
    Signaux positifs:
    + 0.4 : commune dans URL
    + 0.25 : commune dans snippet
    + 0.15 : commune dans titre
    + 0.1 : département cohérent
    + 0.3 : demandeur mentionné (bonus fort)
    + 0.2 : type consultation publique
    
    Signaux négatifs:
    - 0.5 : autre commune dans URL
    - 0.2 : département absent
    """
    signals = {}
    score = 0.0
    rejection_reason = None
    
    # Normalisation
    commune_variants = get_commune_variations(commune)
    url_lower = url.lower()
    snippet_lower = snippet.lower()
    title_lower = title.lower()
    
    # ========== SIGNAL 1 : Commune dans URL ==========
    if any(var in url_lower for var in commune_variants):
        signals["commune_in_url"] = 0.4
        score += 0.4
        logger.debug(f"    ✓ Commune dans URL: +0.4")
    else:
        signals["commune_in_url"] = 0.0
    
    # ⚠️ ANTI-SIGNAL : Autre commune explicite dans URL
    other_communes_in_url = extract_commune_names_from_text(url)
    if other_communes_in_url:
        found_other = [c for c in other_communes_in_url if normalize_commune(c) != normalize_commune(commune)]
        if found_other:
            signals["other_commune_in_url"] = -0.5
            score -= 0.5
            rejection_reason = f"URL contient autre commune: {found_other[0]}"
            logger.debug(f"    ⚠️  Autre commune dans URL: {found_other[0]} (attendu: {commune})")
    
    # ========== SIGNAL 2 : Commune dans snippet ==========
    if any(var in snippet_lower for var in commune_variants):
        signals["commune_in_snippet"] = 0.25
        score += 0.25
        logger.debug(f"    ✓ Commune dans snippet: +0.25")
    
    # ========== SIGNAL 3 : Commune dans titre ==========
    if any(var in title_lower for var in commune_variants):
        signals["commune_in_title"] = 0.15
        score += 0.15
        logger.debug(f"    ✓ Commune dans titre: +0.15")
    
    # ========== SIGNAL 4 : Département cohérent ==========
    dept_pattern = rf"\b{dept}\b"
    if re.search(dept_pattern, url_lower + snippet_lower + title_lower):
        signals["dept_match"] = 0.1
        score += 0.1
    else:
        signals["dept_match"] = -0.2
        score -= 0.2
        logger.debug(f"    ⚠️  Département {dept} absent")
    
    # ========== SIGNAL 5 : Demandeur mentionné (bonus FORT) ==========
    if demandeur and len(demandeur) > 10:
        demandeur_clean = clean_company_name(demandeur)
        if demandeur_clean and len(demandeur_clean) > 5:
            if demandeur_clean in snippet_lower or demandeur_clean in title_lower:
                signals["demandeur_match"] = 0.3
                score += 0.3
                logger.debug(f"    ✓✓ Demandeur trouvé: +0.3 (signal très fort)")
    
    # ========== SIGNAL 6 : Type de source ==========
    if "consultation-du-public" in url_lower or "participation" in url_lower:
        signals["source_type_consultation"] = 0.2
        score += 0.2
    elif "/IMG/pdf/ap-" in url_lower or "arrete" in url_lower:
        signals["source_type_arrete"] = 0.15
        score += 0.15
    
    # ========== SIGNAL 7 : URL opaque (neutre) ==========
    if re.search(r'/\d{4,}\.pdf$', url_lower):
        signals["opaque_url"] = 0.0
        logger.debug("    ⚪ URL opaque (ID numérique) → neutre")
    
    # ========== Décision finale ==========
    score = max(0.0, min(1.0, score))
    
    # Seuil de téléchargement adaptatif
    should_download = score >= 0.25
    
    # Calcul priorité
    if score >= 0.7:
        priority = 1
    elif score >= 0.5:
        priority = 2
    elif score >= 0.3:
        priority = 3
    elif score >= 0.1:
        priority = 4
    else:
        priority = 5
        should_download = False
    
    return URLScore(
        url=url,
        score=score,
        signals=signals,
        should_download=should_download,
        priority=priority,
        rejection_reason=rejection_reason
    )


# ============ VALIDATION CONTENU PDF ============

def validate_content_match(
    pdf_text: str,
    commune: str,
    demandeur: Optional[str] = None,
    min_mentions: int = 2
) -> Tuple[bool, float, str]:
    """
    Valide qu'un PDF extrait correspond bien au projet cible
    
    Returns:
        (is_valid, confidence_adjustment, reason)
    """
    text_lower = pdf_text.lower()
    reasons = []
    confidence_adjustment = 1.0
    
    # 1. Vérifier présence commune
    commune_variants = get_commune_variations(commune)
    mentions = sum(text_lower.count(var) for var in commune_variants)
    
    if mentions == 0:
        return False, 0.0, f"Commune '{commune}' absente du PDF"
    
    if mentions < min_mentions:
        reasons.append(f"Commune mentionnée seulement {mentions}x")
        confidence_adjustment *= 0.8
    else:
        reasons.append(f"Commune mentionnée {mentions}x ✓")
    
    # 2. Vérifier demandeur
    if demandeur:
        demandeur_clean = clean_company_name(demandeur)
        if demandeur_clean and len(demandeur_clean) > 5:
            if demandeur_clean in text_lower:
                reasons.append(f"Demandeur '{demandeur}' trouvé ✓")
                confidence_adjustment *= 1.1
            else:
                # Fuzzy match : chercher parties du demandeur
                parts = demandeur_clean.split()
                if any(part in text_lower for part in parts if len(part) > 5):
                    reasons.append(f"Partie demandeur trouvée")
                else:
                    reasons.append(f"Demandeur '{demandeur}' absent (warning)")
                    confidence_adjustment *= 0.9
    
    # 3. Anti-pattern : autre commune dominante
    all_communes = extract_commune_names_from_text(pdf_text[:5000])
    if all_communes:
        commune_counts = {c: all_communes.count(c) for c in set(all_communes)}
        if commune_counts:
            most_frequent = max(commune_counts, key=commune_counts.get)
            if normalize_commune(most_frequent) != normalize_commune(commune):
                if commune_counts[most_frequent] > mentions * 2:
                    return False, 0.0, f"Autre commune dominante: {most_frequent} ({commune_counts[most_frequent]}x vs {commune} {mentions}x)"
    
    reason = " | ".join(reasons)
    is_valid = confidence_adjustment >= 0.7
    
    return is_valid, confidence_adjustment, reason


# ============ VALIDATION POST-EXTRACTION ============

def validate_permit_data(
    project: Dict,
    permit_data: Dict,
    pdf_text: Optional[str] = None
) -> Tuple[bool, List[str]]:
    """
    Validation finale avant écriture CSV
    
    Returns:
        (is_valid, list_of_warnings)
    """
    warnings = []
    
    # 1. Vérifier cohérence dates
    cas_par_cas_date = project.get('cas_par_cas_date')
    permit_deposit_date = permit_data.get('deposit_date')
    
    if cas_par_cas_date and permit_deposit_date:
        # Le permis ne peut pas être déposé AVANT la décision cas par cas
        if permit_deposit_date < cas_par_cas_date:
            warnings.append(f"❌ Date PC ({permit_deposit_date}) < cas par cas ({cas_par_cas_date})")
            return False, warnings
    
    # 2. Vérifier URL contient commune (si pas opaque)
    source_url = permit_data.get('source_url', '')
    commune = project.get('commune', '')
    
    if source_url and commune:
        commune_normalized = normalize_commune(commune)
        
        # Si URL contient une autre commune, warning fort
        other_communes = extract_commune_names_from_text(source_url)
        if other_communes and commune not in other_communes:
            if not any(normalize_commune(c) == commune_normalized for c in other_communes):
                warnings.append(f"⚠️  URL mentionne autre commune: {other_communes}")
    
    # 3. Vérifier confiance minimale
    confidence = permit_data.get('confidence', 0.0)
    if confidence < 0.3:
        warnings.append(f"⚠️  Confiance très faible: {confidence}")
    
    # 4. Si PDF text disponible, valider contenu
    if pdf_text:
        is_valid, conf_adj, reason = validate_content_match(
            pdf_text,
            project.get('commune', ''),
            project.get('demandeur')
        )
        if not is_valid:
            warnings.append(f"❌ Validation contenu: {reason}")
            return False, warnings
        
        if conf_adj < 1.0:
            warnings.append(f"⚠️  Contenu: {reason}")
    
    # Validation réussie si pas de rejet
    is_valid = not any("❌" in w for w in warnings)
    return is_valid, warnings
