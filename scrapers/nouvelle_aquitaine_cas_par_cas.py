#!/usr/bin/env python3
"""
Récupération des projets « examen au cas par cas » Nouvelle-Aquitaine.
-------------------------------------------------------------------------

Le visualiseur cartographique de l'ARB NA expose ces données via le WFS
SIGENA (`https://datacarto.sigena.fr/wfs`). Trois couches sont disponibles :

    - ms:l_cc_ae_p_r75  → localisations ponctuelles
    - ms:l_cc_ae_l_r75  → tracés linéaires
    - ms:l_cc_ae_s_r75  → périmètres surfaciques

Ce script interroge directement l'API WFS (OGC 2.0.0), convertit les
réponses GML en GeoJSON et sauvegarde le tout dans un fichier unique.
Il est volontairement « test » : aucune dépendance externe, tout repose
sur la bibliothèque standard.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlencode
from urllib.request import urlopen
import xml.etree.ElementTree as ET

LOGGER = logging.getLogger("cas_par_cas_na")

WFS_ENDPOINT = "https://datacarto.sigena.fr/wfs"
NAMESPACES = {
    "wfs": "http://www.opengis.net/wfs/2.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "ms": "http://mapserver.gis.umn.edu/mapserver",
}

LAYER_REGISTRY = {
    "cas_par_cas_points": "ms:l_cc_ae_p_r75",
    "cas_par_cas_lineaires": "ms:l_cc_ae_l_r75",
    "cas_par_cas_surfaces": "ms:l_cc_ae_s_r75",
}


def _local_name(tag: str) -> str:
    """Retourne le nom local d'une balise XML (`{ns}Tag` → `Tag`)."""
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _parse_pos_list(text: str, swap_axes: bool) -> List[List[float]]:
    """Convertit une chaîne gml:posList en coordonnées GeoJSON."""
    if not text:
        return []
    values = [float(val) for val in text.split()]
    coords: List[List[float]] = []
    for idx in range(0, len(values), 2):
        first, second = values[idx], values[idx + 1]
        if swap_axes:
            coords.append([second, first])
        else:
            coords.append([first, second])
    return coords


def _swap_needed(srs_name: str) -> bool:
    """Détermine si l'ordre lat/lon doit être inversé pour du GeoJSON."""
    if not srs_name:
        return False
    normalized = srs_name.lower()
    return "4326" in normalized


def _point_coords(point_elem: ET.Element, swap_axes: bool) -> List[float]:
    pos_text = point_elem.findtext("gml:pos", namespaces=NAMESPACES)
    coords = _parse_pos_list(pos_text or "", swap_axes)
    if not coords:
        raise ValueError("Point sans coordonnées")
    return coords[0]


def _parse_point(point_elem: ET.Element, swap_axes: bool) -> Dict:
    return {"type": "Point", "coordinates": _point_coords(point_elem, swap_axes)}


def _linestring_coords(line_elem: ET.Element, swap_axes: bool) -> List[List[float]]:
    pos_text = line_elem.findtext("gml:posList", namespaces=NAMESPACES)
    coords = _parse_pos_list(pos_text or "", swap_axes)
    if not coords:
        raise ValueError("Ligne sans coordonnées")
    return coords


def _parse_linestring(line_elem: ET.Element, swap_axes: bool) -> Dict:
    return {"type": "LineString", "coordinates": _linestring_coords(line_elem, swap_axes)}


def _polygon_rings(poly_elem: ET.Element, swap_axes: bool) -> List[List[List[float]]]:
    rings: List[List[List[float]]] = []
    for ring in poly_elem.findall(".//gml:LinearRing", namespaces=NAMESPACES):
        pos_text = ring.findtext("gml:posList", namespaces=NAMESPACES)
        ring_coords = _parse_pos_list(pos_text or "", swap_axes)
        if ring_coords:
            rings.append(ring_coords)
    if not rings:
        raise ValueError("Polygone sans anneaux")
    exterior, *holes = rings
    return [exterior, *holes]


def _parse_polygon(poly_elem: ET.Element, swap_axes: bool) -> Dict:
    return {"type": "Polygon", "coordinates": _polygon_rings(poly_elem, swap_axes)}


def _parse_multipoint(multi_elem: ET.Element, swap_axes: bool) -> Dict:
    coords: List[List[float]] = []
    for point in multi_elem.findall(".//gml:Point", namespaces=NAMESPACES):
        try:
            coords.append(_point_coords(point, swap_axes))
        except ValueError:
            continue
    if not coords:
        raise ValueError("MultiPoint sans coordonnées")
    return {"type": "MultiPoint", "coordinates": coords}


def _parse_multicurve(multi_elem: ET.Element, swap_axes: bool) -> Dict:
    lines: List[List[List[float]]] = []
    line_candidates = multi_elem.findall(".//gml:LineString", namespaces=NAMESPACES)
    if not line_candidates:
        line_candidates = multi_elem.findall(".//gml:LineStringSegment", namespaces=NAMESPACES)
    for line in line_candidates:
        try:
            lines.append(_linestring_coords(line, swap_axes))
        except ValueError:
            continue
    if not lines:
        raise ValueError("MultiCurve sans coordonnées")
    return {"type": "MultiLineString", "coordinates": lines}


def _parse_multisurface(multi_elem: ET.Element, swap_axes: bool) -> Dict:
    polygons: List[List[List[List[float]]]] = []
    polygon_candidates = multi_elem.findall(".//gml:Polygon", namespaces=NAMESPACES)
    if not polygon_candidates:
        polygon_candidates = multi_elem.findall(".//gml:PolygonPatch", namespaces=NAMESPACES)
    for poly in polygon_candidates:
        try:
            polygons.append(_polygon_rings(poly, swap_axes))
        except ValueError:
            continue
    if not polygons:
        raise ValueError("MultiSurface sans coordonnées")
    return {"type": "MultiPolygon", "coordinates": polygons}


def _parse_geometry(container: Optional[ET.Element], fallback_srs: str) -> Optional[Dict]:
    if container is None or not list(container):
        return None
    geometry_elem = list(container)[0]
    geom_type = _local_name(geometry_elem.tag)
    srs_name = geometry_elem.attrib.get("srsName", fallback_srs)
    swap_axes = _swap_needed(srs_name)
    if geom_type == "Point":
        return _parse_point(geometry_elem, swap_axes)
    if geom_type == "LineString":
        return _parse_linestring(geometry_elem, swap_axes)
    if geom_type == "Polygon":
        return _parse_polygon(geometry_elem, swap_axes)
    if geom_type == "MultiPoint":
        return _parse_multipoint(geometry_elem, swap_axes)
    if geom_type == "MultiCurve":
        return _parse_multicurve(geometry_elem, swap_axes)
    if geom_type in {"MultiSurface", "MultiPolygon"}:
        return _parse_multisurface(geometry_elem, swap_axes)
    raise NotImplementedError(f"Géométrie {geom_type} non supportée")


def _extract_properties(feature: ET.Element) -> Dict:
    props: Dict[str, Optional[str]] = {}
    for child in feature:
        local = _local_name(child.tag)
        if local in {"boundedBy", "msGeometry"}:
            continue
        if len(child) == 1 and _local_name(child[0].tag) == "timePosition":
            props[local] = (child[0].text or "").strip()
            continue
        text_content = "".join(child.itertext()).strip()
        props[local] = text_content
    gml_id = feature.attrib.get("{http://www.opengis.net/gml/3.2}id")
    if gml_id:
        props["gml_id"] = gml_id
    return props


def parse_wfs_response(payload: bytes, layer_key: str, fallback_srs: str) -> List[Dict]:
    """Convertit la réponse WFS (GML) en features GeoJSON."""
    root = ET.fromstring(payload)
    features: List[Dict] = []
    for member in root.findall("wfs:member", namespaces=NAMESPACES):
        feature_elem = next(iter(member), None)
        if feature_elem is None:
            continue
        try:
            geometry = _parse_geometry(feature_elem.find("ms:msGeometry", NAMESPACES), fallback_srs)
        except NotImplementedError as exc:
            LOGGER.warning("Géométrie ignorée (%s): %s", layer_key, exc)
            geometry = None
        props = _extract_properties(feature_elem)
        props["source_layer"] = layer_key
        features.append(
            {
                "type": "Feature",
                "id": props.get("gml_id"),
                "geometry": geometry,
                "properties": props,
            }
        )
    return features


def fetch_layer(
    alias: str,
    typename: str,
    srs_name: str,
    page_size: int,
    limit: Optional[int],
) -> List[Dict]:
    """Récupère l'intégralité d'une couche WFS par pagination."""
    start = 0
    collected: List[Dict] = []
    LOGGER.info("Téléchargement couche %s (%s)", alias, typename)
    while True:
        remaining = limit - len(collected) if limit is not None else None
        effective_size = page_size if remaining is None else min(page_size, max(remaining, 0))
        if effective_size == 0:
            break
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": typename,
            "srsName": srs_name,
            "count": effective_size,
            "startIndex": start,
        }
        url = f"{WFS_ENDPOINT}?{urlencode(params)}"
        LOGGER.debug("GET %s", url)
        with urlopen(url, timeout=60) as response:
            payload = response.read()
        batch = parse_wfs_response(payload, alias, srs_name)
        if not batch:
            break
        collected.extend(batch)
        LOGGER.debug("  + %d entités (total %d)", len(batch), len(collected))
        if len(batch) < effective_size:
            break
        start += effective_size
        if limit is not None and len(collected) >= limit:
            break
    LOGGER.info("Couche %s → %d entités", alias, len(collected))
    return collected


def run(
    layers: Sequence[str],
    output: Path,
    srs_name: str,
    page_size: int,
    limit: Optional[int],
) -> Dict:
    """Collecte les couches demandées et renvoie le FeatureCollection."""
    features: List[Dict] = []
    for alias in layers:
        typename = LAYER_REGISTRY[alias]
        features.extend(fetch_layer(alias, typename, srs_name, page_size, limit))
    metadata = {
        "source": WFS_ENDPOINT,
        "layers": list(layers),
        "srs": srs_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "feature_count": len(features),
    }
    feature_collection = {"type": "FeatureCollection", "features": features, "metadata": metadata}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(feature_collection, ensure_ascii=False, indent=2))
    LOGGER.info("GeoJSON écrit dans %s", output)
    return feature_collection


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Télécharge toutes les couches 'cas par cas' Nouvelle-Aquitaine (WFS SIGENA)."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("out/cas_par_cas_nouvelle_aquitaine.geojson"),
        help="Fichier GeoJSON de sortie.",
    )
    parser.add_argument(
        "--srs",
        default="EPSG:4326",
        help="Projection demandée auprès du WFS (défaut: EPSG:4326).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Nombre d'entités par page WFS.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite d'entités par couche (debug).",
    )
    parser.add_argument(
        "--layers",
        nargs="*",
        choices=list(LAYER_REGISTRY.keys()),
        default=list(LAYER_REGISTRY.keys()),
        help="Sous-ensemble de couches à interroger.",
    )
    parser.add_argument("--log-level", default="INFO", help="Niveau de log (INFO, DEBUG, ...).")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(message)s")
    run(args.layers, args.output, args.srs, args.page_size, args.limit)


if __name__ == "__main__":
    main()
