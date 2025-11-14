import asyncio
import json
import logging
import re
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Route, Page
from typing import Dict, List

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# L'URL du formulaire de recherche (pas l'accueil)
FORM_URL = "https://side.developpement-durable.gouv.fr/PAE/form.aspx?SC=AE-GENERAL"

# L'API que nous voulons intercepter
API_URL_PATTERN = "**/Search.svc/Search"

# Dictionnaire pour stocker nos découvertes
found_keys: Dict[str, Dict] = {}

async def intercept_search_request(route: Route, region_name: str):
    """
    Intercepte la requête POST, extrait le payload JSON,
    et trouve la clé de facette pour la région.
    """
    global found_keys
    
    if route.request.method != "POST":
        await route.continue_()
        return
        
    try:
        payload = route.request.post_data_json
        query = payload.get("query", {})
        facet_filter_str = query.get("FacetFilter")
        
        if not facet_filter_str:
            logger.warning(f"[{region_name}] Aucun FacetFilter trouvé dans le payload.")
            await route.continue_()
            return

        facets = json.loads(facet_filter_str)
        
        region_key = None
        region_label_norm = region_name.lower().strip()
        
        # Nous cherchons la clé (ex: _212) dont la valeur (ex: "Occitanie")
        # correspond au nom du lien sur lequel nous avons cliqué.
        for key, value in facets.items():
            if key.startswith("_") and key != "_1722": # _1722 est l'année
                if isinstance(value, str) and value.lower().strip() == region_label_norm:
                    region_key = key
                    break
        
        if region_key:
            logger.info(f"[{region_name}] ✅ Clé de facette trouvée : {region_key}")
            slug = _normalize_slug(region_name)
            found_keys[slug] = {
                "label": region_name,
                "facet_key": region_key
            }
        else:
            logger.warning(f"[{region_name}] Impossible de trouver la clé de facette pour '{region_label_norm}' dans : {facets}")

    except Exception as e:
        logger.error(f"[{region_name}] Erreur durant l'interception : {e}")
    
    # Laisse la requête se poursuivre pour ne pas bloquer la page
    await route.continue_()

def _normalize_slug(text: str) -> str:
    """Crée un slug python-friendly à partir du label."""
    text = text.lower()
    text = text.replace(" ", "-").replace("'", "-")
    # Gère les cas comme "Bourgogne-Franche Comté"
    text = text.replace("-comté", "-comte") 
    text = re.sub(r'-+', '-', text)
    return text.strip('-')

async def main():
    """
    Lance le navigateur, visite le formulaire de recherche, et clique sur
    chaque filtre de région pour intercepter les clés de l'API.
    """
    try:
        async with async_playwright() as p:
            logger.info("Lancement du navigateur (Chromium)...")
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            logger.info(f"Navigation vers le formulaire de recherche : {FORM_URL}")
            # 'domcontentloaded' est suffisant, 'networkidle' peut être trop long
            await page.goto(FORM_URL, wait_until="domcontentloaded")

            # Attend que le panneau de filtre "Autorité environnementale" soit visible
            # C'est la garantie que le JavaScript est chargé
            logger.info("Attente du panneau de filtre 'Autorité environnementale'...")
            try:
                # Utilise un sélecteur de texte robuste
                filter_heading = page.get_by_text("Autorité environnementale").first
                await filter_heading.wait_for(state="visible", timeout=15000)
            except Exception as e:
                logger.error("Impossible de trouver le panneau de filtre 'Autorité environnementale'.")
                logger.error("La structure de la page a peut-être changé ou le chargement est trop lent.")
                await browser.close()
                return

            # Trouve le conteneur 'ul' qui suit ce titre
            filter_panel = filter_heading.locator("xpath=./following-sibling::ul[1]")
            region_links = await filter_panel.locator("li > a").all()
            
            if not region_links:
                logger.error("Aucun lien de région trouvé sous 'Autorité environnementale'.")
                await browser.close()
                return

            # Récupère les noms avant de cliquer (l'itération peut être instable sinon)
            regions_to_click = []
            for link in region_links:
                text = await link.text_content()
                if text:
                    regions_to_click.append(text.strip())
            
            logger.info(f"{len(regions_to_click)} filtres de région trouvés. Début de l'analyse...")

            # Met en place l'intercepteur une seule fois
            # Il attrapera TOUTES les requêtes Search, nous devons l'affiner
            captured_payloads = {}
            def handle_route(route: Route):
                if route.request.method == "POST":
                    try:
                        payload = route.request.post_data_json
                        captured_payloads[route.request.url] = payload
                    except Exception:
                        pass # Ignore les payloads non-JSON
                asyncio.create_task(route.continue_())
            
            await page.route(API_URL_PATTERN, handle_route)

            # Clique sur chaque lien de région
            for region_name in regions_to_click:
                try:
                    logger.info(f"--- Test de la région : {region_name} ---")
                    link_locator = page.get_by_text(region_name).first
                    await link_locator.click(timeout=5000)
                    
                    # Attend que l'activité réseau se termine (l'API a été appelée)
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception as e:
                    logger.error(f"Échec du clic ou timeout pour {region_name} : {e}")
            
            await browser.close()
            logger.info("Navigateur fermé.")

    except Exception as e:
        logger.error(f"Erreur Playwright : {e}")
        logger.error("Assurez-vous d'avoir installé Playwright :")
        logger.error("  pip install playwright")
        logger.error("  playwright install")
        return

    # --- Analyse des Payloads Capturés ---
    if not captured_payloads:
        logger.error("Aucune requête API 'Search.svc' n'a été interceptée.")
        return

    logger.info(f"{len(captured_payloads)} requêtes API ont été capturées. Analyse des FacetFilters...")
    
    final_keys = {}
    for url, payload in captured_payloads.items():
        try:
            query = payload.get("query", {})
            facet_filter_str = query.get("FacetFilter")
            if not facet_filter_str: continue

            facets = json.loads(facet_filter_str)
            
            for key, value in facets.items():
                if key.startswith("_") and key != "_1722": # _1722 est l'année
                    if isinstance(value, str) and value.strip():
                        region_name = value.strip()
                        slug = _normalize_slug(region_name)
                        if slug not in final_keys:
                            logger.info(f"[{region_name}] ✅ Clé de facette trouvée : {key}")
                            final_keys[slug] = {
                                "label": region_name,
                                "facet_key": key
                            }
        except Exception as e:
            logger.error(f"Erreur d'analyse du payload : {e}")

    # --- Affichage du résultat ---
    if not final_keys:
        logger.error("Aucune clé de facette n'a pu être extraite des payloads capturés.")
        return
        
    logger.info("\n\n" + "="*70)
    logger.info("🎉 CAPTURE TERMINÉE 🎉")
    logger.info("Voici le dictionnaire à copier dans le script 'side_comparaison.py':")
    
    print("\nREGIONS_A_TESTER = {")
    for slug, config in sorted(final_keys.items()):
        py_slug = slug.replace("-", "_")
        print(f"    \"{py_slug}\": {{")
        print(f"        \"label\": \"{config['label']}\",")
        print(f"        \"facet_key\": \"{config['facet_key']}\"")
        print(f"    }},")
    print("}\n")
    logger.info("Vous pouvez maintenant coller ceci dans 'side_comparaison.py' et l'exécuter.")

if __name__ == "__main__":
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright n'est pas installé.")
        logger.error("Veuillez exécuter : pip install playwright")
        logger.error("Puis : playwright install")
        exit(1)
        
    asyncio.run(main())