import asyncio
import json
import logging
from pathlib import Path
from playwright.async_api import async_playwright, Route

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# L'URL du formulaire/de la recherche que vous avez trouvée
FORM_URL = "https://side.developpement-durable.gouv.fr/PAE/form.aspx?SC=AE-GENERAL#/Search/(query:(AdvancedQuery:(queryGroups:!((logical:!n,queryClauses:!((index:Title_idx,logical:0,operator:0,otherValue:!n,value:stockage))))),AdvancedQueryDisplay:'(Titre=stockage)',FacetFilter:'%7B%22_1722%22:%222024%22,%22_212%22:%22Occitanie%22%7D',ForceSearch:!t,InitialSearch:!f,Page:0,PageRange:3,QueryGuid:ceffc470-e8a5-4927-954c-aa593e774e3e,ResultSize:50,ScenarioCode:AE-GENERAL,ScenarioDisplayMode:display-standard,SearchContext:1,SearchGridFieldsShownOnResultsDTO:!(),SearchTerms:'%20stockage',SortField:DateOfModification_sort,SortOrder:0,TemplateParams:(Scenario:'',Scope:PAE,Size:!n,Source:'',Support:'',UseCompact:!f),UseSpellChecking:!n),sst:4)"

# L'URL de l'API que nous voulons intercepter
API_URL = "https://side.developpement-durable.gouv.fr/PAE/Portal/Recherche/Search.svc/Search"

# Le fichier où sauvegarder le payload
OUTPUT_FILE = Path("payload_occitanie.json")

# Variable globale pour stocker le payload (simpliste mais efficace)
payload_found = None

async def intercept_search_request(route: Route):
    """
    Cette fonction est appelée pour chaque requête qui match API_URL.
    Elle capture le payload POST et laisse la requête continuer.
    """
    global payload_found
    
    if route.request.method == "POST":
        logger.info(f"Interception de la requête POST vers {API_URL}")
        try:
            payload = route.request.post_data_json
            payload_found = payload
            logger.info("Payload JSON capturé !")
        except Exception as e:
            logger.error(f"Impossible de lire le JSON du payload : {e}")
            
    # Laisse la requête se poursuivre normalement
    await route.continue_()

async def main():
    global payload_found
    
    # Vérifie si Playwright est installé
    try:
        async with async_playwright() as p:
            logger.info("Lancement du navigateur (Chromium)...")
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Mettre en place l'interception AVANT d'accéder à la page
            logger.info(f"Mise en place de l'interception pour : {API_URL}")
            await page.route(API_URL, intercept_search_request)
            
            logger.info(f"Navigation vers l'URL du formulaire...")
            try:
                # 'networkidle' attend que le réseau se calme (après les appels API)
                await page.goto(FORM_URL, wait_until="networkidle", timeout=60000)
            except Exception as e:
                logger.warning(f"Erreur de navigation (timeout?): {e}")

            await browser.close()
            logger.info("Navigateur fermé.")

    except Exception as e:
        logger.error(f"Erreur Playwright : {e}")
        logger.error("Avez-vous installé Playwright ? ('pip install playwright')")
        logger.error("Et ses navigateurs ? ('playwright install')")
        return

    # --- Sauvegarde du Payload ---
    if payload_found:
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(payload_found, f, indent=2, ensure_ascii=False)
            logger.info(f"✅ SUCCÈS ! Payload sauvegardé dans : {OUTPUT_FILE}")
            logger.info("Vous pouvez maintenant lancer 'python test_payload.py' pour valider ce payload.")
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du fichier JSON : {e}")
    else:
        logger.error("❌ ÉCHEC : Aucun payload 'Search' n'a été intercepté.")
        logger.error("Vérifiez que l'URL du formulaire est correcte et qu'elle déclenche bien une recherche.")

if __name__ == "__main__":
    # Vérification des dépendances
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright n'est pas installé.")
        logger.error("Veuillez exécuter : pip install playwright")
        logger.error("Puis : playwright install")
        exit(1)
        
    asyncio.run(main())