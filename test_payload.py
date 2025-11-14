import httpx
import json
import logging
import time  # <--- Importation corrigée
from pathlib import Path

# Configuration
SEARCH_ENDPOINT = "https://side.developpement-durable.gouv.fr/PAE/Portal/Recherche/Search.svc/Search"
PAYLOAD_FILE = Path("payload_occitanie.json")

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

def test_payload():
    """
    Charge un payload JSON depuis un fichier et l'envoie à l'API SIDE.
    Ceci teste si le payload capturé est valide.
    """
    
    # --- 1. Charger le Payload ---
    if not PAYLOAD_FILE.exists():
        logger.error(f"Erreur : Fichier payload non trouvé : {PAYLOAD_FILE}")
        logger.error("Veuillez capturer le JSON depuis votre navigateur (Outils F12 -> Réseau -> XHR -> Search -> Charge utile) et le sauvegarder ici.")
        return
        
    logger.info(f"Chargement du payload depuis {PAYLOAD_FILE}...")
    try:
        with open(PAYLOAD_FILE, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        logger.info("Payload chargé avec succès.")
    except Exception as e:
        logger.error(f"Erreur de lecture du JSON : {e}")
        return

    # --- 2. Envoyer la requête POST ---
    logger.info(f"Envoi de la requête POST vers {SEARCH_ENDPOINT}...")
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.post(SEARCH_ENDPOINT, json=payload)
            response.raise_for_status() # Lève une exception si HTTP 4xx/5xx
            
            data = response.json()
            
            # --- 3. Analyser la réponse de l'API ---
            if data.get("success", False):
                logger.info("✅ SUCCÈS ! L'API a accepté le payload.")
                
                # 'd' est la clé principale de la réponse
                api_data = data.get("d", {})
                results = api_data.get("Results", [])
                total_hits = api_data.get("SearchInfo", {}).get("NBResults", 0)
                
                logger.info(f"Nombre total de résultats trouvés : {total_hits}")
                logger.info(f"Affichage des {len(results)} premiers résultats :")
                
                for i, res in enumerate(results):
                    field_list = res.get("FieldList", {})
                    title = (field_list.get("Title") or ["Titre inconnu"])[0]
                    logger.info(f"  {i+1}. {title}")
                    
            else:
                # C'est l'erreur que nous avions : {"success": false, ...}
                logger.error(f"❌ ÉCHEC : L'API a rejeté le payload.")
                logger.error(f"   Message de l'API : {data.get('message', 'Aucun message')}")
                logger.error(f"   Données complètes : {data}")

    except httpx.HTTPStatusError as e:
        logger.error(f"Erreur HTTP : {e.response.status_code} {e.response.text}")
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")

if __name__ == "__main__":
    logger.info("--- DÉBUT DU TEST DE PAYLOAD ---")
    start_time = time.time()
    
    test_payload()
    
    duration = time.time() - start_time
    logger.info(f"--- TEST TERMINÉ (Durée: {duration:.2f}s) ---")