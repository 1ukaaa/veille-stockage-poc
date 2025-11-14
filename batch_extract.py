#!/usr/bin/env python3
"""
Lanceur de batch pour extract.py

Ce script trouve tous les fichiers CSV 'projets_bess_TOUTES_REGIONS_*.csv'
dans le dossier courant et exécute 'extract.py --input <fichier>'
pour chacun d'eux.
"""
import subprocess
import glob
import logging
from pathlib import Path

# Configuration du logging pour voir ce qu'il se passe
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Nom du script à appeler (votre script)
EXTRACT_SCRIPT = "extract.py"
# Modèle de nom pour les fichiers CSV
CSV_PATTERN = "projets_bess_TOUTES_REGIONS_*.csv"

def main():
    # S'assurer que le script d'extraction existe
    if not Path(EXTRACT_SCRIPT).exists():
        logger.error(f"Erreur fatale : Le script '{EXTRACT_SCRIPT}' est introuvable.")
        logger.error("Veuillez placer ce script dans le même dossier que extract.py.")
        return

    # Trouver tous les fichiers CSV qui correspondent au modèle
    csv_files = glob.glob(CSV_PATTERN)
    if not csv_files:
        logger.warning(f"Aucun fichier CSV trouvé correspondant au modèle '{CSV_PATTERN}'.")
        return

    logger.info(f"Trouvé {len(csv_files)} fichiers CSV à traiter : {sorted(csv_files)}")

    success_count = 0
    fail_count = 0

    # Boucler sur chaque fichier CSV trouvé
    for csv_file in sorted(csv_files): # Trier pour un ordre prévisible
        logger.info("\n%s\nTraitement de : %s\n%s", "="*50, csv_file, "="*50)
        
        # Construire la commande, exactement comme dans votre 'Usage'
        command = [
            "python",
            EXTRACT_SCRIPT,
            "--input",
            csv_file
        ]
        
        try:
            # Exécuter extract.py et attendre qu'il se termine
            # Nous capturons la sortie pour l'afficher en cas d'erreur
            subprocess.run(
                command,
                check=True,       # Lève une erreur si extract.py échoue
                text=True,        # Capture la sortie en texte
                encoding="utf-8", # S'assurer de l'encodage
                capture_output=True # Capture stdout/stderr
            )
            logger.info(f"✅ Succès du traitement pour {csv_file}")
            success_count += 1
        
        except subprocess.CalledProcessError as e:
            # Gérer l'échec de extract.py sur ce fichier
            logger.error(f"❌ ÉCHEC du traitement pour {csv_file}.")
            logger.error(f"Le script '{EXTRACT_SCRIPT}' a retourné un code d'erreur : {e.returncode}")
            logger.error(f"Sortie STDERR:\n{e.stderr}")
            logger.error(f"Sortie STDOUT:\n{e.stdout}")
            fail_count += 1
        except FileNotFoundError:
            logger.error(f"Erreur : L'interpréteur 'python' est-il introuvable ?")
            break # Erreur système, inutile de continuer
        except Exception as e:
            logger.error(f"Une erreur inattendue est survenue pour {csv_file}: {e}")
            fail_count += 1

    logger.info("\n" + "="*50 + "\nTraitement par lot terminé.")
    logger.info(f"RÉSUMÉ : {success_count} Succès | {fail_count} Échecs")

if __name__ == "__main__":
    main()