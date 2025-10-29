text
# Veille Stockage POC

Outil de veille automatisée pour les projets de stockage d'énergie par batterie (BESS) en France.

## 🎯 Fonctionnalités

1. **Découverte** : Scraping sites DREAL par région/année
2. **Extraction** : Téléchargement et analyse documents (PDF/ZIP)
3. **Enrichissement** : Recherche permis de construire via IA (Gemini + Google Search)

## 📦 Installation

Créer environnement virtuel
python -m venv venv
source venv/bin/activate # Linux/Mac

OU
venv\Scripts\activate # Windows

Installer dépendances
pip install -r requirements.txt

Configurer variables d'environnement
Copier .env.example vers .env et remplir avec vos clés API

## 🔑 Configuration

Éditez le fichier `.env` avec vos clés API Google :

GOOGLE_API_KEY=votre_cle_gemini_ici
OAUTH_CLIENT_ID=votre_oauth_client_id
OAUTH_CLIENT_SECRET=votre_oauth_secret
OAUTH_REFRESH_TOKEN=votre_refresh_token
DOCAI_PROCESS_URL=https://documentai.googleapis.com/.../process

## 🚀 Usage

### Workflow Complet

Étape 1 : Découverte projets DREAL
python discover.py --region auvergne-rhone-alpes --year 2024 --output out/projects/aura_2024.csv

Étape 2 : Extraction + Analyse documents
python extract.py --input out/projects/aura_2024.csv --output out/analyzed/aura_2024.csv

Étape 3 : Enrichissement permis de construire
python enrich.py --input out/analyzed/aura_2024.csv --output out/enriched/aura_2024.csv


### Options Avancées

Découverte département spécifique
python discover.py --region auvergne-rhone-alpes --dept 01 --year 2024

Extraction avec cache
python extract.py --input projects.csv --use-cache

Enrichissement avec validation manuelle
python enrich.py --input analyzed.csv --confidence-threshold 0.8

## 📁 Structure Projet

veille-stockage-poc/
├── config.py # Configuration centralisée
├── models.py # Modèles de données
├── utils.py # Fonctions utilitaires
├── discover.py # CLI découverte
├── extract.py # CLI extraction
├── enrich.py # CLI enrichissement
├── scrapers/ # Modules scrapers régionaux
│ ├── init.py
│ └── auvergne_rhone_alpes.py
├── out/ # Résultats (créé automatiquement)
│ ├── projects/
│ ├── analyzed/
│ └── enriched/
└── cache/ # Cache optionnel (créé automatiquement)

## 📊 Résultats

Les résultats sont exportés en CSV avec les colonnes suivantes :

**Découverte** : project_id, region, dept, year, project_title, project_url

**Analyse** : + puissance_MW, energie_MWh, commune, demandeur, cas_par_cas_decision

**Enrichissement** : + permit_number, permit_date, permit_source, permit_confidence

## 🔧 Régions Supportées

- Auvergne-Rhône-Alpes
- Normandie (à venir)

Pour ajouter une région : créer `scrapers/votre_region.py` avec méthode `discover_projects()`

## 📝 Logs

Les logs sont affichés en console. Pour sauvegarder :

python discover.py --region aura --year 2024 2>&1 | tee logs/discover.log

text

## ⚠️ Limites

- Quota Gemini : 1500 requêtes/jour (gratuit)
- Document AI : quota selon config GCP
- Rate limiting : 0.8s entre requêtes HTTP

## 📄 Licence

Usage interne uniquement
Ordre de Création Recommandé
Pour développer dans l'ordre logique :

✅ Fichiers configuration (déjà fournis)

requirements.txt

.env.example

config.py

models.py

✅ Utilitaires (déjà fourni)

utils.py

📝 Scraper régional (prochain à créer)

scrapers/auvergne_rhone_alpes.py

📝 CLI Découverte (après scraper)

discover.py

📝 CLI Extraction (après discover)

extract.py

📝 CLI Enrichissement (en dernier)

enrich.py