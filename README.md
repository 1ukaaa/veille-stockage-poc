text
# Veille Stockage POC

Outil de veille automatisée pour les projets de stockage d'énergie par batterie (BESS) en France.

## 🎯 Fonctionnalités

1. **Découverte** : Scraping sites DREAL par région/année
2. **Extraction** : Téléchargement et analyse documents (PDF/ZIP)
3. **Enrichissement** : Recherche permis de construire via IA (Gemini + Google Search)
4. **Enrichissement v2** : Version optimisée ciblant les sites préfectoraux pour récupérer les preuves de dépôt (PC & ICPE)

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

Étape 3 bis : Enrichissement v2 dédié aux préfectures (preuves de dépôt PC + ICPE)
python enrich_v2.py --input out/analyzed/aura_2024.csv --output out/enriched/aura_2024_v2.csv

Étape 3 ter : Enrichissement v2 avec validation IA (Gemini)
python enrich_v2.py --input out/analyzed/aura_2024.csv --use-ai --limit 5

Étape 4 : Enrichissement v3 (SerpAPI + IA, recommandé pour les preuves de dépôt)
python enrich_v3.py --input out/analyzed/aura_2024.csv --search-engine auto --use-ai --limit 5

Étape 4 bis : Enrichissement v3 avec Gemini Ground Search (sans SerpAPI)
python enrich_v3.py --input out/analyzed/aura_2024.csv --search-engine ground --ground-key $GEMINI_API_KEY --use-ai --limit 5


### Options Avancées

Découverte département spécifique
python discover.py --region auvergne-rhone-alpes --dept 01 --year 2024

Extraction avec cache
python extract.py --input projects.csv --use-cache

Enrichissement avec validation manuelle
python enrich.py --input analyzed.csv --debug

## 📁 Structure Projet

veille-stockage-poc/
├── config.py # Configuration centralisée
├── models.py # Modèles de données
├── utils.py # Fonctions utilitaires
├── discover.py # CLI découverte
├── extract.py # CLI extraction
├── enrich.py # CLI enrichissement
├── scrapers/ # Modules scrapers régionaux
│ ├── __init__.py
│ ├── auvergne_rhone_alpes.py
│ └── bourgogne_franche_comte.py
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

**Enrichissement v2** (en plus des champs ci-dessus) :
- permit_deposit_date, permit_issue_date, permit_summary
- icpe_deposit_date, icpe_reference, icpe_source, icpe_confidence, icpe_summary
- Option `--use-ai` : ajoute une validation Gemini des documents suspects pour confirmer les preuves de dépôt

**Enrichissement v3** :
- Exploite Gemini Ground Search, SerpAPI ou CSE (selon configuration) + pipeline IA Gemini
- Même champs que v2
- Options supplémentaires : `--search-engine {auto,cse,serpapi,ground}`, `--serpapi-key`, `--serpapi-max`, `--ground-key`, `--ground-model`, `--use-ai`

## 🔑 API externes

- GOOGLE_API_KEY / GOOGLE_CSE_ID : pour CSE et fallback
- SERPAPI_KEY : pour activer le moteur SerpAPI (recommandé pour retrouver les preuves introuvables via CSE)
- GEMINI_API_KEY : pour la validation IA (sinon réutilise GOOGLE_API_KEY si compatible)
- GEMINI_SEARCH_KEY (optionnel) : clé dédiée au Gemini Ground Search (sinon `GEMINI_API_KEY`)

## 🔧 Régions Supportées

- Auvergne-Rhône-Alpes
- Bourgogne-Franche-Comté

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
