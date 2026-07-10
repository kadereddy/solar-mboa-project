# SolarMboa – Semaine 5 : Bases de données NoSQL

Ce dossier regroupe l'exploration pratique de 5 bases NoSQL appliquées au cas
d'usage SolarMboa (supervision de 48 000 installations solaires IoT au
Cameroun) : cache de sessions, profils clients, télémétrie haute fréquence,
réseau de distribution et métriques analytiques de production.

## Structure du dossier

```
week5-nosql/
├── scripts/
│   ├── redis_cache.py          # Cache des sessions IoT (compteurs actifs)
│   ├── mongodb_profiles.py     # Profils CRM des installations
│   ├── cassandra_ingest.py     # Ingestion télémétrie haute fréquence
│   ├── neo4j_network.py        # Réseau de distribution & détection de fraude
│   └── influxdb_metrics.py     # Métriques analytiques de production solaire
└── cap-decision-fiches/
    ├── FDC-001-redis.md
    ├── FDC-002-mongodb.md
    ├── FDC-003-cassandra.md
    ├── FDC-004-neo4j.md
    └── FDC-005-influxdb.md
```

Le `docker-compose.yml` orchestrant les 5 bases se trouve à la **racine du
projet** (`solar-mboa-project/docker-compose.yml`), pas dans ce dossier.

## Bases de données utilisées

| Base          | Rôle dans SolarMboa                                                  | Modèle CAP | Fiche de décision                                   |
| ------------- | -------------------------------------------------------------------- | ---------- | --------------------------------------------------- |
| **Redis**     | Cache des sessions des 5 000 compteurs IoT les plus actifs           | AP         | [FDC-001](cap-decision-fiches/FDC-001-redis.md)     |
| **MongoDB**   | Profils CRM des 4 800 installations clients                          | CP         | [FDC-002](cap-decision-fiches/FDC-002-mongodb.md)   |
| **Cassandra** | Ingestion télémétrie brute haute fréquence                           | AP         | [FDC-003](cap-decision-fiches/FDC-003-cassandra.md) |
| **Neo4j**     | Réseau Distributeur → Technicien → Installation, détection de fraude | CP         | [FDC-004](cap-decision-fiches/FDC-004-neo4j.md)     |
| **InfluxDB**  | Métriques analytiques de production solaire (séries temporelles)     | AP         | [FDC-005](cap-decision-fiches/FDC-005-influxdb.md)  |

Chaque fiche justifie le choix technologique selon le théorème CAP, la
volumétrie estimée et le pattern d'accès (lecture vs écriture-intensive).

## Prérequis

- **Docker Desktop** installé et démarré, avec suffisamment d'espace disque
  libre (les 5 conteneurs + volumes nécessitent quelques Go)
- **Python 3.11+** avec un environnement virtuel actif (`venv`)
- Un fichier **`.env`** à la racine du projet (voir `.env.example`), avec au
  minimum :
  ```
  MONGO_USER=solarmboa
  MONGO_PASSWORD=solarmboa2025
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=solarmboa2025
  INFLUX_USER=solarmboa
  INFLUX_PASSWORD=solarmboa2025
  INFLUX_TOKEN=solarmboa-super-secret-token-2025
  INFLUX_ORG=solarmboa-tech
  INFLUX_BUCKET=solar_metrics
  ```
- Les fichiers de données sources, placés dans `data/` **à la racine du
  projet** (pas dans `week5-nosql/`) :
  - `sensors_telemetry.csv` (télémétrie brute, utilisée par Redis, Cassandra, InfluxDB)
  - `installations.json` (profils clients, utilisé par MongoDB et Neo4j)
  - `network_nodes_distributors.csv` (utilisé par Neo4j)
  - `network_nodes_technicians.csv` (utilisé par Neo4j)
  - `network_graph.csv` (relations du réseau, utilisé par Neo4j)

## Installation

Depuis la racine du projet (`solar-mboa-project/`) :

# 1. Cloner et se placer dans le dossier

git clone https://github.com/kadereddy/solar-mboa-project.git
cd solar-mboa-project

# 2. Copier les variables d'environnement

cp .env.example .env

# 3. Créer l'environnement virtuel

python -m venv venv

# 4. Activer l'environnement virtuel

.\venv\Scripts\Activate.ps1

# En cas d'erreur de politique d'exécution, lancer d'abord :

# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# 5. Installer les dépendances

pip install -r requirements.txt

# 6. Démarrer les 5 bases de données

docker compose up -d

# 7. Vérifier que tout est "healthy" (prévoir ~60-90s pour Cassandra/Neo4j)

docker compose ps

## Exécution des scripts

⚠️ **Important** : tous les scripts doivent être lancés **depuis la racine du
projet** (`solar-mboa-project/`), pas depuis `week5-nosql/scripts/`, car les
chemins vers `data/` sont résolus par rapport au répertoire d'exécution.

```powershell
python .\week5-nosql\scripts\redis_cache.py
python .\week5-nosql\scripts\mongodb_profiles.py
python .\week5-nosql\scripts\cassandra_ingest.py
python .\week5-nosql\scripts\neo4j_network.py
python .\week5-nosql\scripts\influxdb_metrics.py
```

Chaque script :

1. Vérifie la connexion à sa base
2. Charge/normalise les données depuis `data/` (gestion des formats de
   date et de région hétérogènes du jeu de données source)
3. Exécute des requêtes analytiques spécifiques à la base
4. Affiche un résumé (volumes, débit, résultats)

## Interfaces web

| Base          | URL                   |
| ------------- | --------------------- |
| Neo4j Browser | http://localhost:7474 |
| InfluxDB UI   | http://localhost:8086 |

## Arrêt de l'environnement

```powershell
# Arrêt simple (conserve les données dans les volumes)
docker compose stop

# Arrêt et suppression des conteneurs (conserve les volumes/données)
docker compose down

# Suppression complète y compris les données (à utiliser en dernier recours)
docker compose down -v
```
