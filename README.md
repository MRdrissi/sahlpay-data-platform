# sahlpay-data-platform

Distributed data platform for real-time mobile payment analytics and fraud detection using Apache Kafka, Spark, Delta Lake, GraphX and GraphFrames.

Projet du module **Écosystème Big Data 2** — 5ème année EMSI, année universitaire 2026–2027.

---

## 1. Contexte

**SahlPay.ma** (entreprise fictive) est un portefeuille mobile : paiements chez les commerçants par QR code (**P2M**) et transferts d'argent entre particuliers (**P2P**). L'application de paiement (API Spring Boot + PostgreSQL) produit un événement à chaque changement d'état d'une transaction : `INITIATED`, `AUTHORIZED`, `COMPLETED`, `REJECTED`, `REVERSED`.

Cette plateforme est une **preuve de concept reproductible** qui :

- collecte les événements dans **Apache Kafka** ;
- les traite de manière distribuée avec **Apache Spark** (SparkCore, Spark SQL, DataFrames, Structured Streaming) ;
- calcule des indicateurs de volume, de taux de rejet et des métriques sur fenêtres temporelles ;
- conserve les résultats en **Parquet / Delta Lake** dans **MinIO** (couches Bronze, Silver, Gold) ;
- analyse les relations comptes–appareils–bénéficiaires avec **GraphX** et **GraphFrames** afin de repérer des structures inhabituelles (appareils partagés, transferts circulaires, comptes collecteurs), sans conclure automatiquement à la fraude.

Elle complète le système de paiement sans remplacer PostgreSQL.

---

## 2. Architecture

```mermaid
flowchart LR
    P[producer.py] -->|topic transactions<br/>3 partitions| K[(Kafka KRaft)]
    K --> S[Spark Structured Streaming<br/>Master + 2 Workers]
    M[merchants.json] -.-> S
    S --> B[Bronze<br/>JSON bruts]
    B --> SI[Silver<br/>Parquet nettoyé et enrichi]
    SI --> G[Gold<br/>KPI Delta Lake]
    SI --> GR[GraphX / GraphFrames]
    GR --> GA[Gold<br/>graph_analysis]
    G --> D[Tableau de bord conformité]
    GA --> D
```

### Organisation du bucket `lake` (MinIO)

```
lake/
├── bronze/
│   ├── history/               # transactions_history.jsonl (batch, Partie 2)
│   └── transactions_raw/      # JSON bruts lus dans Kafka (Partie 4)
├── silver/
│   ├── transactions_history/  # Parquet partitionné par city (Partie 3)
│   └── transactions_clean/    # Parquet partitionné par city et date (Partie 5)
├── gold/
│   ├── kpi_city_10min/        # Delta Lake, KPI fenêtre tumbling (Partie 5)
│   └── graph_analysis/        # Parquet partitionné par type d'anomalie (Partie 6)
└── checkpoints/               # checkpoints Structured Streaming
```

---

## 3. Stack technique

| Composant | Rôle | Version |
|---|---|---|
| Apache Spark (standalone) | Traitement distribué : 1 Master, 2 Workers (2 cœurs, 2 Go chacun) | 3.5.7 (Scala 2.12, Java 17) |
| Apache Kafka (mode KRaft) | Bus de messages, topic `transactions` | 3.9.1 |
| MinIO | Stockage objet compatible S3 (data lake) | RELEASE.2025-10-15 |
| JupyterLab + PySpark | Driver et développement des traitements | JupyterLab 4.4.10 |
| Delta Lake | Tables Gold transactionnelles | à configurer (US-3) |
| GraphFrames | Analyse de graphes en PySpark | à configurer (US-3) |
| GraphX (Scala) | Analyse de graphes en Scala | à configurer (US-3) |

Le détail des choix d'infrastructure est documenté dans [`docs/epics/EPIC-01-infrastructure.md`](docs/epics/EPIC-01-infrastructure.md).

---

## 4. Structure du dépôt

```
sahlpay-data-platform/
├── docker-compose.yaml        # Kafka, Spark Master + 2 Workers, MinIO, Jupyter
├── .env.example               # Modèle de configuration locale
├── infrastructure/
│   └── jupyter/Dockerfile     # Image Jupyter alignée sur le cluster Spark
├── producer/                  # producer.py, merchants.json
│   └── tests/                 # Tests automatisés du générateur
├── spark/
│   ├── batch/                 # Parties 2 et 3 : RDD, DataFrames, Spark SQL
│   └── streaming/             # Parties 4 et 5 : Structured Streaming, Delta Lake
├── graph/
│   ├── graphframes/           # Partie 6 : GraphFrames (PySpark)
│   └── graphx/                # Partie 6 : GraphX (Scala)
├── notebooks/                 # Notebooks Jupyter (montés dans le conteneur)
├── data/                      # Données locales (ignorées par Git)
├── docs/
│   ├── epics/                 # Documentation détaillée par epic
│   └── screenshots/           # Captures et preuves (EPIC-XX-US-XX_description.png)
├── services/                  # EPIC-07 : microservices et dashboard (bonus)
├── .gitattributes
├── .gitignore
└── README.md
```

---

## 5. Démarrage rapide

### Prérequis

- Git
- Docker Desktop (avec WSL 2 sous Windows)
- 16 Go de RAM recommandés

### Installation

```bash
git clone https://github.com/MRdrissi/sahlpay-data-platform.git
cd sahlpay-data-platform
git switch epic/01-infrastructure
cp .env.example .env          # puis remplacer le mot de passe MinIO
docker compose up -d --build
docker compose ps
```

### Interfaces

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark Worker 1 / Worker 2 | http://localhost:8081 · http://localhost:8082 |
| Spark Application UI | http://localhost:4040 (pendant l'exécution d'un job) |
| MinIO Console | http://localhost:9001 |
| JupyterLab | http://localhost:8888 (jeton : `docker compose logs jupyter`) |
| Kafka (depuis l'hôte) | `localhost:29092` (`kafka:9092` depuis les conteneurs) |

### Arrêt

```bash
docker compose down           # conserve les volumes (données MinIO et Kafka)
docker compose down -v        # supprime aussi les volumes
```

Le guide complet de déploiement et de vérification se trouve dans [`docs/epics/EPIC-01-infrastructure.md`](docs/epics/EPIC-01-infrastructure.md#11-guide-de-déploiement-et-de-vérification).

---

## 6. Organisation du travail

### Branches

| Branche | Rôle |
|---|---|
| `main` | Versions livrées (jalons) |
| `develop` | Intégration des epics validés (prévue) |
| `epic/<numéro>-<nom>` | Développement d'un epic (ex. `epic/01-infrastructure`) |

### Workflow d'une user story

```bash
git pull --ff-only origin epic/01-infrastructure    # avant de commencer
# ... développement de la user story ...
git status && git diff
git add <fichiers-concernes>
git commit -m "feat(US-X): description"            # un seul commit par user story
git pull --rebase origin epic/01-infrastructure     # avant de pousser
git push origin epic/01-infrastructure
```

### Jalons

| Jalon | Contenu | Échéance |
|---|---|---|
| Jalon 1 | Parties 1, 2, 3 (batch) | 22/11/2026 |
| Jalon 2 | Parties 4, 5, 6 (streaming, GraphX, GraphFrames) + soutenance | 20/12/2026 |

---

## 7. Documentation par epic

| Epic | Contenu | Document |
|---|---|---|
| EPIC-01 | Infrastructure | [EPIC-01-infrastructure.md](docs/epics/EPIC-01-infrastructure.md) |
| EPIC-02 | Spark RDD | à venir |
| EPIC-03 | Spark SQL et DataFrames | à venir |
| EPIC-04 | Structured Streaming | à venir |
| EPIC-05 | Pipeline de production | à venir |
| EPIC-06 | Analyse de graphes | à venir |

---

## 8. Équipe

- Marwane
- Mouad
- Zakaria
