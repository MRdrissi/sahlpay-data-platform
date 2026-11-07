# US-06 — Génération et stockage des transactions Bronze

**EPIC-02 — Spark RDD**

## 1. Objectif et réalisation

Cette US prépare un historique de **50 000 transactions simulées** pour les traitements Spark des US suivantes.

**Pipeline :**

`producer.py → JSONL → Validation → MinIO Bronze → Spark S3A`

- **Génération :** 50 000 événements JSONL, dont 1 000 doublons intentionnels.
- **Validation :** vérification des 10 champs, des types, des valeurs métier et des dates UTC.
- **Stockage :** dépôt du fichier brut dans MinIO, sans nettoyage ni suppression des doublons.
- **Lecture Spark :** vérification de l'accès via S3A et du comptage des 50 000 lignes.

**Chemin Bronze :**

`s3a://lake/bronze/history/transactions_history.jsonl`

## 2. Reproduire la procédure

Toutes les commandes PowerShell s'exécutent depuis la racine du dépôt.

### A. Préparer l'environnement

Créer `.env` depuis `.env.example` et configurer `MINIO_ROOT_USER` et `MINIO_ROOT_PASSWORD`.

```powershell
# Uniquement si l'environnement virtuel n'existe pas
py -3.11 -m venv producer/.venv

& .\producer\.venv\Scripts\python.exe -m pip install -r producer/requirements.txt

docker compose up -d --build minio spark-master spark-worker-1 spark-worker-2 jupyter
```

### B. Générer et valider les données

**Génération reproductible :**

```powershell
& .\producer\.venv\Scripts\python.exe -m producer `
  --mode file `
  --n 50000 `
  --output "data/transactions_history.jsonl" `
  --merchants "data/merchants.json" `
  --seed 42 `
  --start-time "2026-11-07T08:00:00Z" `
  --duration-seconds 14400
```

**Important :** `data/merchants.json` contient le référentiel des 300 commerçants. Le générateur le crée automatiquement avec la graine `42` s'il n'existe pas, puis le réutilise lors des exécutions suivantes.

Depuis un clone neuf, la commande suffit pour générer les deux fichiers. Si un ancien `merchants.json` a été généré avec d'autres paramètres, il faut le sauvegarder puis le supprimer avant de relancer cette commande pour obtenir le jeu de données de référence.

**Validation :**

```powershell
& .\producer\.venv\Scripts\python.exe -m producer.validation.validate_history `
  --input data/transactions_history.jsonl `
  --expected-count 50000

& .\producer\.venv\Scripts\python.exe -m unittest -v producer.tests.test_producer
```

**Résultat attendu :** `VALIDATION OK`, 50 000 événements, 49 000 identifiants uniques et 1 000 doublons.

### C. Charger le fichier dans MinIO

```powershell
docker compose exec minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"'

docker compose exec minio mc mb --ignore-existing local/lake

docker compose cp data/transactions_history.jsonl minio:/tmp/transactions_history.jsonl

docker compose exec minio mc cp `
  /tmp/transactions_history.jsonl `
  local/lake/bronze/history/transactions_history.jsonl

docker compose exec minio mc stat `
  local/lake/bronze/history/transactions_history.jsonl
```

**Résultat attendu :** l'objet `transactions_history.jsonl` est présent dans `lake/bronze/history/`.

### D. Vérifier la lecture depuis Spark

Dans JupyterLab (`http://localhost:8888`), ouvrir :

`notebooks/04_us06_bronze_validation.ipynb`

Le notebook initialise une SparkSession distribuée avec les identifiants MinIO fournis par Docker Compose.

**Vérification principale :**

```python
path = "s3a://lake/bronze/history/transactions_history.jsonl"

df = spark.read.text(path)

assert df.count() == 50000

print("VALIDATION OK : 50000 événements")
```

Arrêter la session après validation avec `spark.stop()`.

## 3. Utilisation par les US suivantes

- **US-07 — Spark RDD :** lire les transactions brutes depuis `s3a://lake/bronze/history/transactions_history.jsonl`.
- **US-09 — Spark DataFrames :** lire le même fichier avec `spark.read.json()` et un schéma explicite.

Les doublons et événements retardés sont volontairement conservés dans Bronze.

**Important :** les fichiers `data/*.jsonl` et `data/merchants.json` ne sont pas versionnés sur GitHub. Chaque collaborateur doit exécuter la procédure et charger les données dans son propre MinIO local.

## 4. Critères de validation

- [x] ST-06-01 — 50 000 événements générés
- [x] ST-06-02 — JSONL et schéma validés
- [x] ST-06-03 — Données stockées dans MinIO et intégrité SHA-256 vérifiée
- [x] ST-06-04 — Lecture Spark via S3A et comptage de 50 000 lignes

**Livrables :** validateur Python, notebook Spark et documentation de reproductibilité.