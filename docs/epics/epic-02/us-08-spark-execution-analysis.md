# US-08 — Analyse de l'exécution Spark RDD

**EPIC-02 — Spark RDD**

## 1. Objectif et prérequis

Cette US analyse l'exécution distribuée des traitements de l'US-07 : transformations, actions, lineage, DAG, stages, tasks et shuffles.

**Prérequis :**
- Infrastructure Spark de l'US-03 opérationnelle.
- Historique Bronze de 50 000 événements chargé dans MinIO conformément aux [commandes de l'US-06](us-06-bronze-history.md#2-reproduire-la-procédure).
- Code RDD de l'[US-07](../../../spark/batch/us07_rdd.py).

Source : `s3a://lake/bronze/history/transactions_history.jsonl`

## 2. Reproduire les expériences

Depuis PowerShell, à la racine du dépôt :

```powershell
docker compose up -d --build minio spark-master spark-worker-1 spark-worker-2 jupyter
docker compose cp .\spark\batch jupyter:/tmp
```

Depuis JupyterLab (`http://localhost:8888`), ouvrir :

`notebooks/05_us08_spark_execution_analysis.ipynb`

Redémarrer le kernel, puis exécuter les cellules dans l'ordre. Le notebook utilise les fonctions de l'US-07, sans les modifier.

**Important :** `sc.addPyFile("/tmp/batch/us07_rdd.py")` distribue le module Python aux Executors. Un simple ajout à `sys.path` ne suffit pas sur les Workers.

Spark UI : `http://localhost:4040` (garder la session Spark active).

## 3. Transformations, actions et lineage — ST-08-01/02

| Opération | Catégorie | Effet |
|---|---|---|
| `map`, `filter`, `mapValues` | Narrow | Traitement local des partitions |
| `reduceByKey` | Wide | Agrégation par clé avec shuffle |
| `count`, `collect`, `takeOrdered` | Action | Déclenche un calcul Spark |
| `cache` | Persistance | Mémorise les partitions après matérialisation |

`toDebugString()` permet d'inspecter les dépendances RDD. Le lineage observé contient `HadoopRDD`, `PythonRDD` et `ShuffledRDD`.

**Validation :** 2 partitions mises en cache (2,3 MiB), 1 919 transactions P2M terminées à Marrakech, total de **4 755 142,96 MAD**.

## 4. DAG, Stages et Tasks — ST-08-03/04

Le Job d'agrégation observé dans Spark UI comporte :

| Mesure | Stage 1 | Stage 2 |
|---|---|---|
| Rôle | Préparation du shuffle | Agrégation finale |
| Tasks | 2/2 | 2/2 |
| Durée | 0,7 s | 0,8 s |
| Shuffle | Write : 236 B | Read : 236 B |

Le shuffle sépare les deux stages. Les Tasks d'un même stage peuvent s'exécuter en parallèle. Les Tasks observées ont été exécutées sur `spark-worker-1`.

Captures : [EPIC-02-US-08_dag_aggregation.png](../../screenshots/epic-02/1-EPIC-02-US-08_dag_aggregation.png) et [EPIC-02-US-08_stage2_tasks.png](../../screenshots/epic-02/2-EPIC-02-US-08_stage2_tasks.png).

## 5. Analyse des shuffles — ST-08-05

Expériences sur **32 123 événements P2M** :

| Traitement | Stages | Shuffle |
|---|---:|---:|
| `filter + map + count` | 1 | 0 B |
| `reduceByKey + collect` | 2 | 910 B |

Comparaison de deux agrégations donnant les mêmes totaux par ville :

| Mesure | reduceByKey | groupByKey |
|---|---:|---:|
| Shuffle Write | 838 B | 215,4 KiB |
| Shuffle Read | 838 B | 215,4 KiB |
| Durée Python | 1,800 s | 1,206 s |

`groupByKey()` a redistribué environ **263 fois plus de données**. `reduceByKey()` réduit le trafic grâce à l'agrégation locale.

Les durées ne constituent pas un benchmark : ces exécutions sont trop limitées pour conclure à la supériorité temporelle d'une opération.

## 6. Validation et réutilisation

**Vérifications effectuées :**
- Calculs et assertions du notebook : `VALIDATION OK`.
- Jobs Spark terminés avec `SUCCEEDED`.
- Lineage et DAG inspectés.
- Shuffle Read/Write mesurés.
- Résultats `reduceByKey` et `groupByKey` identiques.

Pour les US suivantes, réutiliser le notebook et la méthode d'analyse : identifier les actions, examiner le lineage, puis consulter les Jobs, Stages, Tasks et métriques Shuffle dans Spark UI.

Les identifiants de Jobs, les durées et les volumes de shuffle peuvent varier selon l'exécution, le partitionnement et l'environnement.