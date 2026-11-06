# Producer — US-02

Générateur de transactions simulées SahlPay : paiements commerçants (P2M) et transferts entre particuliers (P2P).

- **File** : 50 000 événements JSONL par défaut, avec des arrivées réparties sur les quatre dernières heures.
- **Kafka** : publication continue dans le topic `transactions`, à un débit configurable de 20 événements/s par défaut.
- Cinq statuts : `INITIATED`, `AUTHORIZED`, `COMPLETED`, `REJECTED`, `REVERSED`.
- Environ 5 % d'événements retardés jusqu'à 15 minutes et 2 % de doublons de même `tx_id` et `status`.
- Trois scénarios de fraude potentielle : appareils partagés, transferts circulaires et comptes collecteurs.
- Référentiel de 300 commerçants, créé automatiquement s'il est absent.

Chaque événement contient : `tx_id`, `type`, `payer_wallet`, `payee_id`, `device_id`, `city`, `channel`, `status`, `amount_mad` et `event_time` (UTC).

## Installation

Prérequis : Python 3.11 et Docker Desktop pour Kafka. Exécuter les commandes dans **PowerShell, depuis la racine du dépôt**.

```powershell
# Créer l'environnement uniquement s'il n'existe pas encore
py -3.11 -m venv producer/.venv

& .\producer\.venv\Scripts\python.exe -m pip install -r producer/requirements.txt
```

## Tests métier

```powershell
& .\producer\.venv\Scripts\python.exe -m unittest -v producer.tests.test_producer
```

Les tests vérifient le fichier de 50 000 événements, le schéma, les types et statuts, les retards, les doublons, les scénarios simulés et le référentiel commerçants. Ils ne nécessitent pas Kafka ; l'échange réel se vérifie avec les commandes ci-dessous.

## Mode File

```powershell
& .\producer\.venv\Scripts\python.exe -m producer --mode file

# Vérifier le nombre de lignes et afficher un exemple
(Get-Content data/transactions_history.jsonl | Measure-Object -Line).Lines
Get-Content data/transactions_history.jsonl -TotalCount 1
```

Le résultat est écrit dans `data/transactions_history.jsonl` (remplacé s'il existe). Le référentiel est conservé dans `data/merchants.json`.

Sans `--start-time`, le début correspond à l'heure UTC du lancement moins quatre heures. Les événements retardés peuvent être antérieurs à ce début. La génération n'attend pas quatre heures.

Pour choisir la période :

```powershell
& .\producer\.venv\Scripts\python.exe -m producer --mode file --start-time "2026-11-06T08:00:00Z" --duration-seconds 14400
```

## Mode Kafka

Démarrer Docker Desktop, puis Kafka :

```powershell
docker compose up -d kafka

# Attendre que cette commande réussisse avant de continuer
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Créer le topic local s'il est absent
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic transactions --partitions 1 --replication-factor 1
```

Dans un premier terminal, écouter les nouveaux messages :

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic transactions
```

Dans un second terminal, depuis la racine du dépôt, lancer le producteur :

```powershell
& .\producer\.venv\Scripts\python.exe -m producer --mode kafka --bootstrap-servers localhost:29092 --rate 20
```

Arrêter avec `Ctrl+C`. Pour un essai limité, ajouter `--max-events 200` à la commande du producteur.

## Options utiles

`--n` règle la taille du lot File ; `--output` son chemin ; `--merchants` le chemin du référentiel ; `--seed` la graine ; `--topic` le topic Kafka.

```powershell
& .\producer\.venv\Scripts\python.exe -m producer --help
```
