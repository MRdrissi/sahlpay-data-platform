"""CLI orchestrator for the transaction producer."""

import argparse

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .config import BATCH_DURATION_SECONDS, TOPIC
from .adapters.file_output import write_file
from .generation.batch import make_batch
from .adapters.kafka_output import publish_kafka
from .adapters.repository import ensure_merchants
from .utils import to_utc


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Simulateur de transactions",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--mode",
        choices=("file", "kafka"),
        required=True,
        help="Mode de production : fichier JSONL ou publication Kafka",
    )

    parser.add_argument(
        "--n",
        type=int,
        default=50000,
        help="Nombre total d'événements à générer en mode File, doublons inclus",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/transactions_history.jsonl"),
        help="Chemin du fichier JSONL de sortie en mode File",
    )

    parser.add_argument(
        "--merchants",
        type=Path,
        default=Path("data/merchants.json"),
        help="Chemin du référentiel JSON contenant les 300 commerçants",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Graine du générateur pseudo-aléatoire pour la reproductibilité",
    )

    parser.add_argument(
        "--start-time",
        type=to_utc,
        help=(
            "Début des arrivées simulées en mode File, "
            "au format ISO 8601 avec fuseau horaire"
        ),
    )

    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=BATCH_DURATION_SECONDS,
        help="Durée de l'historique simulé en mode File, en secondes",
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:29092",
        help="Adresses des brokers Kafka, séparées par des virgules",
    )

    parser.add_argument(
        "--topic",
        default=TOPIC,
        help="Nom du topic Kafka dans lequel publier les transactions",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=20.0,
        help="Débit cible de publication en mode Kafka (événements par seconde)",
    )

    parser.add_argument(
        "--max-events",
        type=int,
        help="Nombre maximal d'événements à publier en mode Kafka ; illimité si absent",
    )

    args = parser.parse_args(argv)

    if args.mode == "file":
        if args.n <= 0:
            parser.error("--n doit être strictement positif")

        if args.duration_seconds <= 0:
            parser.error("--duration-seconds doit être strictement positif")

    if args.mode == "kafka" and (
            args.rate <= 0
            or (args.max_events is not None and args.max_events <= 0)
    ):
        parser.error(
            "--rate et --max-events doivent etre strictement positifs"
        )

    merchants = ensure_merchants(
        args.merchants,
        args.seed,
    )

    if args.mode == "file":

        start_time = args.start_time

        if start_time is None:
            start_time = (
                    datetime.now(timezone.utc).replace(microsecond=0)
                    - timedelta(seconds=args.duration_seconds)
            )

        events = make_batch(
            args.n,
            merchants,
            args.seed,
            start_time,
            args.duration_seconds,
        )

        write_file(
            args.output,
            events,
        )

        print(
            f"Mode file: {len(events)} evenements "
            f"ecrits dans {args.output}"
        )

    else:

        sent = publish_kafka(
            merchants,
            args.seed,
            args.bootstrap_servers,
            args.topic,
            args.rate,
            args.max_events,
        )

        print(
            f"Mode kafka: {sent} evenements "
            f"confirmes dans {args.topic}"
        )

    return 0
