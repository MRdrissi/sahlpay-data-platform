"""Validate a generated JSONL history."""

import argparse
import json
import math

from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from ..config import CHANNELS, CITIES, STATUSES


FIELDS = {
    "tx_id",
    "type",
    "payer_wallet",
    "payee_id",
    "device_id",
    "city",
    "channel",
    "status",
    "amount_mad",
    "event_time",
}


def validate(path: Path, expected_count: int) -> None:
    if not path.is_file():
        raise FileNotFoundError(path)

    count = 0
    pairs = Counter()
    types = Counter()
    statuses = Counter()

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Ligne {line_number}: JSON invalide"
                ) from exc

            if not isinstance(event, dict):
                raise ValueError(
                    f"Ligne {line_number}: objet JSON attendu"
                )

            if set(event) != FIELDS:
                raise ValueError(
                    f"Ligne {line_number}: schema incorrect"
                )

            for field in FIELDS - {"amount_mad"}:
                value = event[field]
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"Ligne {line_number}: {field} invalide"
                    )

            if event["type"] not in ("P2M", "P2P"):
                raise ValueError(
                    f"Ligne {line_number}: type inconnu"
                )

            if event["status"] not in STATUSES:
                raise ValueError(
                    f"Ligne {line_number}: statut inconnu"
                )

            if event["channel"] not in CHANNELS:
                raise ValueError(
                    f"Ligne {line_number}: canal inconnu"
                )

            if event["city"] not in CITIES:
                raise ValueError(
                    f"Ligne {line_number}: ville inconnue"
                )

            amount = event["amount_mad"]
            if (
                    type(amount) not in (int, float)
                    or not math.isfinite(amount)
                    or amount <= 0
            ):
                raise ValueError(
                    f"Ligne {line_number}: montant invalide"
                )

            if event["type"] == "P2M":
                expected_prefix = "M-"
            else:
                expected_prefix = "W-"

            if not event["payee_id"].startswith(expected_prefix):
                raise ValueError(
                    f"Ligne {line_number}: beneficiaire invalide"
                )

            event_time = event["event_time"]

            try:
                parsed = datetime.fromisoformat(
                    event_time.replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise ValueError(
                    f"Ligne {line_number}: date invalide"
                ) from exc

            if parsed.utcoffset() != timedelta(0):
                raise ValueError(
                    f"Ligne {line_number}: date non UTC"
                )

            pairs[(event["tx_id"], event["status"])] += 1
            types[event["type"]] += 1
            statuses[event["status"]] += 1
            count += 1

    if count != expected_count:
        raise ValueError(
            f"Nombre incorrect : {count} au lieu de {expected_count}"
        )

    duplicates = sum(n - 1 for n in pairs.values())

    print("VALIDATION OK")
    print(f"Evenements : {count}")
    print(f"Identifiants uniques : "
          f"{len({tx_id for tx_id, _ in pairs})}")
    print(f"Doublons (tx_id, status) : {duplicates}")
    print(f"Types : {dict(types)}")
    print(f"Statuts : {dict(statuses)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validation de l'historique JSONL SahlPay"
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/transactions_history.jsonl"),
        help="Fichier JSONL à valider",
    )

    parser.add_argument(
        "--expected-count",
        type=int,
        default=50000,
        help="Nombre exact d'événements attendu",
    )

    args = parser.parse_args()

    if args.expected_count <= 0:
        parser.error("--expected-count doit être positif")

    validate(args.input, args.expected_count)


if __name__ == "__main__":
    main()
