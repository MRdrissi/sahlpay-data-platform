"""US-07 : filtrer les transactions Bronze, calculer les montants par ville et classer les commerçants."""

import argparse
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from operator import add

from pyspark import RDD
from pyspark.sql import SparkSession


BRONZE_PATH = "s3a://lake/bronze/history/transactions_history.jsonl"


def decode_transaction(line: str) -> dict:
    transaction = json.loads(line, parse_float=Decimal)
    if not isinstance(transaction, dict):
        raise ValueError("Chaque ligne Bronze doit contenir un objet JSON.")
    return transaction


def filter_transactions(
    transactions: RDD,
    transaction_type: str | None = None,
    status: str | None = None,
    city: str | None = None,
) -> RDD:
    """Garder les événements qui satisfont tous les critères fournis."""
    if transaction_type is not None:
        transactions = transactions.filter(lambda event: event["type"] == transaction_type)
    if status is not None:
        transactions = transactions.filter(lambda event: event["status"] == status)
    if city is not None:
        city_key = city.casefold()
        transactions = transactions.filter(lambda event: event["city"].casefold() == city_key)
    return transactions


def city_metrics(total_and_count: tuple) -> tuple[int, Decimal, Decimal]:
    total, count = total_and_count
    total = Decimal(total)
    average = (total / count).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return count, total, average


def summarize_by_city(transactions: RDD) -> RDD:
    """Additionner les montants et les effectifs avant de calculer chaque moyenne."""
    by_city = transactions.map(lambda event: (event["city"], (event["amount_mad"], 1)))
    totals = by_city.reduceByKey(
        lambda left, right: (left[0] + right[0], left[1] + right[1])
    )
    return totals.mapValues(city_metrics)


def top_five_merchants(transactions: RDD) -> list[tuple[str, Decimal]]:
    """Classer par total P2M COMPLETED décroissant, puis par identifiant croissant."""
    payments = filter_transactions(transactions, transaction_type="P2M", status="COMPLETED")
    by_merchant = payments.map(lambda event: (event["payee_id"], Decimal(event["amount_mad"])))
    totals = by_merchant.reduceByKey(add)
    return totals.takeOrdered(5, key=lambda item: (-item[1], item[0]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--type", dest="transaction_type", type=str.upper, choices=("P2M", "P2P")
    )
    parser.add_argument("--status", type=str.upper, help="Statut à retenir, par exemple COMPLETED.")
    parser.add_argument("--city", type=str.strip, help="Ville à retenir, par exemple Marrakech.")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("SahlPay-US07-RDD")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .getOrCreate()
    )
    try:
        lines = spark.sparkContext.textFile(BRONZE_PATH)
        transactions = lines.map(decode_transaction).cache()

        total = transactions.count()
        print(f"Source Bronze : {BRONZE_PATH}")
        print(f"Transactions décodées : {total}")

        selected = filter_transactions(
            transactions, args.transaction_type, args.status, args.city
        )
        criteria = (("type", args.transaction_type), ("statut", args.status), ("ville", args.city))
        labels = [f"{field}={value}" for field, value in criteria if value is not None]
        if labels:
            selected.cache()
            count = selected.count()
            print("Filtres : " + " | ".join(labels))
            print(f"Transactions retenues : {count}")

        sample = selected.take(1)
        if sample:
            event = sample[0]
            print(f"Exemple décodé ({len(event)} champs) : {event['tx_id']}")
            print(
                f"  {event['type']} | {event['status']} | {event['payee_id']}"
                f" | {event['city']} | {event['amount_mad']:.2f} MAD"
            )

        cities = sorted(summarize_by_city(selected).collect())
        print("Montants par ville")
        if cities:
            print(f"{'Ville':<14} | {'Événements':>10} | {'Total (MAD)':>14} | {'Moyenne (MAD)':>13}")
            for city, (count, total_mad, average_mad) in cities:
                print(f"{city:<14} | {count:>10} | {total_mad:>14.2f} | {average_mad:>13.2f}")
        else:
            print("Aucune ville pour cette sélection.")

        merchants = top_five_merchants(selected)
        print("Top 5 des commerçants (P2M COMPLETED)")
        if merchants:
            print(f"{'Rang':<4} | {'Commerçant':<12} | {'Total (MAD)':>14}")
            for rank, (merchant_id, total_mad) in enumerate(merchants, start=1):
                print(f"{rank:<4} | {merchant_id:<12} | {total_mad:>14.2f}")
        else:
            print("Aucun paiement P2M COMPLETED pour cette sélection.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
