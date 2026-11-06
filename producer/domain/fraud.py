"""Inject known suspicious transaction patterns for graph analysis."""

import random


def fraud_overrides(
        index: int,
        rng: random.Random,
        merchant: dict,
) -> dict:
    """Inject a suspicious pattern every 5,000 unique events."""

    slot = index % 5000

    # Scenario 1: one device shared by 16 wallets.
    if slot < 16:
        return {
            "payer_wallet": f"W-{88001 + slot}",
            "device_id": "D-FRAUD-SHARED-000",
            "type": "P2M",
            "payee_id": merchant["merchant_id"],
            "city": merchant["city"],
            "status": "COMPLETED",
        }

    # Scenario 2: circular transfers A -> B -> C -> A.
    elif slot < 40:
        circle = ("W-88101", "W-88102", "W-88103")
        step = (slot - 16) % 3

        payer = circle[step]
        payee = circle[(step + 1) % 3]

        return {
            "payer_wallet": payer,
            "payee_id": payee,
            "device_id": "D-" + payer.split("-")[1],
            "type": "P2P",
            "status": "COMPLETED",
            "amount_mad": 120.0,
            "city": "Rabat",
        }

    # Scenario 3: many small transfers to one collector.
    elif slot < 80:
        payer = f"W-{88200 + slot - 40}"

        return {
            "payer_wallet": payer,
            "payee_id": "W-88999",
            "device_id": "D-" + payer.split("-")[1],
            "type": "P2P",
            "status": "COMPLETED",
            "amount_mad": round(rng.uniform(5, 30), 2),
            "city": "Rabat",
        }

    return {}
