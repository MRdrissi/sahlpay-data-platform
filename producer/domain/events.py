"""Create individual simulated payment events."""

import random
from datetime import datetime, timedelta

from ..config import (
    CHANNELS,
    CITIES,
    MAX_DELAY_SECONDS,
    STATUSES,
)
from .fraud import fraud_overrides
from ..utils import timestamp


def make_event(
        index: int,
        rng: random.Random,
        merchants: list[dict],
        arrival_time: datetime,
        late: bool = False,
) -> dict:
    """Create one unique event with optional lateness."""

    payer = f"W-{rng.randrange(10000, 20000)}"
    device = "D-" + payer.split("-")[1]

    merchant = rng.choice(merchants)

    tx_type = "P2M" if rng.random() < 0.65 else "P2P"

    if tx_type == "P2M":
        payee = merchant["merchant_id"]
        city = merchant["city"]
    else:
        payee = f"W-{rng.randrange(10000, 20000)}"

        if payee == payer:
            payee = "W-20000"

        city = rng.choice(tuple(CITIES))

    status = rng.choices(
        STATUSES,
        weights=(10, 12, 58, 16, 4),
        k=1,
    )[0]

    amount = round(rng.uniform(5, 5000), 2)

    event = {
        "type": tx_type,
        "payer_wallet": payer,
        "payee_id": payee,
        "device_id": device,
        "city": city,
        "status": status,
        "amount_mad": amount,
    }

    # Apply known suspicious patterns.
    event.update(
        fraud_overrides(index, rng, merchant)
    )

    # Simulate out-of-order / late event timestamps.
    if late:
        event_time = arrival_time - timedelta(
            seconds=rng.randint(1, MAX_DELAY_SECONDS)
        )
    else:
        event_time = arrival_time

    return {
        "tx_id": f"TX-{index + 1:010d}",
        "type": event["type"],
        "payer_wallet": event["payer_wallet"],
        "payee_id": event["payee_id"],
        "device_id": event["device_id"],
        "city": event["city"],
        "channel": rng.choice(CHANNELS),
        "status": event["status"],
        "amount_mad": event["amount_mad"],
        "event_time": timestamp(event_time),
    }
