"""Generate the merchant reference dataset."""

import random

from ..config import CATEGORIES, CITIES, MERCHANT_COUNT


def create_merchants(
        seed: int,
        count: int = MERCHANT_COUNT,
) -> list[dict]:

    rng = random.Random(seed)
    locations = list(CITIES.items())
    merchants = []

    for i in range(count):
        city, (latitude, longitude) = locations[i % len(locations)]

        merchants.append({
            "merchant_id": f"M-{i + 1:03d}",
            "name": f"Merchant {i + 1:03d}",
            "city": city,
            "category": CATEGORIES[i % len(CATEGORIES)],
            "latitude": round(
                latitude + rng.uniform(-0.045, 0.045), 5
            ),
            "longitude": round(
                longitude + rng.uniform(-0.045, 0.045), 5
            ),
        })

    return merchants

