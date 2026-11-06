"""Load and validate the merchant reference dataset."""

import json
from pathlib import Path

from ..config import MERCHANT_COUNT
from ..domain.merchants import create_merchants


def ensure_merchants(path: Path, seed: int) -> list[dict]:

    if not path.exists():
        merchants = create_merchants(seed)

        path.parent.mkdir(parents=True, exist_ok=True)

        path.write_text(
            json.dumps(
                merchants,
                ensure_ascii=False,
                indent=2,
            ) + "\n",
            encoding="utf-8",
            )
    else:
        merchants = json.loads(
            path.read_text(encoding="utf-8")
        )

    required = {
        "merchant_id",
        "name",
        "city",
        "category",
        "latitude",
        "longitude",
    }

    if (
            not isinstance(merchants, list)
            or len(merchants) != MERCHANT_COUNT
            or not all(isinstance(m, dict) and required <= m.keys() for m in merchants)
            or len({m["merchant_id"] for m in merchants}) != MERCHANT_COUNT
    ):
        raise ValueError(
            f"{path} doit contenir exactement "
            f"{MERCHANT_COUNT} merchants valides et uniques"
        )

    return merchants