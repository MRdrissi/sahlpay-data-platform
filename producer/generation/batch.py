"""Generate a batch following a simulated arrival timeline."""

import random
from datetime import datetime, timedelta, timezone

from ..config import (
    BATCH_DURATION_SECONDS,
    DUPLICATE_RATIO,
    LATE_RATIO,
)
from ..domain.events import make_event


def make_batch(
        n: int,
        merchants: list[dict],
        seed: int,
        start_time: datetime,
        duration_seconds: int = BATCH_DURATION_SECONDS,
) -> list[dict]:
    """Generate n events over a simulated arrival period."""

    if n <= 0:
        raise ValueError("Le nombre doit être strictement positif")

    if duration_seconds <= 0:
        raise ValueError("La durée doit être strictement positive")

    if start_time.tzinfo is None or start_time.utcoffset() is None:
        raise ValueError("La date doit contenir un fuseau horaire")

    start_time = start_time.astimezone(timezone.utc).replace(
        microsecond=0
    )

    rng = random.Random(seed)

    duplicate_count = round(n * DUPLICATE_RATIO)
    unique_count = n - duplicate_count

    late_indices = set(
        rng.sample(
            range(unique_count),
            round(unique_count * LATE_RATIO),
        )
    )

    duplicate_indices = set(
        rng.sample(
            range(unique_count),
            duplicate_count,
        )
    )

    events = []

    for index in range(unique_count):
        # Répartir les arrivées sur la période.
        # Plusieurs événements peuvent arriver pendant la même seconde.
        offset_seconds = (
                index * duration_seconds // max(unique_count - 1, 1)
        )

        arrival_time = start_time + timedelta(
            seconds=offset_seconds
        )

        event = make_event(
            index,
            rng,
            merchants,
            arrival_time,
            late=index in late_indices,
        )

        events.append(event)

        # Une copie conserve les données et l'instant d'arrivée simulé
        # de son original.
        if index in duplicate_indices:
            events.append(dict(event))

    return events