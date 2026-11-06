"""Batch and streaming transaction generators."""

import itertools
import random

from datetime import datetime, timezone
from typing import Callable, Iterator

from ..domain.events import make_event


def stream_events(
        merchants: list[dict],
        seed: int,
        clock: Callable[[], datetime] = lambda: datetime.now(
            timezone.utc
        ),
) -> Iterator[dict]:
    """Yield an infinite stream of simulated events."""

    rng = random.Random(seed)

    unique_index = 0
    previous = None

    for position in itertools.count(1):

        # Duplicate every 50th published event.
        if position % 50 == 0:
            yield dict(previous)
            continue

        # Delay every 20th unique event.
        late = (unique_index + 1) % 20 == 0

        previous = make_event(
            unique_index,
            rng,
            merchants,
            clock().astimezone(timezone.utc).replace(
                microsecond=0
            ),
            late,
        )

        unique_index += 1

        yield previous
