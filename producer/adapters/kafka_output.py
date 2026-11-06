"""Publish generated transaction events to Apache Kafka."""

import json
import time

from datetime import datetime, timezone
from typing import Callable, Optional

from ..generation.streaming import stream_events


def publish_kafka(
        merchants: list[dict],
        seed: int,
        bootstrap_servers: str,
        topic: str,
        rate: float,
        max_events: Optional[int] = None,
        producer_factory=None,
        clock: Callable[[], datetime] = lambda: datetime.now(
            timezone.utc
        ),
) -> int:
    if rate <= 0:
        raise ValueError("Le rate doit etre strictement positif")

    if max_events is not None and max_events <= 0:
        raise ValueError(
            "Le 'max events' doit etre strictement positif"
        )

    if producer_factory is None:
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "Installer les dependances: "
                "pip install -r producer/requirements.txt"
            ) from exc

        producer_factory = KafkaProducer

    producer = producer_factory(
        bootstrap_servers=bootstrap_servers.split(","),
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda payload: json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8"),
    )

    start = time.monotonic()
    sent = 0

    try:
        events = stream_events(
            merchants,
            seed,
            clock,
        )

        for event in events:

            if max_events is not None and sent >= max_events:
                break

            remaining = (
                    start
                    + sent / rate
                    - time.monotonic()
            )

            if remaining > 0:
                time.sleep(remaining)

            producer.send(
                topic,
                key=event["tx_id"],
                value=event,
            ).get(timeout=30)

            sent += 1

    except KeyboardInterrupt:
        print(
            f"Arret demande apres {sent} evenements publies."
        )

    finally:
        producer.flush(timeout=30)
        producer.close()

    return sent
