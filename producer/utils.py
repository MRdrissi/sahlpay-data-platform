"""UTC timestamp conversion utilities."""

from datetime import datetime, timezone


def to_utc(value: str) -> datetime:

    parsed = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        raise ValueError(
            "La date doit contenir un fuseau horaire explicite"
        )

    return parsed.astimezone(timezone.utc).replace(
        microsecond=0
    )


def timestamp(value: datetime) -> str:

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            "La date doit contenir un fuseau horaire explicite"
        )

    return value.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
