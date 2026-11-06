"""Write generated transactions to a JSONL file."""

import json
from pathlib import Path


def write_file(
        output: Path,
        events: list[dict],
) -> None:

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output.open(
            "w",
            encoding="utf-8",
            newline="\n",
    ) as file:

        for event in events:
            file.write(
                json.dumps(
                    event,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ) + "\n"
            )
