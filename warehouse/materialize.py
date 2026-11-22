from collections.abc import Mapping
from dataclasses import dataclass

from contracts import ReadingKey
from feed.events import EventKind
from staging.operations import ReadingOperation


@dataclass(frozen=True)
class MaterializedLedger:
    rows: dict[ReadingKey, int | None]
    changes: tuple[ReadingOperation, ...]


def materialize(previous: Mapping[ReadingKey, int | None],
                operations: tuple[ReadingOperation, ...]) -> MaterializedLedger:
    rows = dict(previous)
    for operation in operations:
        if operation.kind == EventKind.DELETE:
            rows.pop(operation.key, None)
        else:
            rows[operation.key] = operation.reading
    return MaterializedLedger(rows, operations)
