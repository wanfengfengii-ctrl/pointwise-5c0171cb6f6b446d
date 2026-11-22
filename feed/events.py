import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from contracts import InvalidEvent


class EventKind(StrEnum):
    WRITE = 'write'
    DELETE = 'delete'
    SAVEPOINT = 'savepoint'
    ROLLBACK_TO = 'rollback_to'
    COMMIT = 'commit'
    ABORT = 'abort'


@dataclass(frozen=True)
class SourceEvent:
    source_id: str
    number: int
    transaction_id: str
    kind: EventKind | str
    day: str
    station: str | None = None
    point: str | None = None
    reading: int | None = None
    savepoint: str | None = None


def load_events(path: str | Path) -> tuple[SourceEvent, ...]:
    with Path(path).open(encoding='utf-8') as source:
        try:
            records = json.load(source)
        except ValueError as error:
            raise InvalidEvent(f'invalid event file {path}') from error
    if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
        raise InvalidEvent('event file must contain an array of event objects')
    try:
        return tuple(SourceEvent(**record) for record in records)
    except TypeError as error:
        raise InvalidEvent(f'invalid fields in event file {path}') from error
