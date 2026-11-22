from dataclasses import dataclass

from contracts import InvalidEvent, ReadingKey
from feed.events import EventKind
from intake.decoder import DecodedEvent


@dataclass(frozen=True)
class ReadingOperation:
    source_id: str
    number: int
    transaction_id: str
    kind: EventKind
    key: ReadingKey
    reading: int | None


def to_operation(event: DecodedEvent) -> ReadingOperation:
    if event.kind not in (EventKind.WRITE, EventKind.DELETE) or event.key is None:
        raise InvalidEvent(f'event {event.number} is not a reading operation')
    return ReadingOperation(event.source_id, event.number, event.transaction_id,
                            event.kind, event.key, event.reading)
