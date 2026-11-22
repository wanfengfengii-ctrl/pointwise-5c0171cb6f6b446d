from dataclasses import dataclass

from contracts import InvalidEvent, ReadingKey, valid_day, valid_name, valid_number
from feed.events import EventKind, SourceEvent


@dataclass(frozen=True)
class DecodedEvent:
    source_id: str
    number: int
    transaction_id: str
    kind: EventKind
    day: str
    key: ReadingKey | None = None
    reading: int | None = None
    savepoint: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.kind in (EventKind.COMMIT, EventKind.ABORT)


def decode_event(event: SourceEvent) -> DecodedEvent:
    if not isinstance(event, SourceEvent):
        raise InvalidEvent('expected a source event')
    if (not valid_name(event.source_id) or not valid_name(event.transaction_id)
            or not valid_number(event.number) or not valid_day(event.day)):
        raise InvalidEvent(f'invalid identity or date on event {event.number!r}')
    if not isinstance(event.kind, str):
        raise InvalidEvent(f'invalid event kind at {event.number}')
    try:
        kind = EventKind(event.kind)
    except ValueError as error:
        raise InvalidEvent(f'unknown event kind at {event.number}') from error

    key = None
    if kind in (EventKind.WRITE, EventKind.DELETE):
        if not valid_name(event.station) or not valid_name(event.point) or event.savepoint is not None:
            raise InvalidEvent(f'invalid measurement key or fields at {event.number}')
        if event.reading is not None and (kind == EventKind.DELETE or type(event.reading) is not int):
            raise InvalidEvent(f'invalid reading at {event.number}')
        key = ReadingKey(event.day, event.station, event.point)
    else:
        if any(value is not None for value in (event.station, event.point, event.reading)):
            raise InvalidEvent(f'unexpected measurement fields at {event.number}')
        if kind in (EventKind.SAVEPOINT, EventKind.ROLLBACK_TO):
            if not valid_name(event.savepoint):
                raise InvalidEvent(f'invalid savepoint name at {event.number}')
        elif event.savepoint is not None:
            raise InvalidEvent(f'unexpected savepoint name at {event.number}')

    return DecodedEvent(event.source_id, event.number, event.transaction_id, kind,
                        event.day, key, event.reading, event.savepoint)
