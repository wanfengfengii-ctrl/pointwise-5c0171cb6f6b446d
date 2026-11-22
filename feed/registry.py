from collections.abc import Iterable
from dataclasses import dataclass, field

from contracts import InvalidEvent, UnknownSource, valid_name, valid_number
from feed.events import SourceEvent


@dataclass
class SourceStream:
    source_id: str
    events: tuple[SourceEvent, ...]
    acknowledged_through: int = field(default=0, init=False)
    discarded_through: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if not valid_name(self.source_id):
            raise InvalidEvent('invalid source identity')
        self.events = tuple(self.events)
        for number, event in enumerate(self.events, 1):
            if (not isinstance(event, SourceEvent) or event.source_id != self.source_id
                    or not valid_number(event.number) or event.number != number):
                raise InvalidEvent(f'source {self.source_id} needs event {number}')

    @property
    def last_number(self) -> int:
        return self.discarded_through + len(self.events)


class SourceRegistry:
    def __init__(self) -> None:
        self._sources: dict[str, SourceStream] = {}

    def register(self, source_id: str, events: Iterable[SourceEvent]) -> SourceStream:
        if valid_name(source_id) and source_id in self._sources:
            raise InvalidEvent(f'source already registered: {source_id}')
        source = SourceStream(source_id, tuple(events))
        self._sources[source_id] = source
        return source

    def get(self, source_id: str) -> SourceStream:
        if not valid_name(source_id) or source_id not in self._sources:
            raise UnknownSource(f'unknown source: {source_id!r}')
        return self._sources[source_id]
