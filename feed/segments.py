from dataclasses import dataclass

from contracts import InvalidRange, valid_name
from feed.events import SourceEvent
from feed.registry import SourceRegistry
from feed.retention import require_readable


@dataclass(frozen=True)
class SourceSegment:
    source_id: str
    segment_id: str
    start: int
    end: int
    events: tuple[SourceEvent, ...]


def segment(registry: SourceRegistry, source_id: str, start: int, end: int,
            segment_id: str | None = None) -> SourceSegment:
    stream = registry.get(source_id)
    require_readable(stream, start, end)
    identity = f'{start}:{end}' if segment_id is None else segment_id
    if not valid_name(identity):
        raise InvalidRange('segment identity must be nonempty text')
    offset = stream.discarded_through
    events = stream.events[start - offset - 1:end - offset]
    return SourceSegment(source_id, identity, start, end, events)
