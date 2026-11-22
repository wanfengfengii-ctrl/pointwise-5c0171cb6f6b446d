from sqlite3 import Connection

from contracts import InvalidEvent, InvalidRange, SegmentGap, valid_name, valid_number
from feed.events import SourceEvent
from feed.segments import SourceSegment


def get_frontier(db: Connection, source_id: str) -> int:
    row = db.execute('SELECT event_number FROM frontiers WHERE source_id = ?', (source_id,)).fetchone()
    return row[0] if row else 0


def check_continuity(batch: SourceSegment) -> None:
    if not isinstance(batch, SourceSegment):
        raise InvalidRange('expected a source segment')
    if not valid_name(batch.source_id) or not valid_name(batch.segment_id):
        raise InvalidRange('source and segment identities must be nonempty text')
    if (not valid_number(batch.start) or not valid_number(batch.end) or batch.start > batch.end
            or not isinstance(batch.events, tuple)):
        raise InvalidRange('invalid segment bounds or event sequence')
    if len(batch.events) != batch.end - batch.start + 1:
        raise SegmentGap('event count does not cover the declared range')
    for number, event in enumerate(batch.events, batch.start):
        if not isinstance(event, SourceEvent) or event.source_id != batch.source_id:
            raise InvalidEvent(f'expected an event from {batch.source_id} at position {number}')
        if not valid_number(event.number):
            raise InvalidEvent(f'invalid event identity at position {number}')
        if event.number != number:
            raise SegmentGap(f'expected event {number}, received {event.number}')


def advance_frontier(db: Connection, source_id: str, end: int) -> None:
    db.execute('''
        INSERT INTO frontiers (source_id, event_number) VALUES (?, ?)
        ON CONFLICT(source_id) DO UPDATE SET event_number = excluded.event_number
    ''', (source_id, end))
