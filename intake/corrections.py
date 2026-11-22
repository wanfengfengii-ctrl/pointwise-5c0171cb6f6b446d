"""Storage and validation helpers for grouped source corrections.

A correction segment is received like an ordinary segment (same frontier,
segment identity space and transaction rules) but its terminal is not posted
to the day ledger until a joint group publication runs.  The preimage
conditions supplied with the segment are part of its identity and are checked
again, operation by operation, when the group is published.
"""
import json
from dataclasses import dataclass
from sqlite3 import Connection

from contracts import InvalidEvent, decode_reading
from feed.events import EventKind, SourceEvent
from intake.decoder import DecodedEvent, decode_event
from staging.replay import replay_pending


@dataclass(frozen=True)
class StoredCorrection:
    source_id: str
    segment_id: str
    group_id: str
    request_body: str
    before_body: str
    transaction_id: str
    terminal_number: int
    day: str


_COLUMNS = ('source_id, segment_id, group_id, request_body, before_body, '
            'transaction_id, terminal_number, day')


def normalize_before(before, events: tuple[DecodedEvent, ...]) -> dict[int, tuple[bool, int | None]]:
    """Validate and normalize a preimage mapping for one correction segment."""
    if before is None:
        return {}
    if not isinstance(before, dict):
        raise InvalidEvent('before must map write or delete event numbers to preimages')
    operations = {event.number for event in events
                  if event.kind in (EventKind.WRITE, EventKind.DELETE)}
    conditions: dict[int, tuple[bool, int | None]] = {}
    for number, image in before.items():
        if type(number) is not int or number not in operations:
            raise InvalidEvent(f'preimage condition does not name a write or delete: {number!r}')
        if not isinstance(image, (tuple, list)) or len(image) != 2:
            raise InvalidEvent(f'preimage at {number} must be a (present, reading) pair')
        present, reading = image
        if type(present) is not bool:
            raise InvalidEvent(f'preimage presence at {number} must be a boolean')
        if present:
            if reading is not None and type(reading) is not int:
                raise InvalidEvent(f'preimage reading at {number} must be an integer or null')
        elif reading is not None:
            raise InvalidEvent(f'a missing row at {number} cannot carry a reading')
        conditions[number] = (present, reading)
    return conditions


def encode_before(conditions: dict[int, tuple[bool, int | None]]) -> str:
    return json.dumps([[number, [present, reading]]
                       for number, (present, reading) in sorted(conditions.items())],
                      ensure_ascii=True, separators=(',', ':'))


def decode_before(body: str) -> dict[int, tuple[bool, int | None]]:
    return {number: (present, reading) for number, (present, reading) in json.loads(body)}


def decode_body(body: str) -> tuple[tuple[DecodedEvent, ...], int, int]:
    start, end, records = json.loads(body)
    events = []
    for record in records:
        source_id, number, transaction_id, kind, day, station, point, reading, savepoint = record
        events.append(decode_event(SourceEvent(source_id, number, transaction_id, kind, day,
                                               station, point, decode_reading(reading), savepoint)))
    return tuple(events), start, end


def member_plan(record: StoredCorrection):
    """Return the retained operations and preimage conditions of a stored correction."""
    events, _, _ = decode_body(record.request_body)
    if (not events or events[-1].kind != EventKind.COMMIT
            or events[-1].number != record.terminal_number):
        raise InvalidEvent(f'stored correction {record.source_id}/{record.segment_id} is incomplete')
    view = replay_pending(events[:-1])
    return view.operations, decode_before(record.before_body)


def get_stored(db: Connection, source_id: str, segment_id: str) -> StoredCorrection | None:
    row = db.execute(f'SELECT {_COLUMNS} FROM correction_segments '
                     'WHERE source_id = ? AND segment_id = ?',
                     (source_id, segment_id)).fetchone()
    return StoredCorrection(*row) if row is not None else None


def store_stored(db: Connection, record: StoredCorrection) -> None:
    db.execute('''INSERT INTO correction_segments
                  (source_id, segment_id, group_id, request_body, before_body,
                   transaction_id, terminal_number, day)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
               (record.source_id, record.segment_id, record.group_id, record.request_body,
                record.before_body, record.transaction_id, record.terminal_number, record.day))


def ordinary_identity_exists(db: Connection, source_id: str, segment_id: str) -> bool:
    row = db.execute('SELECT 1 FROM accepted_segments WHERE source_id = ? AND segment_id = ?',
                     (source_id, segment_id)).fetchone()
    return row is not None


def group_published(db: Connection, group_id: str) -> bool:
    row = db.execute('SELECT 1 FROM group_publications WHERE group_id = ?',
                     (group_id,)).fetchone()
    return row is not None
