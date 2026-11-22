from sqlite3 import Connection

from contracts import InvalidEvent, ReadingKey, decode_reading, encode_reading
from feed.events import EventKind
from intake.decoder import DecodedEvent


def load_pending(db: Connection, source_id: str, transaction_id: str) -> tuple[DecodedEvent, ...]:
    rows = db.execute('''
        SELECT e.event_number, e.kind, tx.day, e.station, e.point, e.reading, e.savepoint
        FROM pending_events AS e JOIN pending_transactions AS tx USING (source_id)
        WHERE tx.source_id = ? AND tx.transaction_id = ?
        ORDER BY e.event_number
    ''', (source_id, transaction_id))
    events = []
    for number, kind, day, station, point, reading, savepoint in rows:
        key = ReadingKey(day, station, point) if station is not None else None
        events.append(DecodedEvent(source_id, number, transaction_id, EventKind(kind), day,
                                   key, decode_reading(reading), savepoint))
    return tuple(events)


def append_events(db: Connection, events: tuple[DecodedEvent, ...]) -> None:
    if not events:
        return
    first = events[0]
    identity = first.source_id, first.transaction_id, first.day
    if any((event.source_id, event.transaction_id, event.day) != identity for event in events):
        raise InvalidEvent(f'a terminal is required before changing transaction or date at {first.number}')
    if any(event.is_terminal for event in events[:-1]):
        raise InvalidEvent('a journal append may only end with a terminal')

    active = db.execute('''
        SELECT transaction_id, day FROM pending_transactions WHERE source_id = ?
    ''', (first.source_id,)).fetchone()
    if active is None:
        if first.is_terminal:
            raise InvalidEvent(f'terminal {first.number} has no pending transaction')
        completed = db.execute('''
            SELECT 1 FROM completions WHERE source_id = ? AND transaction_id = ?
        ''', identity[:2]).fetchone()
        if completed is not None:
            raise InvalidEvent(f'transaction identity already completed: {first.transaction_id}')
        db.execute('INSERT INTO pending_transactions VALUES (?, ?, ?)', identity)
    elif active != identity[1:]:
        raise InvalidEvent(f'event {first.number} does not belong to the active transaction')

    rows = []
    for event in events:
        rows.append((event.source_id, event.number, event.kind.value,
                     event.key.station if event.key else None, event.key.point if event.key else None,
                     encode_reading(event.reading), event.savepoint))
    db.executemany('''
        INSERT INTO pending_events (source_id, event_number, kind, station, point, reading, savepoint)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', rows)


def close_pending(db: Connection, source_id: str, transaction_id: str) -> None:
    db.execute('DELETE FROM pending_transactions WHERE source_id = ? AND transaction_id = ?',
               (source_id, transaction_id))
