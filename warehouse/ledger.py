from dataclasses import dataclass
from sqlite3 import Connection

from contracts import InvalidEvent, ReadingKey, ResultNotFound, decode_reading, encode_reading
from feed.events import EventKind
from intake.decoder import DecodedEvent
from staging.operations import ReadingOperation
from warehouse.materialize import MaterializedLedger


@dataclass(frozen=True)
class PublicationReceipt:
    result_id: int
    source_id: str
    transaction_id: str
    terminal_number: int
    day: str


@dataclass(frozen=True)
class PublishedLedger:
    result_id: int
    day: str
    rows: dict[ReadingKey, int | None]
    changes: tuple[ReadingOperation, ...]
    receipt: PublicationReceipt


@dataclass(frozen=True)
class Completion:
    result_id: int | None


def get_ledger(db: Connection, result_id: int) -> PublishedLedger:
    header = db.execute('''
        SELECT l.day, c.source_id, c.transaction_id, c.terminal_number, c.day
        FROM ledgers AS l JOIN completions AS c USING (result_id)
        WHERE l.result_id = ?
    ''', (result_id,)).fetchone()
    if header is None:
        raise ResultNotFound(f'no published ledger {result_id}')
    day, source, transaction, number, receipt_day = header
    receipt = PublicationReceipt(result_id, source, transaction, number, receipt_day)
    row_records = db.execute('''
        SELECT station, point, reading FROM ledger_rows WHERE result_id = ? ORDER BY station, point
    ''', (result_id,))
    rows = {ReadingKey(day, station, point): decode_reading(reading)
            for station, point, reading in row_records}
    change_records = db.execute('''
        SELECT source_id, event_number, transaction_id, kind, station, point, reading
        FROM ledger_changes WHERE result_id = ? ORDER BY position
    ''', (result_id,))
    changes = tuple(
        ReadingOperation(change_source, event_number, change_tx, EventKind(kind),
                         ReadingKey(day, station, point), decode_reading(value))
        for change_source, event_number, change_tx, kind, station, point, value in change_records
    )
    return PublishedLedger(result_id, day, rows, changes, receipt)


def get_latest_ledger(db: Connection, day: str) -> PublishedLedger | None:
    row = db.execute('SELECT result_id FROM ledgers WHERE day = ? ORDER BY result_id DESC LIMIT 1',
                     (day,)).fetchone()
    return get_ledger(db, row[0]) if row is not None else None


def _record_completion(db: Connection, terminal: DecodedEvent, result_id: int | None) -> None:
    db.execute('''
        INSERT INTO completions (source_id, terminal_number, transaction_id, day, kind, result_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (terminal.source_id, terminal.number, terminal.transaction_id, terminal.day,
          terminal.kind.value, result_id))


def store_ledger(db: Connection, terminal: DecodedEvent, contents: MaterializedLedger) -> PublishedLedger:
    result_id = db.execute('INSERT INTO ledgers (day) VALUES (?)', (terminal.day,)).lastrowid
    db.executemany('''
        INSERT INTO ledger_rows (result_id, station, point, reading) VALUES (?, ?, ?, ?)
    ''', ((result_id, key.station, key.point, encode_reading(reading))
          for key, reading in sorted(contents.rows.items())))
    db.executemany('''
        INSERT INTO ledger_changes (result_id, position, source_id, event_number, transaction_id,
                                    kind, station, point, reading)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', ((result_id, position, change.source_id, change.number, change.transaction_id,
           change.kind.value, change.key.station, change.key.point, encode_reading(change.reading))
          for position, change in enumerate(contents.changes, 1)))
    _record_completion(db, terminal, result_id)
    return get_ledger(db, result_id)


def find_completion(db: Connection, terminal: DecodedEvent) -> Completion | None:
    row = db.execute('''
        SELECT transaction_id, day, kind, result_id FROM completions
        WHERE source_id = ? AND terminal_number = ?
    ''', (terminal.source_id, terminal.number)).fetchone()
    if row is None:
        return None
    transaction_id, day, kind, result_id = row
    if (transaction_id, day, kind) != (terminal.transaction_id, terminal.day, terminal.kind.value):
        raise InvalidEvent(f'conflicting completed terminal at {terminal.number}')
    return Completion(result_id)


def record_abort(db: Connection, terminal: DecodedEvent) -> None:
    _record_completion(db, terminal, None)
