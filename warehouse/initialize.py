from sqlite3 import Connection

from warehouse.storage import write_transaction


_SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY, value TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS frontiers (
        source_id TEXT PRIMARY KEY, event_number INTEGER NOT NULL CHECK (event_number > 0)
    )''',
    '''CREATE TABLE IF NOT EXISTS accepted_segments (
        source_id TEXT NOT NULL, segment_id TEXT NOT NULL,
        request_body TEXT NOT NULL, result_ids TEXT NOT NULL,
        PRIMARY KEY (source_id, segment_id)
    )''',
    '''CREATE TABLE IF NOT EXISTS pending_transactions (
        source_id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL, day TEXT NOT NULL
    )''',
    '''CREATE TABLE IF NOT EXISTS pending_events (
        source_id TEXT NOT NULL REFERENCES pending_transactions(source_id) ON DELETE CASCADE,
        event_number INTEGER NOT NULL, kind TEXT NOT NULL,
        station TEXT, point TEXT, reading TEXT, savepoint TEXT,
        PRIMARY KEY (source_id, event_number)
    )''',
    '''CREATE TABLE IF NOT EXISTS ledgers (
        result_id INTEGER PRIMARY KEY, day TEXT NOT NULL
    )''',
    '''CREATE INDEX IF NOT EXISTS ledger_day ON ledgers (day, result_id)''',
    '''CREATE TABLE IF NOT EXISTS ledger_rows (
        result_id INTEGER NOT NULL REFERENCES ledgers(result_id),
        station TEXT NOT NULL, point TEXT NOT NULL, reading TEXT,
        PRIMARY KEY (result_id, station, point)
    )''',
    '''CREATE TABLE IF NOT EXISTS ledger_changes (
        result_id INTEGER NOT NULL REFERENCES ledgers(result_id), position INTEGER NOT NULL,
        source_id TEXT NOT NULL, event_number INTEGER NOT NULL, transaction_id TEXT NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('write', 'delete')),
        station TEXT NOT NULL, point TEXT NOT NULL, reading TEXT,
        PRIMARY KEY (result_id, position), UNIQUE (result_id, event_number)
    )''',
    '''CREATE TABLE IF NOT EXISTS completions (
        source_id TEXT NOT NULL, terminal_number INTEGER NOT NULL,
        transaction_id TEXT NOT NULL, day TEXT NOT NULL, kind TEXT NOT NULL,
        result_id INTEGER UNIQUE REFERENCES ledgers(result_id),
        PRIMARY KEY (source_id, terminal_number), UNIQUE (source_id, transaction_id),
        CHECK ((kind = 'commit' AND result_id IS NOT NULL) OR
               (kind = 'abort' AND result_id IS NULL))
    )''',
)


def initialize(db: Connection) -> None:
    with write_transaction(db):
        for statement in _SCHEMA:
            db.execute(statement)
        db.executemany('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (
            ('centre_name', '澄湾观测资料中心'),
            ('review_label', '夜班观测复核'),
        ))
