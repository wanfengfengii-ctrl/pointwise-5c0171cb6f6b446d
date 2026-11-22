import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


def open_warehouse(path: str | Path) -> sqlite3.Connection:
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute('PRAGMA foreign_keys = ON')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def write_transaction(db: sqlite3.Connection, *, join: bool = False) -> Iterator[None]:
    if db.in_transaction:
        if not join:
            raise sqlite3.ProgrammingError('operation requires its own SQLite transaction')
        yield
        return
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.commit()
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise
