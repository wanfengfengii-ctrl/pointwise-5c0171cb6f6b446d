from sqlite3 import Connection

from contracts import InvalidQuery, valid_number
from warehouse.ledger import PublishedLedger, get_ledger


def select_ledger(db: Connection, result_id: int) -> PublishedLedger:
    if not valid_number(result_id):
        raise InvalidQuery('result ID must be a positive SQLite integer')
    return get_ledger(db, result_id)


def read(db: Connection, result_id: int) -> PublishedLedger:
    return select_ledger(db, result_id)
