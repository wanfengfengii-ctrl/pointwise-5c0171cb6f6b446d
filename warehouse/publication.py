from sqlite3 import Connection

from contracts import InvalidEvent
from intake.decoder import DecodedEvent, decode_event
from feed.events import SourceEvent
from staging.journal import close_pending
from staging.terminal import resolve_terminal
from warehouse.ledger import (PublishedLedger, find_completion, get_latest_ledger, get_ledger,
                              record_abort, store_ledger)
from warehouse.materialize import materialize
from warehouse.storage import write_transaction

from warehouse.groups import publish_group, read_group  # noqa: E402  (re-exported interface)


def _check_terminal(terminal: DecodedEvent) -> None:
    if (not isinstance(terminal, DecodedEvent) or not terminal.is_terminal
            or terminal.key is not None):
        raise InvalidEvent('finish requires a decoded commit or abort')
    decode_event(SourceEvent(terminal.source_id, terminal.number, terminal.transaction_id,
                             terminal.kind, terminal.day, reading=terminal.reading,
                             savepoint=terminal.savepoint))


def finish(db: Connection, terminal: DecodedEvent) -> PublishedLedger | None:
    _check_terminal(terminal)
    with write_transaction(db, join=True):
        completion = find_completion(db, terminal)
        if completion is not None:
            if completion.result_id is None:
                return None
            return get_ledger(db, completion.result_id)

        if db.execute('SELECT 1 FROM correction_segments WHERE source_id = ? AND terminal_number = ?',
                      (terminal.source_id, terminal.number)).fetchone() is not None:
            raise InvalidEvent(
                f'correction terminal {terminal.number} has not been jointly published')

        decision = resolve_terminal(db, terminal)
        result = None
        if decision.candidate is None:
            record_abort(db, decision.terminal)
        else:
            candidate = decision.candidate
            previous = get_latest_ledger(db, candidate.terminal.day)
            contents = materialize(previous.rows if previous else {}, candidate.operations)
            result = store_ledger(db, candidate.terminal, contents)
        close_pending(db, terminal.source_id, terminal.transaction_id)
    return result
