from sqlite3 import Connection

from contracts import SegmentGap
from feed.segments import SourceSegment
from intake.decoder import DecodedEvent, decode_event
from intake.progress import advance_frontier, check_continuity, get_frontier
from intake.receipts import AcceptReceipt, lookup_receipt, request_body, store_receipt
from staging.journal import append_events, load_pending
from staging.replay import replay_pending
from warehouse.publication import finish
from warehouse.storage import write_transaction


def _deliver(db: Connection, events: tuple[DecodedEvent, ...]) -> tuple[int, ...]:
    result_ids = []
    start = 0
    for index, event in enumerate(events):
        if event.is_terminal:
            append_events(db, events[start:index + 1])
            ledger = finish(db, event)
            if ledger is not None:
                result_ids.append(ledger.result_id)
            start = index + 1

    if start < len(events):
        tail = events[start:]
        append_events(db, tail)
        first = tail[0]
        replay_pending(load_pending(db, first.source_id, first.transaction_id))
    return tuple(result_ids)


def accept(db: Connection, batch: SourceSegment) -> AcceptReceipt:
    check_continuity(batch)
    events = tuple(decode_event(event) for event in batch.events)
    body = request_body(batch, events)
    with write_transaction(db):
        receipt = lookup_receipt(db, batch, events, body)
        if receipt is None:
            expected = get_frontier(db, batch.source_id) + 1
            if batch.start != expected:
                raise SegmentGap(f'expected new segment to start at {expected}, got {batch.start}')
            result_ids = _deliver(db, events)
            advance_frontier(db, batch.source_id, batch.end)
            receipt = store_receipt(db, batch, events, body, result_ids)
    return receipt
