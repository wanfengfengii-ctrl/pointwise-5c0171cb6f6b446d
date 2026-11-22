from sqlite3 import Connection

from contracts import (InvalidEvent, SegmentConflict, SegmentGap, valid_name)
from feed.events import EventKind
from feed.segments import SourceSegment
from intake.corrections import (StoredCorrection, encode_before, get_stored, group_published,
                                normalize_before, ordinary_identity_exists, store_stored)
from intake.decoder import DecodedEvent, decode_event
from intake.progress import advance_frontier, check_continuity, get_frontier
from intake.receipts import AcceptReceipt, lookup_receipt, request_body, store_receipt
from staging.journal import append_events, close_pending, load_pending
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


def _check_ordinary_identity(db: Connection, events: tuple[DecodedEvent, ...]) -> None:
    """An ordinary segment cannot reuse a correction segment's transaction name."""
    first = events[0]
    if db.execute('SELECT 1 FROM correction_segments WHERE source_id = ? AND transaction_id = ?',
                  (first.source_id, first.transaction_id)).fetchone() is not None:
        raise InvalidEvent(f'transaction identity already received by a correction: '
                           f'{first.transaction_id}')


def accept(db: Connection, batch: SourceSegment) -> AcceptReceipt:
    check_continuity(batch)
    events = tuple(decode_event(event) for event in batch.events)
    body = request_body(batch, events)
    with write_transaction(db):
        receipt = lookup_receipt(db, batch, events, body)
        if receipt is None:
            if get_stored(db, batch.source_id, batch.segment_id) is not None:
                raise SegmentConflict(
                    f'{batch.source_id}/{batch.segment_id} is a received correction segment')
            _check_ordinary_identity(db, events)
            expected = get_frontier(db, batch.source_id) + 1
            if batch.start != expected:
                raise SegmentGap(f'expected new segment to start at {expected}, got {batch.start}')
            result_ids = _deliver(db, events)
            advance_frontier(db, batch.source_id, batch.end)
            receipt = store_receipt(db, batch, events, body, result_ids)
    return receipt


def _confirmation(batch: SourceSegment, events: tuple[DecodedEvent, ...]) -> AcceptReceipt:
    return AcceptReceipt(batch.source_id, batch.segment_id, batch.start, batch.end,
                         tuple(event for event in events if event.is_terminal), ())


def _check_correction_shape(events: tuple[DecodedEvent, ...]) -> None:
    if not events or events[-1].kind != EventKind.COMMIT:
        raise InvalidEvent('a correction batch must end with its only commit terminal')
    if any(event.is_terminal for event in events[:-1]):
        raise InvalidEvent('a correction batch may contain only its final commit')
    identity = events[0].source_id, events[0].transaction_id, events[0].day
    if any((event.source_id, event.transaction_id, event.day) != identity for event in events):
        raise InvalidEvent('a correction batch must cover one transaction on one day')
    replay_pending(events[:-1])


def accept_correction(db: Connection, group_id: str, batch: SourceSegment,
                      before=None) -> AcceptReceipt:
    if not valid_name(group_id):
        raise InvalidEvent('group identity must be nonempty text')
    check_continuity(batch)
    events = tuple(decode_event(event) for event in batch.events)
    _check_correction_shape(events)
    conditions = normalize_before(before, events)
    body = request_body(batch, events)
    before_body = encode_before(conditions)
    with write_transaction(db):
        if ordinary_identity_exists(db, batch.source_id, batch.segment_id):
            raise SegmentConflict(
                f'{batch.source_id}/{batch.segment_id} is an ordinary accepted segment')
        stored = get_stored(db, batch.source_id, batch.segment_id)
        if stored is not None:
            if stored.group_id != group_id:
                raise SegmentConflict(
                    f'{batch.source_id}/{batch.segment_id} belongs to group {stored.group_id}')
            if stored.request_body != body:
                raise SegmentConflict(
                    f'{batch.source_id}/{batch.segment_id} was received with different content')
            if stored.before_body != before_body:
                raise SegmentConflict(
                    f'{batch.source_id}/{batch.segment_id} was received with different conditions')
            return _confirmation(batch, events)

        other = db.execute(
            'SELECT segment_id FROM correction_segments WHERE group_id = ? AND source_id = ?',
            (group_id, batch.source_id)).fetchone()
        if other is not None:
            raise SegmentConflict(
                f'group {group_id} already has a segment from {batch.source_id}: {other[0]}')
        if group_published(db, group_id):
            raise SegmentConflict(f'group {group_id} is already published')

        expected = get_frontier(db, batch.source_id) + 1
        if batch.start != expected:
            raise SegmentGap(f'expected new segment to start at {expected}, got {batch.start}')
        if db.execute('SELECT 1 FROM pending_transactions WHERE source_id = ?',
                      (batch.source_id,)).fetchone() is not None:
            raise InvalidEvent('a correction must start its own transaction')
        if db.execute('SELECT 1 FROM correction_segments WHERE source_id = ? AND transaction_id = ?',
                      (batch.source_id, events[0].transaction_id)).fetchone() is not None:
            raise InvalidEvent(
                f'transaction identity already received: {events[0].transaction_id}')

        append_events(db, events)
        close_pending(db, batch.source_id, events[0].transaction_id)
        advance_frontier(db, batch.source_id, batch.end)
        store_stored(db, StoredCorrection(batch.source_id, batch.segment_id, group_id, body,
                                          before_body, events[0].transaction_id,
                                          events[-1].number, events[0].day))
    return _confirmation(batch, events)
