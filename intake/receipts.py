import json
from dataclasses import dataclass
from sqlite3 import Connection

from contracts import SegmentConflict, encode_reading
from feed.segments import SourceSegment
from intake.decoder import DecodedEvent


@dataclass(frozen=True)
class AcceptReceipt:
    source_id: str
    segment_id: str
    start: int
    end: int
    terminals: tuple[DecodedEvent, ...]
    result_ids: tuple[int, ...]


def request_body(batch: SourceSegment, events: tuple[DecodedEvent, ...]) -> str:
    records = []
    for event in events:
        records.append([
            event.source_id, event.number, event.transaction_id, event.kind.value, event.day,
            event.key.station if event.key else None, event.key.point if event.key else None,
            encode_reading(event.reading), event.savepoint,
        ])
    return json.dumps([batch.start, batch.end, records], ensure_ascii=True, separators=(',', ':'))


def _confirmation(batch: SourceSegment, events: tuple[DecodedEvent, ...],
                  result_ids: tuple[int, ...]) -> AcceptReceipt:
    return AcceptReceipt(batch.source_id, batch.segment_id, batch.start, batch.end,
                         tuple(event for event in events if event.is_terminal), result_ids)


def lookup_receipt(db: Connection, batch: SourceSegment, events: tuple[DecodedEvent, ...],
                   body: str) -> AcceptReceipt | None:
    row = db.execute('''
        SELECT request_body, result_ids FROM accepted_segments
        WHERE source_id = ? AND segment_id = ?
    ''', (batch.source_id, batch.segment_id)).fetchone()
    if row is None:
        return None
    original_body, original_results = row
    if original_body != body:
        raise SegmentConflict(f'{batch.source_id}/{batch.segment_id} was accepted with different content')
    return _confirmation(batch, events, tuple(json.loads(original_results)))


def store_receipt(db: Connection, batch: SourceSegment, events: tuple[DecodedEvent, ...],
                  body: str, result_ids: tuple[int, ...]) -> AcceptReceipt:
    db.execute('''
        INSERT INTO accepted_segments (source_id, segment_id, request_body, result_ids)
        VALUES (?, ?, ?, ?)
    ''', (batch.source_id, batch.segment_id, body, json.dumps(result_ids)))
    return _confirmation(batch, events, result_ids)
