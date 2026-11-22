from collections.abc import Callable
from sqlite3 import Connection

from contracts import InvalidConfirmation
from feed.registry import SourceRegistry
from feed.retention import acknowledge
from feed.segments import SourceSegment, segment
from intake.admission import accept
from intake.receipts import AcceptReceipt


def collect_segment(db: Connection, registry: SourceRegistry, source_id: str,
                    start: int, end: int, segment_id: str | None = None, *,
                    receiver: Callable[[Connection, SourceSegment], AcceptReceipt] = accept) -> AcceptReceipt:
    batch = segment(registry, source_id, start, end, segment_id)
    receipt = receiver(db, batch)
    expected = (batch.source_id, batch.segment_id, batch.start, batch.end)
    if not isinstance(receipt, AcceptReceipt):
        raise InvalidConfirmation('receiver did not return an acceptance confirmation')
    if (receipt.source_id, receipt.segment_id, receipt.start, receipt.end) != expected:
        raise InvalidConfirmation('receiver returned a confirmation for a different request')
    acknowledge(registry.get(source_id), receipt)
    return receipt
