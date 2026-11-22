from __future__ import annotations

from typing import TYPE_CHECKING

from contracts import ClearedRange, InvalidConfirmation, InvalidRange, valid_number
from feed.registry import SourceStream

if TYPE_CHECKING:
    from intake.receipts import AcceptReceipt


def require_readable(stream: SourceStream, start: int, end: int) -> None:
    if not valid_number(start) or not valid_number(end) or start > end:
        raise InvalidRange('expected an ascending inclusive event range')
    if start <= stream.discarded_through:
        raise ClearedRange(f'{stream.source_id} cleared through {stream.discarded_through}')
    if end > stream.last_number:
        raise InvalidRange(f'{stream.source_id} ends at {stream.last_number}')


def acknowledge(stream: SourceStream, receipt: AcceptReceipt) -> int:
    if (receipt.source_id != stream.source_id or not valid_number(receipt.start)
            or not valid_number(receipt.end) or not receipt.start <= receipt.end <= stream.last_number):
        raise InvalidConfirmation('confirmation does not cover a range of this source')
    stream.acknowledged_through = max(stream.acknowledged_through, receipt.end)
    return stream.acknowledged_through


def discard_confirmed_prefix(stream: SourceStream, through: int | None = None) -> int:
    target = stream.acknowledged_through if through is None else through
    if type(target) is not int or not stream.discarded_through <= target <= stream.acknowledged_through:
        raise InvalidConfirmation('discard boundary must lie within the remaining confirmed prefix')
    count = target - stream.discarded_through
    stream.events = stream.events[count:]
    stream.discarded_through = target
    return target
