from dataclasses import dataclass

from contracts import InvalidEvent
from feed.events import EventKind
from intake.decoder import DecodedEvent
from staging.operations import ReadingOperation, to_operation
from staging.savepoints import Savepoint, SavepointStack


@dataclass(frozen=True)
class PendingView:
    operations: tuple[ReadingOperation, ...]
    savepoints: tuple[Savepoint, ...]


def replay_pending(events: tuple[DecodedEvent, ...]) -> PendingView:
    stack = SavepointStack()
    retained: list[ReadingOperation] = []
    previous = 0
    for event in events:
        if event.number <= previous or event.is_terminal:
            raise InvalidEvent(f'invalid event order in pending replay at {event.number}')
        previous = event.number
        if event.kind == EventKind.SAVEPOINT:
            stack.push(event.savepoint, event.number)
        elif event.kind == EventKind.ROLLBACK_TO:
            target = stack.rollback_to(event.savepoint)
            while retained and retained[-1].number > target.number:
                retained.pop()
        else:
            retained.append(to_operation(event))
    return PendingView(tuple(retained), stack.active)
