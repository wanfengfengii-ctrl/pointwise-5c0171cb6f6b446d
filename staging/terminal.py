from dataclasses import dataclass
from sqlite3 import Connection

from contracts import InvalidEvent
from feed.events import EventKind
from intake.decoder import DecodedEvent
from staging.journal import load_pending
from staging.operations import ReadingOperation
from staging.replay import replay_pending


@dataclass(frozen=True)
class CommitCandidate:
    terminal: DecodedEvent
    operations: tuple[ReadingOperation, ...]


@dataclass(frozen=True)
class TerminalDecision:
    terminal: DecodedEvent
    candidate: CommitCandidate | None


def resolve_terminal(db: Connection, terminal: DecodedEvent) -> TerminalDecision:
    pending = load_pending(db, terminal.source_id, terminal.transaction_id)
    if len(pending) < 2 or pending[-1] != terminal or not terminal.is_terminal:
        raise InvalidEvent(f'terminal {terminal.number} has no matching journal delivery')
    view = replay_pending(pending[:-1])
    candidate = None
    if terminal.kind == EventKind.COMMIT:
        candidate = CommitCandidate(terminal, view.operations)
    return TerminalDecision(terminal, candidate)
