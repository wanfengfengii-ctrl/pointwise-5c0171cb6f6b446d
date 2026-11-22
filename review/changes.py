from staging.operations import ReadingOperation
from warehouse.ledger import PublishedLedger


def ordered_changes(ledger: PublishedLedger, station: str | None = None) -> tuple[ReadingOperation, ...]:
    if station is None:
        return ledger.changes
    return tuple(change for change in ledger.changes if change.key.station == station)
