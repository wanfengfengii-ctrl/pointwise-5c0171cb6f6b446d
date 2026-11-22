from feed.events import EventKind
from staging.operations import ReadingOperation
from warehouse.ledger import PublishedLedger


def check_provenance(ledger: PublishedLedger, station: str) -> tuple[str, ...]:
    receipt = ledger.receipt
    issues = []
    if (receipt.result_id, receipt.day) != (ledger.result_id, ledger.day):
        issues.append('receipt does not identify the selected ledger')
    previous = 0
    last_by_key: dict = {}
    for change in ledger.changes:
        if not previous < change.number < receipt.terminal_number:
            issues.append(f'event {change.number} is outside original transaction order')
        previous = change.number
        if ((change.source_id, change.transaction_id) != (receipt.source_id, receipt.transaction_id)
                or change.key.day != ledger.day):
            issues.append(f'event {change.number} does not belong to the selected receipt')
        if change.key.station == station:
            last_by_key[change.key] = change

    for operation in last_by_key.values():
        _check_effect(ledger, operation, issues)
    return tuple(issues)


def _check_effect(ledger: PublishedLedger, operation: ReadingOperation, issues: list[str]) -> None:
    key = operation.key
    if operation.kind == EventKind.DELETE:
        if key in ledger.rows:
            issues.append(f'event {operation.number} deleted a point that is still present')
    elif key not in ledger.rows or ledger.rows[key] != operation.reading:
        issues.append(f'event {operation.number} disagrees with the published reading')
