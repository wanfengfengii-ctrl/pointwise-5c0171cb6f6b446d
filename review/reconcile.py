from dataclasses import dataclass
from sqlite3 import Connection

from review.changes import ordered_changes
from review.provenance import check_provenance
from review.selection import select_ledger
from review.station_totals import StationTotals, summarize_station
from staging.operations import ReadingOperation
from warehouse.ledger import PublicationReceipt


@dataclass(frozen=True)
class Reconciliation:
    receipt: PublicationReceipt
    station: str
    totals: StationTotals
    changes: tuple[ReadingOperation, ...]
    issues: tuple[str, ...]


def reconcile(db: Connection, result_id: int, station: str) -> Reconciliation:
    ledger = select_ledger(db, result_id)
    station_totals = summarize_station(ledger, station)
    return Reconciliation(ledger.receipt, station, station_totals, ordered_changes(ledger, station),
                          check_provenance(ledger, station))
