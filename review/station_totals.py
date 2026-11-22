from dataclasses import dataclass
from sqlite3 import Connection

from contracts import InvalidQuery, valid_name
from review.selection import select_ledger
from warehouse.ledger import PublishedLedger


@dataclass(frozen=True)
class StationTotals:
    row_count: int
    measured_count: int
    total: int | None


def summarize_station(ledger: PublishedLedger, station: str) -> StationTotals:
    if not valid_name(station):
        raise InvalidQuery('station key must be nonempty text')
    row_count = measured_count = total = 0
    for key, reading in ledger.rows.items():
        if key.station == station:
            row_count += 1
            if reading is not None:
                measured_count += 1
                total += reading
    return StationTotals(row_count, measured_count, total if measured_count else None)


def totals(db: Connection, result_id: int, station: str) -> StationTotals:
    return summarize_station(select_ledger(db, result_id), station)
