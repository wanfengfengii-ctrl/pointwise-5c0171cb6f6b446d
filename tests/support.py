import json
import sqlite3
from dataclasses import astuple
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from feed.events import SourceEvent, load_events
from feed.registry import SourceRegistry
from intake.admission import accept
from intake.collector import collect_segment
from staging.journal import load_pending
from warehouse.initialize import initialize
from warehouse.storage import open_warehouse


DAY = '2026-05-08'
SOURCE = 'station-feed'
SAMPLES = Path(__file__).resolve().parents[1] / 'samples'


def event(number, transaction, kind, *, day=DAY, station='east', point=None,
          reading=None, savepoint=None):
    return SourceEvent(SOURCE, number, transaction, kind, day,
                       station if point is not None else None, point, reading, savepoint)


def baseline_events():
    return (event(1, 'baseline', 'write', point='x', reading=10),
            event(2, 'baseline', 'commit'))


class ResponseLost(Exception):
    pass


def lose_response(db, batch):
    accept(db, batch)
    raise ResponseLost('acceptance committed before the reply was lost')


class RejectCommit:
    def __init__(self, db):
        self.db = db
        self.attempts = 0

    def __enter__(self):
        self.db.set_authorizer(self.authorize)
        return self

    def __exit__(self, *exc):
        self.db.set_authorizer(None)

    def authorize(self, action, argument, _table, _database, _trigger):
        if action == sqlite3.SQLITE_TRANSACTION and argument == 'COMMIT':
            self.attempts += 1
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK


def point_values(ledger):
    return {key.point: value for key, value in ledger.rows.items() if key.station == 'east'}


def event_numbers(ledger):
    return [change.number for change in ledger.changes]


def station_values(summary):
    return list(astuple(summary))


class WarehouseCase(TestCase):
    def setUp(self):
        directory = TemporaryDirectory(prefix='chengwan-test-')
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'warehouse.sqlite'
        self.db = open_warehouse(self.path)
        self.addCleanup(lambda: self.db.close())
        initialize(self.db)

    def reopen(self):
        self.db.close()
        self.db = open_warehouse(self.path)

    def register(self, events):
        self.registry = SourceRegistry()
        self.stream = self.registry.register(SOURCE, events)
        return self.stream

    def load_sample(self, name):
        self.register(load_events(SAMPLES / name))

    def collect(self, start, end, **kwargs):
        return collect_segment(self.db, self.registry, SOURCE, start, end, **kwargs)

    def pending(self, transaction):
        return load_pending(self.db, SOURCE, transaction)

    def publication_ids(self):
        return [row[0] for row in self.db.execute('SELECT result_id FROM ledgers ORDER BY result_id')]

    def publication_receipt_count(self):
        return self.db.execute('SELECT count(result_id) FROM completions').fetchone()[0]

    def scenario(self, case_id, inputs, expected, actual, downstream_expected, downstream_actual,
                 paths, oracle):
        self.assertEqual(actual, expected)
        self.assertEqual(downstream_actual, downstream_expected)
        report = {'case_id': case_id, 'input': inputs, 'expected': expected, 'actual': actual,
                  'downstream_expected': downstream_expected, 'downstream_actual': downstream_actual,
                  'code_paths': paths, 'oracle': oracle}
        print('POINTWISE_SCENARIO ' + json.dumps(report, ensure_ascii=True, sort_keys=True), flush=True)
