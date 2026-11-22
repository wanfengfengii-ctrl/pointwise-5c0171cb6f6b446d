import json
from contextlib import closing
from dataclasses import astuple
from pathlib import Path
from tempfile import TemporaryDirectory

from contracts import ClearedRange
from feed.events import load_events
from feed.registry import SourceRegistry
from feed.retention import discard_confirmed_prefix
from feed.segments import segment
from intake.collector import collect_segment
from review.reconcile import reconcile
from review.selection import read
from review.station_totals import totals
from warehouse.initialize import initialize
from warehouse.storage import open_warehouse


SAMPLES = Path(__file__).resolve().parent / 'samples'
SOURCE = 'station-feed'


def register(filename: str) -> SourceRegistry:
    registry = SourceRegistry()
    registry.register(SOURCE, load_events(SAMPLES / filename))
    return registry


def ordinary(path: Path) -> None:
    registry = register('ordinary.json')
    with closing(open_warehouse(path)) as db:
        initialize(db)
        result_id, = collect_segment(db, registry, SOURCE, 1, 3).result_ids
        ledger = read(db, result_id)
        station_totals = list(astuple(totals(db, result_id, 'east')))
        rows = {key.point: value for key, value in ledger.rows.items()}
        assert rows == {'zero': 0, 'missing': None}
        assert station_totals == [2, 1, 0]
        print('POINTWISE_SCENARIO ' + json.dumps({
            'case_id': 't7', 'input': {'east': [0, None]},
            'expected': {'zero': 0, 'missing': None}, 'actual': rows,
            'downstream_expected': [2, 1, 0], 'downstream_actual': station_totals,
            'code_paths': ['warehouse/ledger.py', 'review/station_totals.py'],
            'oracle': 'Count both rows and sum the only nonmissing reading, which is zero.',
        }, sort_keys=True))


def savepoint(path: Path) -> None:
    registry = register('savepoint.json')
    with closing(open_warehouse(path)) as db:
        initialize(db)
        baseline, = collect_segment(db, registry, SOURCE, 1, 2).result_ids
        prefix = collect_segment(db, registry, SOURCE, 3, 6)
        assert prefix.end == 6 and prefix.result_ids == ()
        assert discard_confirmed_prefix(registry.get(SOURCE)) == 6
        try:
            segment(registry, SOURCE, 3, 6)
        except ClearedRange:
            pass
        else:
            raise AssertionError('cleared range was still readable')

    with closing(open_warehouse(path)) as db:
        assert {key.point: value for key, value in read(db, baseline).rows.items()} == {'x': 10}
        result_id, = collect_segment(db, registry, SOURCE, 7, 9).result_ids
        ledger = read(db, result_id)
        audit = reconcile(db, result_id, 'east')
        actual = {'rows': {key.point: value for key, value in ledger.rows.items()},
                  'events': [change.number for change in ledger.changes]}
        downstream = {'totals': list(astuple(audit.totals)), 'issues': list(audit.issues)}
        expected = {'rows': {'x': 12}, 'events': [3, 8]}
        downstream_expected = {'totals': [1, 1, 12], 'issues': []}
        if actual != expected or downstream != downstream_expected:
            raise SystemExit(f'WAREHOUSE_ROLLBACK_MISPOST {actual!r} {downstream!r}')
        print('POINTWISE_SCENARIO ' + json.dumps({
            'case_id': 't3.cleared_prefix', 'input': {'sample': 'savepoint.json', 'cleared_through': 6},
            'expected': expected, 'actual': actual,
            'downstream_expected': downstream_expected, 'downstream_actual': downstream,
            'code_paths': ['intake/admission.py', 'staging/replay.py', 'warehouse/publication.py',
                           'review/reconcile.py'],
            'oracle': 'Keep write 3 before S and write 8 after rollback. Both trial writes follow S.',
        }, sort_keys=True))


def main() -> None:
    with TemporaryDirectory(prefix='chengwan-demo-') as directory:
        ordinary(Path(directory) / 'ordinary.sqlite')
        savepoint(Path(directory) / 'savepoint.sqlite')


if __name__ == '__main__':
    main()
