import json
from contextlib import closing
from dataclasses import astuple
from pathlib import Path
from tempfile import TemporaryDirectory

from contracts import ClearedRange, ReadingKey
from feed.events import load_events
from feed.registry import SourceRegistry
from feed.retention import acknowledge, discard_confirmed_prefix
from feed.segments import segment
from intake.admission import accept_correction
from intake.collector import collect_segment
from review.reconcile import reconcile
from review.selection import read
from review.station_totals import totals
from warehouse.groups import publish_group, read_group
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


def corrections(path: Path) -> None:
    import group_fixture
    case = group_fixture.load_case()
    key = ReadingKey(case.base.events[0].day, 'east', 'x')
    base = case.base
    with closing(open_warehouse(path)) as db:
        initialize(db)
        base_id, = collect_segment(db, case.registry, base.source_id, base.start, base.end,
                                   base.segment_id).result_ids
        opening = read(db, base_id).rows[key]
        cleared = []
        for batch, before in case.corrections:
            receipt = accept_correction(db, case.group_id, batch, before)
            acknowledge(case.registry.get(batch.source_id), receipt)
            cleared.append([batch.source_id,
                            discard_confirmed_prefix(case.registry.get(batch.source_id)),
                            read(db, base_id).rows[key]])
        received = [opening] + [item[2] for item in cleared]
        assert opening == 0 and all(item[2] == 0 for item in cleared)

    with closing(open_warehouse(path)) as db:
        members = [(batch.source_id, batch.segment_id) for batch, _ in case.corrections]
        result = publish_group(db, case.group_id, base_id, members)
        assert result['status'] == 'published'
        sequence = [read(db, member['result_id']).rows[key] for member in result['members']]
        assert sequence == [2, 0, 1]
        assert [member['before'] for member in result['members']] == [
            [(1, (True, 0))], [(1, (True, 2))], [(1, (True, 0))]]
        base_rows = read(db, base_id).rows[key]
        refetched = read_group(db, case.group_id)
        swapped = publish_group(db, case.group_id, base_id, list(reversed(members)))
        tail = read(db, result['members'][-1]['result_id'])
        tail_rows = {key_.point: value for key_, value in tail.rows.items()
                     if key_.station == 'east'}
        print('POINTWISE_SCENARIO ' + json.dumps({
            'case_id': 'group_corrections',
            'input': {'base': base_id, 'group': case.group_id,
                      'received': cleared},
            'expected': {'opening': received, 'order': ['B', 'C', 'A'], 'x': [2, 0, 1],
                         'before': [[[1, [True, 0]]], [[1, [True, 2]]], [[1, [True, 0]]]]},
            'actual': {'opening': received,
                       'order': [member['source_id'] for member in result['members']],
                       'x': sequence,
                       'before': [[[number, list(image)] for number, image in member['before']]
                                  for member in result['members']]},
            'downstream_expected': {'base_x': 0, 'tail': {'x': 1}, 'refetch': True,
                                    'swapped_retake': True, 'issues': []},
            'downstream_actual': {'base_x': base_rows, 'tail': tail_rows,
                                  'refetch': refetched == result,
                                  'swapped_retake': swapped == result,
                                  'issues': list(reconcile(db, tail.result_id, 'east').issues)},
            'code_paths': ['intake/corrections.py', 'intake/admission.py', 'warehouse/groups.py',
                           'warehouse/publication.py', 'feed/retention.py'],
            'oracle': 'Opening x is 0; B then C then A post 2, 0, 1; the base stays 0; '
                      'the cleared sources can be re-published only through the group receipt.',
        }, ensure_ascii=False, sort_keys=True))


def main() -> None:
    with TemporaryDirectory(prefix='chengwan-demo-') as directory:
        ordinary(Path(directory) / 'ordinary.sqlite')
        savepoint(Path(directory) / 'savepoint.sqlite')
        corrections(Path(directory) / 'corrections.sqlite')


if __name__ == '__main__':
    main()
