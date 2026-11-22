from dataclasses import replace

from contracts import InvalidQuery, ResultNotFound
from feed.events import load_events
from review.provenance import check_provenance
from review.reconcile import reconcile
from review.selection import read
from review.station_totals import totals
from tests.support import (DAY, SAMPLES, SOURCE, WarehouseCase, event, event_numbers, point_values,
                           station_values)
from warehouse.initialize import initialize


REVIEW = ['review/selection.py', 'review/station_totals.py', 'review/reconcile.py', 'warehouse/ledger.py']
PROVENANCE = ['review/provenance.py', 'review/changes.py', 'review/reconcile.py', 'warehouse/ledger.py']


class ReviewTests(WarehouseCase):
    def test_t7_zero_and_missing(self):
        self.load_sample('ordinary.json')
        result_id, = self.collect(1, 3).result_ids
        ledger = read(self.db, result_id)
        audit = reconcile(self.db, result_id, 'east')
        self.assertEqual(audit.receipt, ledger.receipt)
        self.assertEqual(audit.totals, totals(self.db, result_id, 'east'))
        self.scenario('t7', {'east': [0, None]}, {'zero': 0, 'missing': None}, point_values(ledger),
                      [[2, 1, 0], [1, 2], []],
                      [station_values(audit.totals), [item.number for item in audit.changes], list(audit.issues)],
                      REVIEW, 'Two records exist and the only measured reading is zero.')

    def test_t7_empty_values(self):
        self.register((event(1, 'blank', 'write', point='x'), event(2, 'blank', 'write', point='y'),
                       event(3, 'blank', 'commit')))
        result_id, = self.collect(1, 3).result_ids
        self.scenario('t7.empty_values', {'east': [None, None], 'west': []},
                      [[2, 0, None], [0, 0, None]],
                      [station_values(totals(self.db, result_id, station)) for station in ('east', 'west')],
                      {'x': None, 'y': None}, point_values(read(self.db, result_id)), REVIEW,
                      'The two east rows are both missing and west has no rows. Both sums are absent.')

    def test_t7_large_integer_sum_and_station_filter(self):
        large = 2**62
        negative = -(2**80)
        self.register((
            event(1, 'large', 'write', point='x', reading=large),
            event(2, 'large', 'write', point='y', reading=large),
            event(3, 'large', 'write', point='z'),
            event(4, 'large', 'write', station='west', point='x', reading=negative),
            event(5, 'large', 'commit'),
        ))
        result_id, = self.collect(1, 5).result_ids
        self.reopen()
        self.scenario('t7.large_integer_sum', {'east': [large, large, None], 'west': [negative]},
                      [3, 2, 2**63], station_values(totals(self.db, result_id, 'east')),
                      [1, 1, negative], station_values(totals(self.db, result_id, 'west')), REVIEW,
                      'Two powers of 2 sum to the next power of 2. The west reading applies only to west.')

    def publish_zero_correction(self):
        events = load_events(SAMPLES / 'ordinary.json') + (
            event(4, 'correction', 'write', point='zero', reading=5),
            event(5, 'correction', 'commit'),
        )
        self.register(events)
        old_id, = self.collect(1, 3).result_ids
        old = read(self.db, old_id)
        new_id, = self.collect(4, 5).result_ids
        new = read(self.db, new_id)
        self.registry = self.stream = None
        return old, new

    def test_t8_fixed_results_and_repeated_initialization(self):
        old, new = self.publish_zero_correction()
        self.db.execute('UPDATE settings SET value = ? WHERE key = ?', ('北楼交班', 'review_label'))
        self.db.execute('DELETE FROM settings WHERE key = ?', ('centre_name',))
        self.reopen()
        initialize(self.db)
        initialize(self.db)
        old_again, new_again = read(self.db, old.result_id), read(self.db, new.result_id)
        self.assertEqual((old_again, new_again), (old, new))
        self.scenario('t8', {'old': 1, 'new': 2, 'source_online': False},
                      [{'zero': 0, 'missing': None}, {'zero': 5, 'missing': None}],
                      [point_values(old_again), point_values(new_again)], [[1, 2], [4], 3, 5],
                      [event_numbers(old_again), event_numbers(new_again), old_again.receipt.terminal_number,
                       new_again.receipt.terminal_number], REVIEW,
                      'The dated correction creates its own ledger and receipt while the first ledger remains immutable.')
        settings = dict(self.db.execute('SELECT key, value FROM settings'))
        self.scenario('t8.initialize_preserves_configuration', {'initializations_after_reopen': 2},
                      {'centre_name': '澄湾观测资料中心', 'review_label': '北楼交班'}, settings,
                      [1, 2], self.publication_ids(), ['warehouse/initialize.py', 'warehouse/ledger.py'],
                      'A missing default is restored, the existing custom label survives and both ledgers remain readable.')

    def test_t8_initialize_preserves_pending_transaction_and_frontier(self):
        self.load_sample('savepoint.json')
        self.collect(1, 6)
        before = self.pending('correction')
        self.reopen()
        initialize(self.db)
        self.assertEqual(self.pending('correction'), before)
        result_id, = self.collect(7, 9).result_ids
        self.assertEqual(event_numbers(read(self.db, result_id)), [3, 8])

    def test_t8_unknown_result_and_invalid_query(self):
        self.publish_zero_correction()
        self.reopen()
        failures = []
        for query, args in ((read, (999,)), (totals, (999, 'east')),
                            (read, (True,)), (totals, (1, '  '))):
            with self.assertRaises((ResultNotFound, InvalidQuery)) as failure:
                query(self.db, *args)
            failures.append(type(failure.exception).__name__)
        self.scenario('t8.unknown_result', {'unknown_id': 999, 'invalid_id': True, 'invalid_station': '  '},
                      ['ResultNotFound', 'ResultNotFound', 'InvalidQuery', 'InvalidQuery'], failures,
                      [1, 2], self.publication_ids(), REVIEW,
                      'Only 1 and 2 exist. A boolean ID and a blank station are invalid query parameters.')

    def test_t8_old_zero(self):
        old, new = self.publish_zero_correction()
        self.reopen()
        initialize(self.db)
        reports = [reconcile(self.db, result_id, 'east') for result_id in (old.result_id, new.result_id)]
        self.assertEqual([report.receipt for report in reports], [old.receipt, new.receipt])
        self.assertEqual([report.changes for report in reports], [old.changes, new.changes])
        self.scenario('t8.old_zero', {'old_zero': 0, 'new_zero': 5}, [[2, 1, 0], [2, 1, 5]],
                      [station_values(report.totals) for report in reports], [[], []],
                      [list(report.issues) for report in reports], REVIEW + ['review/provenance.py'],
                      'The single measured value is zero in result 1 and five in result 2. Both results have a missing row.')

    def test_t8_fixed_provenance(self):
        self.register((
            event(1, 'first', 'write', point='x', reading=9),
            event(2, 'first', 'write', point='x', reading=0),
            event(3, 'first', 'write', point='z', reading=4),
            event(4, 'first', 'delete', point='z'),
            event(5, 'first', 'write', point='y'),
            event(6, 'first', 'commit'),
            event(7, 'second', 'write', point='x', reading=5),
            event(8, 'second', 'commit'),
        ))
        old_id, = self.collect(1, 6).result_ids
        new_id, = self.collect(7, 8).result_ids
        self.registry = self.stream = None
        self.reopen()
        ledger = read(self.db, old_id)
        audit = reconcile(self.db, old_id, 'east')
        operations = [[change.number, change.kind.value, change.key.point, change.reading]
                      for change in audit.changes]
        self.assertEqual([(change.source_id, change.transaction_id) for change in audit.changes],
                         [(SOURCE, 'first')] * 5)
        self.assertEqual(audit.receipt.terminal_number, 6)
        self.assertEqual(point_values(read(self.db, new_id)), {'x': 5, 'y': None})
        self.scenario('t8.fixed_provenance', {'old_terminal': 6, 'current_terminal': 8},
                      [[1, 'write', 'x', 9], [2, 'write', 'x', 0], [3, 'write', 'z', 4],
                       [4, 'delete', 'z', None], [5, 'write', 'y', None]], operations,
                      [{'x': 0, 'y': None}, [2, 1, 0], []],
                      [point_values(ledger), station_values(audit.totals), list(audit.issues)], PROVENANCE,
                      'Final write 2 leaves x=0, delete 4 removes z and write 5 preserves a missing y row.')

        altered = replace(ledger, rows=read(self.db, new_id).rows)
        self.assertTrue(check_provenance(altered, 'east'))
        reversed_changes = replace(ledger, changes=ledger.changes[::-1])
        self.assertTrue(check_provenance(reversed_changes, 'east'))
        wrong_origin = replace(ledger, changes=(replace(ledger.changes[0], source_id='other'), *ledger.changes[1:]))
        self.assertTrue(check_provenance(wrong_origin, 'east'))

    def test_new_date_uses_an_independent_prior_result(self):
        other_day = '2026-05-09'
        self.register((
            event(1, 'prior', 'write', point='x', reading=10), event(2, 'prior', 'commit'),
            event(3, 'next_date', 'write', day=other_day, point='y', reading=5),
            event(4, 'next_date', 'commit', day=other_day),
        ))
        first, second = self.collect(1, 4).result_ids
        self.assertEqual(read(self.db, first).day, DAY)
        self.assertEqual(read(self.db, second).day, other_day)
        self.assertEqual(point_values(read(self.db, first)), {'x': 10})
        self.assertEqual(point_values(read(self.db, second)), {'y': 5})
