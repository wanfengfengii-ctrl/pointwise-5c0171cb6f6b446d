import sqlite3
from dataclasses import replace

from contracts import ClearedRange, InvalidEvent, ResultNotFound
from feed.retention import discard_confirmed_prefix
from feed.segments import segment
from intake.admission import accept
from intake.progress import get_frontier
from review.reconcile import reconcile
from review.selection import read
from tests.support import (DAY, SOURCE, RejectCommit, WarehouseCase, baseline_events, event,
                           event_numbers, point_values, station_values)
from warehouse.initialize import initialize
from warehouse.ledger import get_latest_ledger
from warehouse.publication import finish


REPLAY = ['intake/admission.py', 'staging/journal.py', 'staging/replay.py', 'staging/savepoints.py']
PUBLISH = ['warehouse/publication.py', 'warehouse/ledger.py', 'warehouse/materialize.py']
ATOMIC = ['warehouse/storage.py', 'warehouse/publication.py', 'intake/admission.py', 'warehouse/ledger.py']


class PublicationTests(WarehouseCase):
    def prepare_cleared_prefix(self):
        self.load_sample('savepoint.json')
        old_id, = self.collect(1, 2).result_ids
        self.assertEqual(self.collect(3, 6).result_ids, ())
        self.assertEqual(discard_confirmed_prefix(self.stream), 6)
        with self.assertRaises(ClearedRange) as failure:
            segment(self.registry, SOURCE, 3, 6)
        self.assertEqual([item.number for item in self.stream.events], [7, 8, 9])
        self.reopen()
        self.assertEqual(self.publication_ids(), [old_id])
        self.assertEqual(point_values(read(self.db, old_id)), {'x': 10})
        return old_id, type(failure.exception).__name__

    def test_t3_cleared_prefix(self):
        old_id, cleared_error = self.prepare_cleared_prefix()
        result_id, = self.collect(7, 9).result_ids
        ledger = read(self.db, result_id)
        review = reconcile(self.db, result_id, 'east')
        expected = [{'x': 12}, [3, 8]]
        actual = [point_values(ledger), event_numbers(ledger)]
        self.assertEqual(actual, expected, 'WAREHOUSE_ROLLBACK_MISPOST')
        self.assertEqual([change.reading for change in ledger.changes], [11, 12])
        self.scenario('t3', {'writes': [[3, 'x', 11], [5, 'x', 99], [6, 'y', 7], [8, 'x', 12]],
                             'savepoint': 4, 'rollback': 7}, expected, actual,
                      [[1, 1, 12], []], [station_values(review.totals), list(review.issues)],
                      REPLAY + PUBLISH, 'Only writes 3 and 8 survive the rollback. Their ordered application leaves x=12.')
        self.scenario('t3.cleared_prefix', {'cleared_through': 6}, ['ClearedRange', {'x': 10}],
                      [cleared_error, point_values(read(self.db, old_id))], expected, actual,
                      ['feed/retention.py', 'intake/collector.py'] + REPLAY + PUBLISH,
                      'The prefix cannot be reread and the old result remains x=10 after the new result is published.')

    def test_t3_delete_identity(self):
        self.register((
            event(1, 'baseline', 'write', point='x', reading=10),
            event(2, 'baseline', 'write', point='z', reading=3),
            event(3, 'baseline', 'commit'),
            event(4, 'trial', 'write', point='x', reading=11),
            event(5, 'trial', 'savepoint', savepoint='S'),
            event(6, 'trial', 'delete', point='x'),
            event(7, 'trial', 'write', point='y', reading=7),
            event(8, 'trial', 'rollback_to', savepoint='S'),
            event(9, 'trial', 'write', point='x', reading=12),
            event(10, 'trial', 'delete', point='z'),
            event(11, 'trial', 'commit'),
        ))
        self.collect(1, 7)
        discard_confirmed_prefix(self.stream)
        self.reopen()
        result_id, = self.collect(8, 11).result_ids
        ledger = read(self.db, result_id)
        changes = [[item.number, item.kind.value, item.key.point] for item in ledger.changes]
        self.scenario('t3.delete_identity', {'trial_delete': 6, 'kept_delete': 10},
                      [[4, 'write', 'x'], [9, 'write', 'x'], [10, 'delete', 'z']], changes,
                      [{'x': 12}, []], [point_values(ledger), list(reconcile(self.db, result_id, 'east').issues)],
                      REPLAY + PUBLISH, 'Rollback excludes trial delete 6 and trial write 7. The later delete removes z.')

    def test_t4_repeated_rollback_to_recent_same_name(self):
        events = (
            event(1, 'nested', 'savepoint', savepoint='S'),
            event(2, 'nested', 'write', point='x', reading=11),
            event(3, 'nested', 'savepoint', savepoint='S'),
            event(4, 'nested', 'write', point='y', reading=7),
            event(5, 'nested', 'rollback_to', savepoint='S'),
            event(6, 'nested', 'write', point='z', reading=8),
            event(7, 'nested', 'rollback_to', savepoint='S'),
            event(8, 'nested', 'commit'),
        )
        outcomes, downstream = [], []
        for split in (4, 6):
            with self.subTest(split=split):
                self.path = self.path.with_name(f'nested-{split}.sqlite')
                self.reopen()
                initialize(self.db)
                self.register(events)
                self.collect(1, split)
                discard_confirmed_prefix(self.stream)
                self.reopen()
                result_id, = self.collect(split + 1, 8).result_ids
                ledger = read(self.db, result_id)
                outcomes.append([point_values(ledger), event_numbers(ledger)])
                downstream.append(station_values(reconcile(self.db, result_id, 'east').totals))
        self.scenario('t4', {'split_after': [4, 6], 'rollback_events': [5, 7]},
                      [[{'x': 11}, [2]], [{'x': 11}, [2]]], outcomes,
                      [[1, 1, 11], [1, 1, 11]], downstream, REPLAY + PUBLISH,
                      'Both rollbacks target savepoint 3, leaving event 2 before it in both segmentations.')

    def test_t4_outer_rollback_closes_inner_scope(self):
        events = (
            event(1, 'nested', 'savepoint', savepoint='outer'),
            event(2, 'nested', 'write', point='x', reading=11),
            event(3, 'nested', 'savepoint', savepoint='inner'),
            event(4, 'nested', 'write', point='y', reading=7),
            event(5, 'nested', 'rollback_to', savepoint='outer'),
            event(6, 'nested', 'write', point='z', reading=8),
            event(7, 'nested', 'commit'),
        )
        self.register(events)
        self.collect(1, 5)
        discard_confirmed_prefix(self.stream)
        self.reopen()
        batch = segment(self.registry, SOURCE, 6, 7)
        invalid = replace(batch, events=(event(6, 'nested', 'rollback_to', savepoint='inner'), events[6]))
        with self.assertRaises(InvalidEvent) as failure:
            accept(self.db, invalid)
        before = [get_frontier(self.db, SOURCE), [item.number for item in self.pending('nested')]]
        result_id, = self.collect(6, 7).result_ids
        ledger = read(self.db, result_id)
        self.scenario('t4.inner_scope_closed', {'outer_rollback': 5, 'illegal_inner_rollback': 6},
                      ['InvalidEvent', [5, [1, 2, 3, 4, 5]]], [type(failure.exception).__name__, before],
                      [{'z': 8}, [6]], [point_values(ledger), event_numbers(ledger)], REPLAY + PUBLISH,
                      'Rollback to the outer point ends the inner point. Resuming at 6 can only keep the new z write.')

    def test_t5_abort_and_following_transaction(self):
        self.register(baseline_events() + (
            event(3, 'trial', 'write', point='x', reading=23),
            event(4, 'trial', 'write', point='trial_only', reading=9),
            event(5, 'trial', 'abort'),
            event(6, 'next', 'write', point='y', reading=7),
            event(7, 'next', 'commit'),
        ))
        self.collect(1, 4)
        discard_confirmed_prefix(self.stream)
        self.reopen()
        aborted = self.collect(5, 5)
        self.assertIsNone(finish(self.db, aborted.terminals[0]))
        self.scenario('t5', {'provisional_x': 23, 'abort': 5, 'cleared_through': 4}, [[], 5, [1], 1],
                      [list(aborted.result_ids), get_frontier(self.db, SOURCE), self.publication_ids(),
                       self.publication_receipt_count()], [{'x': 10}, 0],
                      [point_values(get_latest_ledger(self.db, DAY)), len(self.pending('trial'))],
                      REPLAY + PUBLISH, 'Abort confirms the source through 5, clears its trial and leaves result 1 alone.')
        result_id, = self.collect(6, 7).result_ids
        next_ledger = read(self.db, result_id)
        self.scenario('t5.following_transaction', {'next_write': [6, 'y', 7]}, [{'x': 10, 'y': 7}, [6]],
                      [point_values(next_ledger), event_numbers(next_ledger)], [[1, 2], 2],
                      [self.publication_ids(), self.publication_receipt_count()], PUBLISH,
                      'The next transaction starts from x=10 and adds y=7 without any trial data.')

    def test_t6_retry_after_failed_sqlite_commit(self):
        old_id, _ = self.prepare_cleared_prefix()
        with RejectCommit(self.db) as rejection:
            with self.assertRaisesRegex(sqlite3.DatabaseError, 'not authorized'):
                self.collect(7, 9)
        self.assertEqual(rejection.attempts, 1)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(self.stream.acknowledged_through, 6)
        self.reopen()
        with self.assertRaises(ResultNotFound):
            read(self.db, 2)
        before = [get_frontier(self.db, SOURCE), [item.number for item in self.pending('correction')],
                  self.publication_ids(), self.publication_receipt_count()]
        original = self.collect(7, 9)
        result_id, = original.result_ids
        self.reopen()
        retry = self.collect(7, 9)
        self.assertEqual(original, retry)
        self.scenario('t6', {'failed_sqlite_commits': rejection.attempts, 'retried_range': [7, 9]},
                      [6, [3, 4, 5, 6], [old_id], 1], before, [2, [1, 2], 2],
                      [result_id, self.publication_ids(), self.publication_receipt_count()], ATOMIC,
                      'One denied COMMIT changes no durable state. A successful retry adds exactly one ledger and receipt.')
        ledger = read(self.db, result_id)
        self.scenario('t6.retry_after_reopen', {'old_prefix_removed': 6}, [{'x': 12}, [3, 8]],
                      [point_values(ledger), event_numbers(ledger)], [9, {'x': 10}, 9],
                      [get_frontier(self.db, SOURCE), point_values(read(self.db, old_id)),
                       ledger.receipt.terminal_number], ATOMIC + ['staging/journal.py'],
                      'After reopen the successful retry uses the original pending evidence and signs terminal 9 once.')

    def test_t6_finish_receipt_retry(self):
        self.prepare_cleared_prefix()
        receipt = self.collect(7, 9)
        terminal, = receipt.terminals
        result_id, = receipt.result_ids
        original = read(self.db, result_id)
        self.reopen()
        self.assertEqual(self.pending('correction'), ())
        with self.assertRaises(InvalidEvent):
            finish(self.db, replace(terminal, transaction_id='different'))
        redelivered = finish(self.db, terminal)
        self.assertEqual(redelivered, original)
        self.assertEqual(self.collect(7, 9), receipt)
        self.scenario('t6.finish_receipt_retry', {'terminal': 9, 'result': 2}, [2, [3, 8], 9],
                      [redelivered.result_id, event_numbers(redelivered), redelivered.receipt.terminal_number],
                      [[1, 2], 2], [self.publication_ids(), self.publication_receipt_count()], ATOMIC,
                      'The matching original terminal retrieves result 2 after its journal was closed.')

    def test_serial_transactions_can_share_a_segment(self):
        self.register(baseline_events() + (
            event(3, 'second', 'write', point='x', reading=12), event(4, 'second', 'commit'),
            event(5, 'third', 'write', point='y', reading=7),
        ))
        first = self.collect(1, 1)
        self.assertEqual(first.result_ids, ())
        batch = segment(self.registry, SOURCE, 2, 5)
        accepted = accept(self.db, batch)
        self.assertEqual(accepted.result_ids, (1, 2))
        self.assertEqual([entry.number for entry in accepted.terminals], [2, 4])
        self.assertEqual(point_values(read(self.db, 1)), {'x': 10})
        self.assertEqual(point_values(read(self.db, 2)), {'x': 12})
        self.assertEqual([item.number for item in self.pending('third')], [5])
        self.assertEqual(accept(self.db, batch), accepted)

    def test_invalid_tail_rolls_back_an_earlier_terminal_in_the_same_segment(self):
        self.register(baseline_events() + (event(3, 'unopened', 'commit'),))
        with self.assertRaises(InvalidEvent):
            self.collect(1, 3)
        self.assertEqual(get_frontier(self.db, SOURCE), 0)
        self.assertEqual(self.publication_ids(), [])
        self.assertEqual(self.publication_receipt_count(), 0)
        self.assertEqual(self.pending('baseline'), ())
        self.assertEqual(self.collect(1, 2).result_ids, (1,))
