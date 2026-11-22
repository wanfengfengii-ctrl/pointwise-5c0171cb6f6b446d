import sqlite3
from dataclasses import replace

from contracts import InvalidEvent, InvalidQuery, ReadingKey, SegmentConflict, SegmentGap
from feed.events import SourceEvent
from feed.registry import SourceRegistry
from feed.retention import acknowledge, discard_confirmed_prefix
from feed.segments import segment
from intake.admission import accept_correction
from intake.collector import collect_segment
from intake.progress import get_frontier
from review.reconcile import reconcile
from review.selection import read
from tests.support import DAY, RejectCommit, ResponseLost, WarehouseCase
from warehouse.groups import publish_group, read_group
from warehouse.ledger import get_latest_ledger
from warehouse.publication import finish


def correction(source, events, day=DAY):
    return [SourceEvent(source, number, transaction, kind, day,
                        station if point is not None else None, point, reading, savepoint)
            for number, transaction, kind, station, point, reading, savepoint in events]


class GroupCase(WarehouseCase):
    def setUp(self):
        super().setUp()
        self.registries = {}

    def add_source(self, source_id, events):
        if source_id not in self.registries:
            self.registries[source_id] = SourceRegistry()
            self.registries[source_id].register(source_id, events)
        return self.registries[source_id]

    def batch(self, source_id, start, end, segment_id=None):
        return segment(self.registries[source_id], source_id, start, end, segment_id)

    def ordinary(self, source_id, start, end, segment_id=None):
        registry = self.registries[source_id]
        return collect_segment(self.db, registry, source_id, start, end, segment_id).result_ids

    def correction(self, group_id, source_id, start, end, before, segment_id=None):
        batch = self.batch(source_id, start, end, segment_id)
        return accept_correction(self.db, group_id, batch, before)

    def key(self, point='x', station='east', day=DAY):
        return ReadingKey(day, station, point)


class FixtureTests(GroupCase):
    """The worked handover sample from GROUP_CORRECTIONS.md."""

    def setUp(self):
        super().setUp()
        import group_fixture
        self.case = group_fixture.load_case()
        self.registries = {}
        for source in ('base', 'A', 'B', 'C'):
            self.registries[source] = self.case.registry

    def load(self):
        base = self.case.base
        self.base_id, = self.ordinary(base.source_id, base.start, base.end, base.segment_id)
        self.assertEqual(read(self.db, self.base_id).rows[self.key()], 0)
        for batch, before in self.case.corrections:
            receipt = accept_correction(self.db, self.case.group_id, batch, before)
            self.assertEqual(receipt.result_ids, ())
            acknowledge(self.case.registry.get(batch.source_id), receipt)
            self.assertEqual(discard_confirmed_prefix(self.case.registry.get(batch.source_id)),
                             batch.end)

    def members(self):
        return [(batch.source_id, batch.segment_id) for batch, _ in self.case.corrections]

    def test_handover_satisfiable_order_and_receipt(self):
        self.load()
        self.assertEqual(read(self.db, self.base_id).rows[self.key()], 0)
        self.reopen()
        result = publish_group(self.db, self.case.group_id, self.base_id, self.members())
        self.assertEqual(result['status'], 'published')
        self.assertEqual(result['base_id'], self.base_id)
        self.assertEqual([member['source_id'] for member in result['members']], ['B', 'C', 'A'])
        self.assertEqual([member['before'] for member in result['members']],
                         [[(1, (True, 0))], [(1, (True, 2))], [(1, (True, 0))]])
        values = [read(self.db, member['result_id']).rows[self.key()]
                  for member in result['members']]
        self.assertEqual(values, [2, 0, 1])
        self.assertEqual(read(self.db, self.base_id).rows[self.key()], 0)
        self.assertEqual([member['terminal_number'] for member in result['members']], [2, 2, 2])
        self.assertEqual(read_group(self.db, self.case.group_id), result)
        self.assertEqual(get_latest_ledger(self.db, DAY).result_id,
                         result['members'][-1]['result_id'])
        for member in result['members']:
            self.assertEqual(reconcile(self.db, member['result_id'], 'east').issues, ())

    def test_retake_swapped_members_and_conflicts(self):
        self.load()
        self.reopen()
        first = publish_group(self.db, self.case.group_id, self.base_id, self.members())
        swapped = publish_group(self.db, self.case.group_id, self.base_id,
                                list(reversed(self.members())))
        self.assertEqual(swapped, first)
        self.assertEqual(publish_group(self.db, self.case.group_id, first['members'][0]['result_id'],
                                       self.members()), {'status': 'conflict'})
        self.assertEqual(publish_group(self.db, self.case.group_id, self.base_id,
                                       self.members()[:-1]), {'status': 'conflict'})
        self.reopen()
        self.assertEqual(read_group(self.db, self.case.group_id), first)

    def test_finish_rejects_unpublished_correction(self):
        self.load()
        receipt = accept_correction(self.db, self.case.group_id,
                                    self.case.corrections[0][0],
                                    self.case.corrections[0][1])
        with self.assertRaises(InvalidEvent):
            finish(self.db, receipt.terminals[0])
        self.reopen()
        result = publish_group(self.db, self.case.group_id, self.base_id, self.members())
        member = next(item for item in result['members'] if item['source_id'] == 'A')
        completed = finish(self.db, receipt.terminals[0])
        self.assertEqual(completed.result_id, member['result_id'])
        self.assertEqual(completed.rows[self.key()], 1)

    def test_later_ordinary_transaction_before_and_after_publish(self):
        self.load()
        self.assertEqual(self.ordinary('A', 3, 3), ())
        self.reopen()
        result = publish_group(self.db, self.case.group_id, self.base_id, self.members())
        self.assertEqual(result['status'], 'published')
        after_publication, = self.ordinary('A', 4, 4)
        tail = read(self.db, after_publication)
        self.assertEqual((tail.rows[self.key()], tail.rows[self.key('y')]), (1, 7))

    def test_unpublished_correction_does_not_block_ordinary_slot(self):
        self.load()
        self.assertEqual(self.ordinary('A', 3, 3), ())
        self.assertEqual(read(self.db, self.base_id).rows[self.key()], 0)
        self.reopen()
        late_id, = self.ordinary('A', 4, 4)
        late = read(self.db, late_id)
        self.assertEqual((late.rows[self.key()], late.rows[self.key('y')]), (0, 7))
        self.assertEqual(read(self.db, self.base_id).rows[self.key()], 0)


class CorrectionIntakeTests(GroupCase):
    def setUp(self):
        super().setUp()
        self.add_source('base', correction('base', [(1, 'open', 'write', 'east', 'x', 0, None),
                                                   (2, 'open', 'commit', None, None, None, None)]))
        self.base_id, = self.ordinary('base', 1, 2)
        self.add_source('A', correction('A', [(1, 'a', 'write', 'east', 'x', 5, None),
                                              (2, 'a', 'savepoint', None, None, None, 'S'),
                                              (3, 'a', 'write', 'east', 'x', 9, None),
                                              (4, 'a', 'rollback_to', None, None, None, 'S'),
                                              (5, 'a', 'commit', None, None, None, None)]))

    def test_same_parameter_retake_returns_original_even_after_publish(self):
        batch = self.batch('A', 1, 5)
        original = accept_correction(self.db, 'g', batch, {1: (True, 0)})
        self.assertEqual(accept_correction(self.db, 'g', batch, {1: (True, 0)}), original)
        self.assertEqual(original.result_ids, ())
        self.assertEqual(get_frontier(self.db, 'A'), 5)
        result = publish_group(self.db, 'g', self.base_id, [('A', batch.segment_id)])
        self.assertEqual(result['status'], 'published')
        self.reopen()
        self.assertEqual(accept_correction(self.db, 'g', batch, {1: (True, 0)}), original)
        self.assertEqual(read_group(self.db, 'g'), result)

    def test_conflicting_identity_and_transaction_names(self):
        batch = self.batch('A', 1, 5)
        accept_correction(self.db, 'g', batch, {1: (True, 0)})
        changed = replace(batch, events=(replace(batch.events[0], reading=6), *batch.events[1:]))
        failures = []
        for call in (
            lambda: accept_correction(self.db, 'g', changed, {1: (True, 0)}),
            lambda: accept_correction(self.db, 'other-group', batch, {1: (True, 0)}),
            lambda: accept_correction(self.db, 'g', batch, {1: (True, 5)}),
            lambda: accept_correction(self.db, 'g', batch, {}),
        ):
            with self.assertRaises(SegmentConflict) as failure:
                call()
            failures.append(type(failure.exception).__name__)
        self.assertEqual(failures, ['SegmentConflict'] * 4)
        self.assertEqual(get_frontier(self.db, 'A'), 5)

    def test_invalid_preimage_parameters(self):
        batch = self.batch('A', 1, 5)
        for before in ({True: (True, 0)}, {2: (False, None)}, {1: (True, '0')},
                       {1: (False, 0)}, {1: (1, 0)}, {1: (True,)}, {1: True}):
            with self.assertRaises(InvalidEvent):
                accept_correction(self.db, 'g', batch, before)
        self.assertEqual(get_frontier(self.db, 'A'), 0)
        self.assertEqual(read_group(self.db, 'g'), None)

    def test_segment_identity_shared_with_ordinary_and_gap(self):
        self.add_source('B', correction('B', [(1, 'b', 'write', 'east', 'x', 1, None),
                                              (2, 'b', 'commit', None, None, None, None)]))
        with self.assertRaises(SegmentGap):
            accept_correction(self.db, 'g', self.batch('B', 2, 2, 'tail'), {})
        ordinary_batch = self.batch('B', 1, 2)
        self.ordinary('B', 1, 2)
        with self.assertRaises(SegmentConflict):
            accept_correction(self.db, 'g', ordinary_batch, {})
        self.assertEqual(get_frontier(self.db, 'B'), 2)

    def test_failed_commit_changes_nothing(self):
        with RejectCommit(self.db) as rejection:
            with self.assertRaisesRegex(sqlite3.DatabaseError, 'not authorized'):
                accept_correction(self.db, 'g', self.batch('A', 1, 5), {1: (True, 0)})
        self.assertEqual(rejection.attempts, 1)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(get_frontier(self.db, 'A'), 0)
        self.assertEqual(read_group(self.db, 'g'), None)
        self.reopen()
        receipt = accept_correction(self.db, 'g', self.batch('A', 1, 5), {1: (True, 0)})
        self.assertEqual(receipt.end, 5)

    def test_lost_reply_then_original_retake(self):
        batch = self.batch('A', 1, 5)

        def lose(db_, received):
            accept_correction(db_, 'g', received, {1: (True, 0)})
            raise ResponseLost('reply lost')

        with self.assertRaises(ResponseLost):
            lose(self.db, batch)
        self.assertEqual(get_frontier(self.db, 'A'), 5)
        self.assertEqual(discard_confirmed_prefix(self.registries['A'].get('A')), 0)
        self.reopen()
        receipt = accept_correction(self.db, 'g', self.batch('A', 1, 5), {1: (True, 0)})
        acknowledge(self.registries['A'].get('A'), receipt)
        self.assertEqual(discard_confirmed_prefix(self.registries['A'].get('A')), 5)

    def test_collector_receiver_forwards_correction_confirmation(self):
        from intake.collector import collect_segment
        receipt = collect_segment(self.db, self.registries['A'], 'A', 1, 5,
                                  receiver=lambda db_, batch: accept_correction(
                                      db_, 'g', batch, {1: (True, 0)}))
        self.assertEqual(receipt.end, 5)
        self.assertEqual(self.registries['A'].get('A').acknowledged_through, 5)
        self.assertEqual(discard_confirmed_prefix(self.registries['A'].get('A')), 5)

    def test_ordinary_cannot_reuse_correction_transaction_name(self):
        accept_correction(self.db, 'g', self.batch('A', 1, 5), {1: (True, 0)})
        # A later ordinary segment on the same source cannot reuse the
        # correction's transaction name.
        self.registries['A'] = SourceRegistry()
        self.registries['A'].register('A', correction('A', [
            (1, 'a', 'write', 'east', 'x', 5, None),
            (2, 'a', 'savepoint', None, None, None, 'S'),
            (3, 'a', 'write', 'east', 'x', 9, None),
            (4, 'a', 'rollback_to', None, None, None, 'S'),
            (5, 'a', 'commit', None, None, None, None),
            (6, 'a', 'write', 'east', 'x', 2, None),
            (7, 'a', 'commit', None, None, None, None)]))
        with self.assertRaises(InvalidEvent):
            self.ordinary('A', 6, 7)
        self.assertEqual(get_frontier(self.db, 'A'), 5)

    def test_batch_must_be_one_complete_commit(self):
        self.add_source('B', correction('B', [(1, 'b', 'write', 'east', 'x', 1, None),
                                              (2, 'b', 'write', 'east', 'y', 2, None),
                                              (3, 'b', 'abort', None, None, None, None)]))
        with self.assertRaises(InvalidEvent):
            accept_correction(self.db, 'g', self.batch('B', 1, 3), {})
        self.assertEqual(get_frontier(self.db, 'B'), 0)


class GroupPublicationTests(GroupCase):
    def setUp(self):
        super().setUp()
        self.add_source('base', correction('base', [
            (1, 'open', 'write', 'east', 'x', 0, None),
            (2, 'open', 'commit', None, None, None, None),
            (3, 'next', 'write', 'east', 'x', 7, None),
            (4, 'next', 'commit', None, None, None, None)]))
        self.base_id, = self.ordinary('base', 1, 2)

    def make(self, group_id, source_id, reading, before, transaction='amend',
             start=1, segment_id=None):
        self.add_source(source_id, correction(source_id, [
            (start, transaction, 'write', 'east', 'x', reading, None),
            (start + 1, transaction, 'commit', None, None, None, None)]))
        return self.correction(group_id, source_id, start, start + 1, before,
                               segment_id or f'{source_id}-seg')

    def test_unsatisfiable_and_pending(self):
        self.make('g', 'A', 1, {1: (True, 0)})
        self.assertEqual(publish_group(self.db, 'g', self.base_id,
                                       [('A', 'A-seg'), ('B', 'B-seg')]), {'status': 'pending'})
        self.make('g', 'B', 2, {1: (True, 0)})
        self.assertEqual(publish_group(self.db, 'g', self.base_id,
                                       [('A', 'A-seg'), ('B', 'B-seg')]),
                         {'status': 'unsatisfiable'})

    def test_unconditional_order_and_first_execution(self):
        self.make('g', 'A', 1, None)
        self.make('g', 'B', 0, None)
        first = publish_group(self.db, 'g', self.base_id, [('A', 'A-seg'), ('B', 'B-seg')])
        self.assertEqual(first['status'], 'published')
        order = [member['source_id'] for member in first['members']]
        self.assertIn(order, (['A', 'B'], ['B', 'A']))
        self.assertEqual([member['before'] for member in first['members']],
                         [[(1, (True, 0))], [(1, (True, 1 if order[0] == 'A' else 0))]])
        self.assertEqual(read(self.db, first['members'][-1]['result_id']).rows[self.key()], 0)
        self.assertEqual(publish_group(self.db, 'g', self.base_id,
                                       [('B', 'B-seg'), ('A', 'A-seg')]), first)

    def test_self_consistent_two_members_when_order_irrelevant(self):
        self.make('g', 'A', 1, None)
        self.make('g', 'B', 0, None)
        first = publish_group(self.db, 'g', self.base_id, [['A', 'A-seg'], ['B', 'B-seg']])
        self.assertEqual(first['status'], 'published')
        self.assertEqual(first['members'][0]['before'][0][1][0], True)

    def test_rolled_back_condition_is_ignored(self):
        self.add_source('D', correction('D', [
            (1, 'd', 'write', 'east', 'z', None, None),
            (2, 'd', 'savepoint', None, None, None, 'S'),
            (3, 'd', 'write', 'east', 'x', 99, None),
            (4, 'd', 'rollback_to', None, None, None, 'S'),
            (5, 'd', 'write', 'east', 'z', 0, None),
            (6, 'd', 'delete', 'east', 'z', None, None),
            (7, 'd', 'commit', None, None, None, None)]))
        receipt = accept_correction(self.db, 'g', self.batch('D', 1, 7),
                                    {3: (True, 999), 5: (True, None), 6: (True, 0)})
        result = publish_group(self.db, 'g', self.base_id, [('D', '1:7')])
        self.assertEqual(result['status'], 'published')
        member = result['members'][0]
        self.assertEqual(member['before'], [(1, (False, None)), (5, (True, None)),
                                            (6, (True, 0))])
        ledger = read(self.db, member['result_id'])
        self.assertNotIn(self.key('z'), ledger.rows)
        self.assertEqual(reconcile(self.db, member['result_id'], 'east').issues, ())

    def test_member_without_effective_operations_gets_result(self):
        self.add_source('E', correction('E', [
            (1, 'e', 'savepoint', None, None, None, 'S'),
            (2, 'e', 'write', 'east', 'x', 3, None),
            (3, 'e', 'rollback_to', None, None, None, 'S'),
            (4, 'e', 'commit', None, None, None, None)]))
        accept_correction(self.db, 'g', self.batch('E', 1, 4), {2: (True, 0)})
        result = publish_group(self.db, 'g', self.base_id, [('E', '1:4')])
        self.assertEqual(result['status'], 'published')
        member = result['members'][0]
        self.assertEqual(member['before'], [])
        self.assertEqual(read(self.db, member['result_id']).rows[self.key()], 0)
        self.assertEqual(read(self.db, member['result_id']).changes, ())

    def make_published_single(self):
        self.add_source('P', correction('P', [
            (1, 'p', 'write', 'east', 'x', 1, None),
            (2, 'p', 'commit', None, None, None, None)]))
        accept_correction(self.db, 'g', self.batch('P', 1, 2), None)
        result = publish_group(self.db, 'g', self.base_id, [('P', '1:2')])
        self.assertEqual(result['status'], 'published')
        return result

    def test_published_group_rejects_new_segment(self):
        self.make_published_single()
        self.add_source('N', correction('N', [
            (1, 'n', 'write', 'east', 'x', 1, None),
            (2, 'n', 'commit', None, None, None, None)]))
        with self.assertRaises(SegmentConflict):
            accept_correction(self.db, 'g', self.batch('N', 1, 2), {})

    def test_conflicts_and_invalid_queries(self):
        self.make('g', 'A', 1, {1: (True, 0)}, transaction='amend')
        self.assertEqual(publish_group(self.db, 'g', 999, [('A', 'A-seg')]),
                         {'status': 'conflict'})
        self.assertEqual(publish_group(self.db, 'g', self.base_id, [('A', 'ordinary')]),
                         {'status': 'pending'})
        self.assertEqual(publish_group(self.db, 'g', self.base_id, [('Z', 'Z-seg')]),
                         {'status': 'pending'})
        for members in ([], [('A',)], [('A', 'A-seg'), ('A', 'Other')]):
            with self.assertRaises(InvalidQuery):
                publish_group(self.db, 'g', self.base_id, members)
        with self.assertRaises(InvalidQuery):
            publish_group(self.db, '', self.base_id, [('A', 'A-seg')])
        with self.assertRaises(InvalidQuery):
            publish_group(self.db, 'g', True, [('A', 'A-seg')])
        # A second ordinary accepted segment cannot masquerade as a member.
        self.add_source('F', correction('F', [(1, 'f', 'write', 'east', 'x', 1, None),
                                              (2, 'f', 'commit', None, None, None, None)]))
        self.ordinary('F', 1, 2)
        self.assertEqual(publish_group(self.db, 'g', self.base_id, [('A', 'A-seg'),
                                                                   ('F', '1:2')]),
                         {'status': 'conflict'})

    def test_conflict_wins_over_pending(self):
        self.make('g', 'A', 1, {1: (True, 0)})
        self.assertEqual(publish_group(self.db, 'g', self.base_id,
                                       [('A', 'A-seg'), ('MISSING', 'x')]),
                         {'status': 'pending'})
        self.assertEqual(publish_group(self.db, 'g', self.base_id,
                                       [('A', 'A-seg'), ('Z', 'Z-seg')]),
                         {'status': 'pending'})

    def test_base_must_be_current_and_members_share_day(self):
        self.make('g', 'A', 1, None)
        next_id, = self.ordinary('base', 3, 4)
        self.assertEqual(publish_group(self.db, 'g', self.base_id, [('A', 'A-seg')]),
                         {'status': 'conflict'})
        moved = publish_group(self.db, 'g', next_id, [('A', 'A-seg')])
        self.assertEqual(moved['status'], 'published')
        self.assertEqual(read(self.db, moved['members'][0]['result_id']).rows[self.key()], 1)
        self.add_source('C', correction('C', [
            (1, 'c', 'write', 'east', 'x', 2, None),
            (2, 'c', 'commit', None, None, None, None)], day='2026-05-09'))
        accept_correction(self.db, 'h', self.batch('C', 1, 2), {1: (True, 0)})
        self.assertEqual(publish_group(self.db, 'h', next_id, [('C', '1:2')]),
                         {'status': 'conflict'})

    def test_failed_commit_leaves_no_first_execution(self):
        self.make('g', 'A', 1, None)
        self.make('g', 'B', 0, None)
        members = [('A', 'A-seg'), ('B', 'B-seg')]
        ledgers = self.publication_ids()
        with RejectCommit(self.db) as rejection:
            with self.assertRaisesRegex(sqlite3.DatabaseError, 'not authorized'):
                publish_group(self.db, 'g', self.base_id, members)
        self.assertEqual(rejection.attempts, 1)
        self.assertFalse(self.db.in_transaction)
        self.assertEqual(self.publication_ids(), ledgers)
        self.assertEqual(read_group(self.db, 'g'), None)
        first = publish_group(self.db, 'g', self.base_id, members)
        self.assertEqual(first['status'], 'published')
        self.assertEqual(read_group(self.db, 'g'), first)

    def test_member_ledgers_and_tail_become_current(self):
        self.make('g', 'A', 1, {1: (True, 0)})
        self.add_source('B', correction('B', [
            (1, 'b', 'write', 'east', 'y', 7, None),
            (2, 'b', 'commit', None, None, None, None)]))
        accept_correction(self.db, 'g', self.batch('B', 1, 2), {})
        result = publish_group(self.db, 'g', self.base_id, [('A', 'A-seg'), ('B', '1:2')])
        self.assertEqual(result['status'], 'published')
        first, second = result['members']
        self.assertEqual(first['source_id'], 'A')
        self.assertEqual(read(self.db, second['result_id']).rows,
                         {self.key(): 1, self.key('y'): 7})
        self.assertEqual(get_latest_ledger(self.db, DAY).result_id, second['result_id'])
        self.add_source('N', correction('N', [
            (1, 'n', 'write', 'east', 'z', 4, None),
            (2, 'n', 'commit', None, None, None, None)]))
        follow_id, = self.ordinary('N', 1, 2)
        follow = read(self.db, follow_id)
        self.assertEqual(follow.rows, {self.key(): 1, self.key('y'): 7, self.key('z'): 4})
