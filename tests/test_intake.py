from dataclasses import replace

from contracts import (ClearedRange, InvalidConfirmation, InvalidEvent, SegmentConflict,
                       SegmentGap, UnknownSource)
from feed.retention import acknowledge, discard_confirmed_prefix
from feed.segments import SourceSegment, segment
from intake.admission import accept
from intake.progress import get_frontier
from review.selection import read
from tests.support import SOURCE, ResponseLost, WarehouseCase, baseline_events, event, lose_response, point_values


ADMISSION = ['intake/admission.py', 'intake/progress.py', 'staging/journal.py']
RECEIPTS = ['intake/admission.py', 'intake/receipts.py', 'feed/retention.py']


class IntakeTests(WarehouseCase):
    def test_t1_partial_and_original_redelivery(self):
        self.load_sample('savepoint.json')
        self.assertEqual(self.collect(1, 2).result_ids, (1,))
        batch = segment(self.registry, SOURCE, 3, 6)
        original = accept(self.db, batch)
        self.reopen()
        duplicate = accept(self.db, batch)
        self.assertEqual(original, duplicate)
        self.assertEqual(original.result_ids, ())
        self.scenario('t1', {'range': [3, 6], 'deliveries': 2}, [6, 6, [3, 4, 5, 6]],
                      [duplicate.end, get_frontier(self.db, SOURCE),
                       [item.number for item in self.pending('correction')]],
                      {'x': 10}, point_values(read(self.db, 1)), ADMISSION + RECEIPTS[1:2],
                      'A repeated range has one copy of each original event and creates no new ledger.')

    def test_t1_source_read_errors(self):
        self.load_sample('savepoint.json')
        with self.assertRaises(UnknownSource) as unknown:
            segment(self.registry, 'unregistered', 1, 2)
        receipt = self.collect(1, 6)
        with self.assertRaises(InvalidConfirmation):
            acknowledge(self.stream, replace(receipt, source_id='unregistered', end=7))
        self.assertEqual(self.stream.acknowledged_through, 6)
        self.assertEqual(discard_confirmed_prefix(self.stream), 6)
        with self.assertRaises(ClearedRange) as cleared:
            segment(self.registry, SOURCE, 3, 6)
        suffix = segment(self.registry, SOURCE, 7, 9)
        self.scenario('t1.source_read_errors', {'cleared': 6}, ['UnknownSource', 'ClearedRange'],
                      [type(unknown.exception).__name__, type(cleared.exception).__name__],
                      [7, 8, 9], [item.number for item in suffix.events],
                      ['feed/registry.py', 'feed/segments.py', 'feed/retention.py'],
                      'An unknown source has no range and the retained suffix starts immediately after 6.')

    def test_t1_lost_response(self):
        self.load_sample('savepoint.json')
        with self.assertRaises(ResponseLost):
            self.collect(1, 6, receiver=lose_response)
        self.assertEqual(get_frontier(self.db, SOURCE), 6)
        with self.assertRaises(InvalidConfirmation):
            discard_confirmed_prefix(self.stream, 6)
        before = self.stream.acknowledged_through
        self.reopen()
        receipt = self.collect(1, 6)
        self.assertEqual(receipt.result_ids, (1,))
        self.scenario('t1.lost_response', {'range': [1, 6], 'lost_replies': 1}, [0, 6],
                      [before, receipt.end], [6, [1], [3, 4, 5, 6]],
                      [discard_confirmed_prefix(self.stream), self.publication_ids(),
                       [item.number for item in self.pending('correction')]],
                      RECEIPTS + ['intake/collector.py'],
                      'The dropped reply grants no cleanup. Recovery of the original reply grants exactly 6.')

    def test_t1_late_duplicate(self):
        self.load_sample('savepoint.json')
        original_batch = segment(self.registry, SOURCE, 1, 4)
        original = self.collect(1, 4)
        later = self.collect(5, 6)
        late = accept(self.db, original_batch)
        self.assertEqual(late, original)
        self.assertEqual(acknowledge(self.stream, late), later.end)
        self.scenario('t1.late_duplicate', {'ranges': [[1, 4], [5, 6], [1, 4]]}, [4, 6, 6],
                      [late.end, get_frontier(self.db, SOURCE), self.stream.acknowledged_through],
                      [[1], [3, 4, 5, 6]],
                      [self.publication_ids(), [item.number for item in self.pending('correction')]],
                      RECEIPTS + ['staging/journal.py'], 'Original reply stays at 4, while progress and cleanup reach 6.')

    def test_t1_conflicting_identity(self):
        self.load_sample('savepoint.json')
        self.collect(1, 2)
        batch = segment(self.registry, SOURCE, 3, 6)
        original = accept(self.db, batch)
        changed_reading = replace(batch, events=(replace(batch.events[0], reading=99), *batch.events[1:]))
        changed_control = replace(batch, events=(batch.events[0], replace(batch.events[1], savepoint='T'),
                                                 *batch.events[2:]))
        failures = []
        for changed in (changed_reading, changed_control):
            with self.assertRaises(SegmentConflict) as failure:
                accept(self.db, changed)
            failures.append(type(failure.exception).__name__)
        self.reopen()
        self.assertEqual(accept(self.db, batch), original)
        pending = self.pending('correction')
        self.scenario('t1.conflicting_identity', {'changes': ['reading', 'savepoint']},
                      ['SegmentConflict', 'SegmentConflict'], failures, [6, 11, 'S'],
                      [get_frontier(self.db, SOURCE), pending[0].reading, pending[1].savepoint],
                      RECEIPTS, 'The original x=11 and savepoint S remain bound to the accepted segment identity.')

    def test_t2_gap_before_new_segment(self):
        self.load_sample('savepoint.json')
        self.collect(1, 2)
        with self.assertRaises(SegmentGap) as failure:
            self.collect(4, 6)
        frontier = get_frontier(self.db, SOURCE)
        self.assertEqual(self.pending('correction'), ())
        receipt = self.collect(3, 6)
        self.scenario('t2', {'ranges': [[1, 2], [4, 6], [3, 6]]}, ['SegmentGap', 2],
                      [type(failure.exception).__name__, frontier], 6, receipt.end, ADMISSION,
                      'The event after 2 must be 3, and rejecting the gapped segment leaves 3 available.')

    def test_t2_internal_gap(self):
        events = baseline_events() + (
            event(3, 'next', 'write', point='x', reading=11),
            event(4, 'next', 'write', point='y', reading=7),
            event(5, 'next', 'write', point='z', reading=8),
            event(6, 'next', 'commit'),
        )
        self.register(events)
        self.collect(1, 3)
        gapped = SourceSegment(SOURCE, 'end', 4, 6, (events[3], events[5], events[5]))
        with self.assertRaises(SegmentGap) as failure:
            accept(self.db, gapped)
        before = [get_frontier(self.db, SOURCE), [item.number for item in self.pending('next')]]
        result_id, = self.collect(4, 6, segment_id='end').result_ids
        self.scenario('t2.internal_gap', {'declared': [4, 6], 'received': [4, 6, 6]},
                      ['SegmentGap', [3, [3]]], [type(failure.exception).__name__, before],
                      {'x': 11, 'y': 7, 'z': 8}, point_values(read(self.db, result_id)), ADMISSION,
                      'Count and endpoints agree but position 5 is missing. Only the repaired packet adds y and z.')

    def test_t2_invalid_event_rolls_back_entire_segment(self):
        self.load_sample('savepoint.json')
        self.collect(1, 3)
        batch = segment(self.registry, SOURCE, 4, 6)
        invalid = replace(batch, events=(*batch.events[:-1], replace(batch.events[-1], reading=True)))
        with self.assertRaises(InvalidEvent) as failure:
            accept(self.db, invalid)
        before = [get_frontier(self.db, SOURCE), [item.number for item in self.pending('correction')]]
        self.assertEqual(accept(self.db, batch).end, 6)
        self.scenario('t2.invalid_event', {'invalid_event': 6, 'reading': True},
                      ['InvalidEvent', [3, [3]]], [type(failure.exception).__name__, before],
                      [3, 4, 5, 6], [item.number for item in self.pending('correction')],
                      ADMISSION + ['intake/decoder.py'],
                      'A boolean is invalid even though the position is continuous. A corrected whole segment adds 4 to 6.')
