"""Joint publication of correction groups against a chosen current day ledger.

A successful call posts one ledger per specified member, in an order that
honours every member's preimage conditions, and records the group's single
first execution.  Later calls with the same identity return that receipt;
different identities are conflicts.
"""
import json
from sqlite3 import Connection

from contracts import InvalidQuery, ReadingKey, ResultNotFound, valid_name, valid_number
from feed.events import EventKind
from intake.corrections import StoredCorrection, decode_body, get_stored, member_plan
from staging.operations import ReadingOperation
from warehouse.ledger import PublishedLedger, get_ledger, get_latest_ledger, store_ledger
from warehouse.materialize import MaterializedLedger
from warehouse.storage import write_transaction


class _Conflict(Exception):
    pass


class _Unsatisfiable(Exception):
    pass


class _Pending(Exception):
    pass


def _normalize_request(group_id, base_id, members):
    if not valid_name(group_id):
        raise InvalidQuery('group identity must be nonempty text')
    if not valid_number(base_id):
        raise InvalidQuery('base result must be a positive SQLite integer')
    if not isinstance(members, (list, tuple)) or not members:
        raise InvalidQuery('members must be a nonempty list of (source, segment) pairs')
    seen_sources = set()
    seen_pairs = set()
    normalized = []
    for member in members:
        if not isinstance(member, (list, tuple)) or len(member) != 2:
            raise InvalidQuery('each member must be a (source_id, segment_id) pair')
        source_id, segment_id = member
        if not valid_name(source_id) or not valid_name(segment_id):
            raise InvalidQuery('member identities must be nonempty text')
        identity = source_id, segment_id
        if source_id in seen_sources or identity in seen_pairs:
            raise InvalidQuery(f'source {source_id} is listed more than once')
        seen_sources.add(source_id)
        seen_pairs.add(identity)
        normalized.append(identity)
    return group_id, base_id, tuple(sorted(normalized))


def _request_body(group_id: str, base_id: int,
                  members: tuple[tuple[str, str], ...]) -> str:
    return json.dumps({'group_id': group_id, 'base_id': base_id,
                       'members': [list(member) for member in members]},
                      ensure_ascii=True, separators=(',', ':'))


def _current_base(db: Connection, base_id: int) -> PublishedLedger:
    try:
        ledger = get_ledger(db, base_id)
    except ResultNotFound:
        raise _Conflict(f'no published result {base_id}') from None
    latest = get_latest_ledger(db, ledger.day)
    if latest is None or latest.result_id != base_id:
        raise _Conflict(f'result {base_id} is no longer the current ledger for its day')
    return ledger


def _collected_members(db: Connection, group_id: str, base: PublishedLedger,
                       members: tuple[tuple[str, str], ...]):
    stored: dict[tuple[str, str], StoredCorrection] = {}
    pending = False
    conflict = None
    for source_id, segment_id in members:
        record = get_stored(db, source_id, segment_id)
        if record is None:
            ordinary = db.execute(
                'SELECT 1 FROM accepted_segments WHERE source_id = ? AND segment_id = ?',
                (source_id, segment_id)).fetchone()
            if ordinary is not None:
                conflict = conflict or _Conflict(
                    f'{source_id}/{segment_id} is an ordinary accepted segment')
            else:
                pending = True
            continue
        if record.group_id != group_id:
            conflict = conflict or _Conflict(
                f'{source_id}/{segment_id} belongs to group {record.group_id}')
            continue
        if record.day != base.day:
            conflict = conflict or _Conflict(
                f'{source_id}/{segment_id} is for a different day')
            continue
        stored[(source_id, segment_id)] = record
    if conflict is not None:
        raise conflict
    if pending:
        raise _Pending()
    return stored


def _read_row(rows: dict[ReadingKey, int | None],
              operation: ReadingOperation) -> tuple[bool, int | None]:
    if operation.key in rows:
        return True, rows[operation.key]
    return False, None


def _matches(image: tuple[bool, int | None], present: bool, reading: int | None) -> bool:
    expected_present, expected_reading = image
    return expected_present == present and expected_reading == reading


def _apply(operation: ReadingOperation, rows: dict[ReadingKey, int | None]) -> None:
    if operation.kind == EventKind.DELETE:
        rows.pop(operation.key, None)
    else:
        rows[operation.key] = operation.reading


def _apply_member(identity, rows, stored):
    """Check and apply one member in place; return its actual preimages."""
    operations, conditions = member_plan(stored[identity])
    actual = []
    for operation in operations:
        present, reading = _read_row(rows, operation)
        if operation.number in conditions:
            if not _matches(conditions[operation.number], present, reading):
                raise _Unsatisfiable(
                    f'{identity[0]}/{identity[1]} event {operation.number} preimage mismatch')
        actual.append((operation.number, (present, reading)))
        _apply(operation, rows)
    return actual


def _choose_order(base: PublishedLedger, stored):
    """Depth-first search for any full order that honours every condition."""
    identities = tuple(sorted(stored))

    def search(remaining, rows, applied):
        if not remaining:
            return [], dict(applied)
        for identity in remaining:
            working = dict(rows)
            try:
                actual = _apply_member(identity, working, stored)
            except _Unsatisfiable:
                continue
            rest = tuple(item for item in remaining if item != identity)
            found = search(rest, working, {**applied, identity: actual})
            if found is not None:
                return [identity, *found[0]], found[1]
        return None

    found = search(identities, dict(base.rows), {})
    if found is None:
        raise _Unsatisfiable('no legal order honours every preimage condition')
    return found[0], found[1]


def _cumulative_rows(base: PublishedLedger, stored, order, identity) -> dict:
    rows = dict(base.rows)
    for item in order:
        operations, _conditions = member_plan(stored[item])
        for operation in operations:
            _apply(operation, rows)
        if item == identity:
            break
    return rows


def _receipt_body(group_id: str, base_id: int, members) -> str:
    records = [{
        'source_id': source_id,
        'segment_id': segment_id,
        'transaction_id': transaction_id,
        'terminal_number': terminal_number,
        'result_id': result_id,
        'before': [[number, [present, reading]]
                   for number, (present, reading) in before],
    } for source_id, segment_id, transaction_id, terminal_number, result_id, before in members]
    return json.dumps({'status': 'published', 'group_id': group_id, 'base_id': base_id,
                       'members': records}, ensure_ascii=True, separators=(',', ':'))


def _rehydrate(body: str) -> dict:
    payload = json.loads(body)
    members = []
    for record in payload['members']:
        members.append({
            'source_id': record['source_id'],
            'segment_id': record['segment_id'],
            'transaction_id': record['transaction_id'],
            'terminal_number': record['terminal_number'],
            'result_id': record['result_id'],
            'before': [(number, (present, reading))
                       for number, (present, reading) in record['before']],
        })
    return {'status': 'published', 'group_id': payload['group_id'],
            'base_id': payload['base_id'], 'members': members}


def _stored_group(db: Connection, group_id: str):
    return db.execute('SELECT request_body, receipt_body FROM group_publications '
                      'WHERE group_id = ?', (group_id,)).fetchone()


def _publish(db: Connection, group_id: str, base_id: int,
             members: tuple[tuple[str, str], ...]) -> dict:
    body = _request_body(group_id, base_id, members)
    stored_group = _stored_group(db, group_id)
    if stored_group is not None:
        if stored_group[0] != body:
            return {'status': 'conflict'}
        return _rehydrate(stored_group[1])

    try:
        base = _current_base(db, base_id)
        stored = _collected_members(db, group_id, base, members)
    except _Conflict:
        return {'status': 'conflict'}
    except _Pending:
        return {'status': 'pending'}

    try:
        order, applied = _choose_order(base, stored)
    except _Unsatisfiable:
        return {'status': 'unsatisfiable'}

    results = []
    for identity in order:
        record = stored[identity]
        events, _, _ = decode_body(record.request_body)
        contents = MaterializedLedger(rows=_cumulative_rows(base, stored, order, identity),
                                      changes=member_plan(record)[0])
        ledger = store_ledger(db, events[-1], contents)
        results.append((record, ledger.result_id, applied[identity]))

    receipt = _receipt_body(group_id, base_id, [
        (record.source_id, record.segment_id, record.transaction_id,
         record.terminal_number, result_id, before)
        for record, result_id, before in results])
    db.execute('INSERT INTO group_publications (group_id, base_id, request_body, receipt_body) '
               'VALUES (?, ?, ?, ?)', (group_id, base_id, body, receipt))
    return _rehydrate(receipt)


def publish_group(db: Connection, group_id, base_id, members) -> dict:
    group_id, base_id, normalized = _normalize_request(group_id, base_id, members)
    with write_transaction(db):
        return _publish(db, group_id, base_id, normalized)


def read_group(db: Connection, group_id):
    if not valid_name(group_id):
        raise InvalidQuery('group identity must be nonempty text')
    row = _stored_group(db, group_id)
    return None if row is None else _rehydrate(row[1])
