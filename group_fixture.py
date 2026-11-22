import json
from contextlib import closing
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

from contracts import ReadingKey
from feed.events import SourceEvent
from feed.registry import SourceRegistry
from feed.retention import discard_confirmed_prefix
from feed.segments import SourceSegment, segment
from intake.collector import collect_segment
from review.selection import read
from tests.support import ResponseLost
from warehouse.initialize import initialize
from warehouse.storage import open_warehouse


FIXTURE = Path(__file__).resolve().parent / "samples" / "group_corrections.json"


@dataclass(frozen=True)
class CorrectionCase:
    group_id: str
    registry: SourceRegistry
    base: SourceSegment
    corrections: tuple[tuple[SourceSegment, dict[int, tuple[bool, int | None]]], ...]


def load_case(path=FIXTURE):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    registry = SourceRegistry()

    def load_segment(record):
        events = tuple(SourceEvent(**fields) for fields in record["events"])
        source_id = events[0].source_id
        registry.register(source_id, events)
        return segment(registry, source_id, record["start"], record["end"], record["segment_id"])

    base = load_segment(data["base"])
    corrections = tuple((load_segment(record), {int(n): tuple(image) for n, image in record["before"].items()})
                        for record in data["corrections"])
    return CorrectionCase(data["group_id"], registry, base, corrections)


def drop_reply(call):
    @wraps(call)
    def wrapper(*args, **kwargs):
        call(*args, **kwargs)
        raise ResponseLost("reply lost after successful call")

    return wrapper


def main():
    case = load_case()
    with closing(open_warehouse(":memory:")) as db:
        initialize(db)
        base = case.base
        base_id, = collect_segment(db, case.registry, base.source_id, base.start, base.end,
                                   base.segment_id).result_ids
        key = ReadingKey(base.events[0].day, "east", "x")
        baseline = read(db, base_id).rows[key]
        assert baseline == 0
        ordinary = []
        for batch, before in case.corrections:
            result_id, = collect_segment(db, case.registry, batch.source_id, batch.start, batch.end,
                                         batch.segment_id).result_ids
            removed_through = discard_confirmed_prefix(case.registry.get(batch.source_id))
            assert removed_through == batch.end
            ordinary.append((batch.source_id, before[1], read(db, result_id).rows[key]))
        assert ordinary == [("A", (True, 0), 1), ("B", (True, 0), 2), ("C", (True, 2), 0)]
        print(json.dumps({"opening_x": baseline, "v1_ordinary_publications": ordinary}, ensure_ascii=False))


if __name__ == "__main__":
    main()
