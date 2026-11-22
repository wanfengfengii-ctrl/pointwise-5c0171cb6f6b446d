from dataclasses import dataclass

from contracts import InvalidEvent


@dataclass(frozen=True)
class Savepoint:
    name: str
    number: int


class SavepointStack:
    def __init__(self) -> None:
        self._active: list[Savepoint] = []

    def push(self, name: str, number: int) -> None:
        self._active.append(Savepoint(name, number))

    def rollback_to(self, name: str) -> Savepoint:
        for index in range(len(self._active) - 1, -1, -1):
            target = self._active[index]
            if target.name == name:
                del self._active[index + 1:]
                return target
        raise InvalidEvent(f'no active savepoint named {name!r}')

    @property
    def active(self) -> tuple[Savepoint, ...]:
        return tuple(self._active)
