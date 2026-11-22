from dataclasses import dataclass
from datetime import date


MAX_NUMBER = 2**63 - 1


class WarehouseError(Exception):
    """Base class for observation warehouse contract errors."""


class UnknownSource(WarehouseError):
    pass


class InvalidRange(WarehouseError):
    pass


class ClearedRange(WarehouseError):
    pass


class InvalidConfirmation(WarehouseError):
    pass


class InvalidEvent(WarehouseError):
    pass


class SegmentGap(WarehouseError):
    pass


class SegmentConflict(WarehouseError):
    pass


class InvalidQuery(WarehouseError):
    pass


class ResultNotFound(WarehouseError):
    pass


@dataclass(frozen=True, order=True)
class ReadingKey:
    day: str
    station: str
    point: str


def valid_name(value: object) -> bool:
    if type(value) is not str or not value.strip() or '\0' in value:
        return False
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        return False
    return True


def valid_number(value: object) -> bool:
    return type(value) is int and 1 <= value <= MAX_NUMBER


def valid_day(value: object) -> bool:
    if type(value) is not str:
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def encode_reading(value: int | None) -> str | None:
    return None if value is None else hex(value)


def decode_reading(value: str | None) -> int | None:
    return None if value is None else int(value, 16)
