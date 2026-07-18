from __future__ import annotations

_UNSET_STRINGS = {"", "-", "(empty)"}


def coerce_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, str) and value in _UNSET_STRINGS:
        return 0.0
    return float(value)


def coerce_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, str) and value in _UNSET_STRINGS:
        return 0
    return int(float(value))


def coerce_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value == "T"
    return bool(value)


def coerce_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str) and value in _UNSET_STRINGS:
        return ""
    return str(value)
