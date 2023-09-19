import datetime
from collections.abc import Iterable
from typing import Optional, Union

import arrow
from cognite.client.data_classes import filters
from cognite.client.utils._time import datetime_to_ms


def custom_contains_any(keys: Union[str, Iterable[str]], values: Union[str, Iterable[str]]) -> filters.Filter:
    """
    Create a filter that checks if any of the keys are equal to any of the values.
    filters.ContainsAny, which does not support metadata key lookups.
    """
    if isinstance(values, str):
        values = [values]
    return filters.Or(*[filters.Equals(keys, v) for v in values])


def _time_to_ms(time: Union[str, arrow.Arrow, datetime.datetime]) -> int:
    """Convert a time to milliseconds since epoch"""
    if isinstance(time, str):
        time = arrow.get(time)
    if isinstance(time, arrow.Arrow):
        return datetime_to_ms(time.datetime)
    if isinstance(time, datetime.datetime):
        return datetime_to_ms(time)

    raise TypeError(f"Could not convert {time} to milliseconds")


def _time_range_filter(
    property: str,
    after_ms: Optional[int] = None,
    before_ms: Optional[int] = None,
) -> filters.Filter:
    """Helper that creates a range filter for a time property"""
    _range = {}
    if after_ms:
        _range["gt"] = after_ms
    if before_ms:
        _range["lt"] = before_ms
    return filters.Range(property=property, **_range)


def custom_time_filter(
    property: str,
    after: Optional[Union[str, arrow.Arrow, datetime.datetime]] = None,
    before: Optional[Union[str, arrow.Arrow, datetime.datetime]] = None,
) -> filters.Filter:
    """Create a filter that checks converts the event occurred within the range"""
    after_ms = _time_to_ms(after) if after else None
    before_ms = _time_to_ms(before) if before else None
    if after_ms and before_ms and after_ms > before_ms:
        raise ValueError(f"Events cannot occur after {after} and before {before}")

    return _time_range_filter(property, after_ms, before_ms)
