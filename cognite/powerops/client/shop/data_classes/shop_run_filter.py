from __future__ import annotations

import datetime

import arrow
from cognite.client.data_classes import filters
from cognite.client.utils._time import datetime_to_ms

from cognite.powerops.client.shop.data_classes.shop_run import SHOPRunEvent


def _custom_contains_any(
    keys: str | list[str],
    values: str | list[str],
) -> filters.Filter:
    """
    Create a filter that checks if any of the keys are equal to any of the values.
    filters.ContainsAny, which does not support metadata key lookups.
    """
    if isinstance(values, str):
        values = [values]
    return filters.Or(*[filters.Equals(keys, v) for v in values])


def _time_to_ms(time: str | arrow.Arrow | datetime.datetime) -> int:
    """Convert a time to milliseconds since epoch"""
    if isinstance(time, str):
        time = arrow.get(time)
    if isinstance(time, arrow.Arrow):
        return datetime_to_ms(time.datetime)
    if isinstance(time, datetime.datetime):
        return datetime_to_ms(time)

    raise TypeError(f"Could not convert {time} to milliseconds")


def _get_time_range_filter(
    property: str,
    after: str | arrow.Arrow | datetime.datetime | None = None,
    before: str | arrow.Arrow | datetime.datetime | None = None,
) -> filters.Filter:
    """Helper that converts and checks validity of time abounds"""
    after_ms = _time_to_ms(after) if after else None
    before_ms = _time_to_ms(before) if before else None
    if after_ms and before_ms and after_ms > before_ms:
        raise ValueError(f"Events cannot occur after {after} and before {before}")

    _range = {}
    if after_ms:
        _range["gt"] = after_ms
    if before_ms:
        _range["lt"] = before_ms
    return filters.Range(property=property, **_range)


class SHOPRunFilter:
    def __init__(
        self,
        watercourse: str | list[str] | None = None,
        source: str | list[str] | None = None,
        start_after: str | arrow.Arrow | datetime.datetime | None = None,
        start_before: str | arrow.Arrow | datetime.datetime | None = None,
        end_after: str | arrow.Arrow | datetime.datetime | None = None,
        end_before: str | arrow.Arrow | datetime.datetime | None = None,
    ):
        self.watercourse = watercourse
        self.source = source
        self.start_after = start_after
        self.start_before = start_before
        self.end_after = end_after
        self.end_before = end_before

    def _create_time_filters(self) -> filters.Filter:
        """Helper that converts and checks validity of time abounds"""

        if self.start_after and self.start_before and _time_to_ms(self.start_after) > _time_to_ms(self.start_before):
            raise ValueError(f"Events cannot start after {self.start_after} and before {self.start_before}")

        if self.end_after and self.end_before and _time_to_ms(self.end_after) > _time_to_ms(self.end_before):
            raise ValueError(f"Events cannot end after {self.end_after} and before {self.end_before}")

        _filter: list[filters.Filter] = []
        if self.start_after or self.start_before:
            _filter.append(_get_time_range_filter("start_time", self.start_after, self.start_before))

        if self.end_after or self.end_before:
            _filter.append(_get_time_range_filter("end_time", self.end_after, self.end_before))

        return _filter[0] if len(_filter) == 1 else filters.And(*_filter)

    def get_filters(self) -> list[filters.Filter]:
        _filters: list[filters.Filter] = []
        if self.watercourse:
            _filters.append(_custom_contains_any(["metadata", SHOPRunEvent.watercourse], self.watercourse))
        if self.source:
            _filters.append(_custom_contains_any("source", self.source))
        if any((self.start_after, self.start_before, self.end_after, self.end_before)):
            _filters.append(self._create_time_filters())
        return _filters
