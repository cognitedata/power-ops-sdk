from __future__ import annotations

import datetime

import arrow
from cognite.client.data_classes import filters
from cognite.client.utils._time import datetime_to_ms

from cognite.powerops.client.shop.shop_run import ShopRunEvent


def custom_contains_any(
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


def custom_time_filter(
    start_after: str | arrow.Arrow | datetime.datetime | None = None,
    start_before: str | arrow.Arrow | datetime.datetime | None = None,
    end_after: str | arrow.Arrow | datetime.datetime | None = None,
    end_before: str | arrow.Arrow | datetime.datetime | None = None,
) -> filters.Filter:
    """
    Generate a custom time filter for events.
    """
    # Validation of time bounds
    if not any((start_after, start_before, end_after, end_before)):
        raise ValueError("At least one time bound must be specified")

    if start_before and end_before and _time_to_ms(start_before) > _time_to_ms(end_before):
        raise ValueError(f"Events cannot occur after {start_before} and before {end_before}")

    if start_after and end_after and _time_to_ms(start_after) > _time_to_ms(end_after):
        raise ValueError(f"Events cannot occur after {start_after} and before {end_after}")

    # Generate filter(s)
    time_filter: list[filters.Filter] = []
    if start_after or start_before:
        time_filter.append(_get_time_range_filter("start_time", start_after, start_before))

    if end_after or end_before:
        time_filter.append(_get_time_range_filter("end_time", end_after, end_before))

    return time_filter[0] if len(time_filter) == 1 else filters.And(*time_filter)


def generate_shop_run_filters(
    watercourse: str | list[str] | None = None,
    source: str | list[str] | None = None,
    start_after: str | arrow.Arrow | datetime.datetime | None = None,
    start_before: str | arrow.Arrow | datetime.datetime | None = None,
    end_after: str | arrow.Arrow | datetime.datetime | None = None,
    end_before: str | arrow.Arrow | datetime.datetime | None = None,
) -> list[filters.Filter]:
    _filters = []
    if watercourse:
        _filters.append(custom_contains_any(["metadata", ShopRunEvent.watercourse], watercourse))
    if source:
        _filters.append(custom_contains_any("source", source))
    if any((start_after, start_before, end_after, end_before)):
        _filters.append(
            custom_time_filter(
                start_after=start_after,
                start_before=start_before,
                end_after=end_after,
                end_before=end_before,
            )
        )
    return _filters
