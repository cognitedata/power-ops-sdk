import datetime
import operator
import re
from functools import reduce
from typing import Any

from cognite.client.utils._time import datetime_to_ms


def format_deep_diff_path(path: str) -> str:
    """
    Formats from deepdiff path to dot separated path
    ```
    >>> format_deep_diff_path(path="root['object_type']['object_name']['attribute']['name']")
    'object_type.object_name.attribute.name'

    ```
    """
    return re.sub(r"['\[\]]", "", path.replace("][", ".").replace("root", ""))


def get_dict_dot_keys(data_dict: dict, dot_keys: str):
    """
    Get for arbitrarily nested keys that are dot separated.
    Handles that the yaml parser parses numeric keys as numbers
    and datetime keys as datetime.datetime(...)

    Solution is not complete,
    there might be other types that need to be handled

    Example:
    ```
    >>> get_dict_dot_keys(data_dict={"my_key": {"my_nested_key": 42,}}, dot_keys="my_key.my_nested_key")
    42

    ```
    """

    keys = [int(k) if k.isdigit() else k for k in dot_keys.split(".")]
    while "datetime" in keys:
        i = keys.index("datetime")
        ts_str_tuple = keys.pop(i + 1).replace("datetime(", "").replace(")", "")
        dt_ts = datetime.datetime(*[int(x) for x in ts_str_tuple.split(",")])
        keys[i] = dt_ts
    return reduce(operator.getitem, keys, data_dict)


def get_data_from_nested_dict(data_dict: dict, deep_diff_path: str):
    dot_keys = format_deep_diff_path(deep_diff_path)
    return get_dict_dot_keys(data_dict, dot_keys)


def is_time_series_dict(data: Any) -> bool:
    return (
        isinstance(data, dict)
        and all(isinstance(k, datetime.datetime) for k in data.keys())
        and all(isinstance(v, (float, int)) for v in data.values())
    )


def str_datetime_to_ms(str_datetime: str, str_format=None) -> int:
    """
    Convert a string datetime to milliseconds since epoch.
    If no format is provided, the function will try to guess the format.

    >>> str_datetime_to_ms("2021-01-01 00:00:00")
    1609459200000
    >>> str_datetime_to_ms("2021-01-01 00:00")
    1609459200000
    >>> str_datetime_to_ms("2021-01-01")
    1609459200000
    >>> str_datetime_to_ms("2021-01-01 00:00:00", str_format="%Y-%m-%d %H:%M:%S")
    1609459200000

    """
    DATE_FORMAT = "%Y-%m-%d"
    date_time_formats = {
        0: DATE_FORMAT,
        1: f"{DATE_FORMAT} %H:%M",
        2: f"{DATE_FORMAT} %H:%M:%S",
    }

    if not str_format:
        str_format = date_time_formats[str_datetime.count(":")]
    datetime_ = datetime.datetime.strptime(str_datetime, str_format)
    if datetime_.tzinfo is None:
        datetime_ = datetime_.replace(tzinfo=datetime.timezone.utc)
    return datetime_to_ms(datetime_)
