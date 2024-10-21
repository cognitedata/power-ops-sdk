import datetime
import operator
import re
from functools import reduce
from typing import Any, Union

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


def get_dict_dot_keys(data_dict: dict, dot_keys: str) -> Union[int, str, datetime.datetime, Any]:
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
    # TODO: This function should be simplified. It doesn't pass mypy checks, which have temporarily been disabled.

    keys = [int(k) if k.isdigit() else k for k in dot_keys.split(".")]
    while "datetime" in keys:
        i = keys.index("datetime")
        ts_str = str(keys.pop(i + 1))
        ts_str_tuple = ts_str.replace("datetime(", "").replace(")", "")
        dt_ts = datetime.datetime(*[int(x) for x in ts_str_tuple.split(",")])  # type: ignore[arg-type]
        keys[i] = dt_ts  # type: ignore[call-overload]
    return reduce(operator.getitem, keys, data_dict)


def get_data_from_nested_dict(data_dict: dict, deep_diff_path: str) -> Union[int, str, datetime.datetime, Any]:
    dot_keys = format_deep_diff_path(deep_diff_path)
    return get_dict_dot_keys(data_dict, dot_keys)


def is_time_series_dict(data: Any) -> bool:
    return (
        isinstance(data, dict)
        and all(isinstance(k, datetime.datetime) for k in data.keys())
        and all(isinstance(v, (float | int)) for v in data.values())
    )


def str_datetime_to_ms(str_datetime: str, str_format: Union[str, None] = None) -> int:
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
    date_format = "%Y-%m-%d"
    date_time_formats = {
        0: date_format,
        1: f"{date_format} %H:%M",
        2: f"{date_format} %H:%M:%S",
    }

    if not str_format:
        str_format = date_time_formats[str_datetime.count(":")]
    datetime_ = datetime.datetime.strptime(str_datetime, str_format)
    if datetime_.tzinfo is None:
        datetime_ = datetime_.replace(tzinfo=datetime.timezone.utc)
    return datetime_to_ms(datetime_)
