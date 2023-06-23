import datetime
import operator
import re
from functools import reduce
from typing import Any


def format_deep_diff_path(path: str) -> str:
    return re.sub(r"['\[\]]", "", path.replace("][", ".").replace("root", ""))


def get_dict_dot_keys(data_dict: dict, dot_keys: str):
    # Getting nested keys dot separated. Handles  that
    # the yaml parser parses numeric keys as numbers
    # and datetime keys as datetime.datetime(...)
    # Not a complete solution:
    # Other datatypes need some handling might exist
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
