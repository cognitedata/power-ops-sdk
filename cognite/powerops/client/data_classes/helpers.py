import datetime
import operator
import re
from functools import reduce


def format_deep_diff_path(path: str) -> str:
    return re.sub(r"['\[\]]", "", path.replace("']['", ".").replace("root", ""))


def get_dict_dot_keys(data_dict: dict, dot_keys: str):
    # FIXME? Workaround for the nested keys without dot get and
    # the fact that the yaml parser parses numeric keys as numbers
    # and datetime keys as strings
    # Not a complete solution:
    # Other datatypes might also have dots in their string representation
    keys = [int(k) if k.isdigit() else k for k in dot_keys.split(".")]
    if "datetime" in keys:
        i = keys.index("datetime")
        ts_str_tuple = keys.pop(i + 1).replace("datetime(", "").replace(")", "")
        dt_ts = datetime.datetime(*[int(x) for x in ts_str_tuple.split(",")])
        keys[i] = dt_ts
    return reduce(operator.getitem, keys, data_dict)


def get_data_from_nested_dict(data_dict: dict, deep_diff_path: str):
    dot_keys = format_deep_diff_path(deep_diff_path)
    return get_dict_dot_keys(data_dict, dot_keys)
