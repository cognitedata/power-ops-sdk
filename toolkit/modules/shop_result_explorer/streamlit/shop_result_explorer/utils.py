from typing import Any


def nested_get(_dict: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if _dict is None:
            return None
        _dict = _dict.get(key)
    return _dict


def filters_to_str(filters: list[str]) -> str:
    if len(filters) == 0:
        return ""
    if len(filters) == 1:
        return f"filter: {filters[0]}, "
    return "filter: {and: [" + ", ".join(filters) + "]}, "
