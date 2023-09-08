import doctest
import operator

from functools import reduce
from pprint import pformat
from typing import Any, Type


def isinstance_list(value: Any, type_: Type):
    return isinstance(value, list) and value and isinstance(value[0], type_)


def format_change_binary(deep_diff: dict[str, dict]) -> list[str]:
    """
    Formats a dict of changes with updated values to a list of strings

    >>> deep_diff = {
    ...     "root[0][plants][name]": {
    ...         "old_value": "Old name",
    ...         "new_value": "New name",
    ...     },
    ... }
    >>> format_change_binary(deep_diff)
    [' * [0][plants][name]:\\n', "\\t- 'Old name'\\t", '  -->   ', "'New name'", '\\n']
    """
    str_builder = []
    for path_, change_dict in deep_diff.items():
        str_builder.extend(
            (
                f" * {path_.replace('root', '') }:\n",
                f'\t- {pformat(change_dict.get("old_value"))}\t',
                "  -->   ",
                f'{pformat(change_dict.get("new_value"))}',
                "\n",
            )
        )
    return str_builder


def format_value_removed(deep_diff: dict[str, dict]) -> list[str]:
    """
    Formats a dict of values that were removed to a list of strings

    >>> deep_diff = {
    ...     "root[0]": {
    ...         "description": "None",
    ...         "name": "Name"
    ...     },
    ... }
    >>> format_value_removed(deep_diff)
    [' * [0]:\\n', "\\t- {'description': 'None', 'name': 'Name'}\\n"]

    """
    str_builder = []
    for _path, removed in deep_diff.items():
        str_builder.extend((f" * {_path.replace('root', '')}:\n", f"\t- {pformat(removed)}\n"))
    return str_builder


def _get_from_deep_diff_path(deep_diff_path: str, lookup_model: dict) -> Any:
    """
    Similar to `format_deep_diff_path` and `get_dict_dot_keys` in
    `cognite.powerops.clients.shop.data_classes.helpers` but modified
    to work with the deepdiff format separated from yaml formats

    >>> _path = "root['key_1'][0]['key_3']"
    >>> _lookup_model = {
    ...    "key_1": [{"key_2": "value_2", "key_3": "value_3"}]
    ... }
    >>> _get_from_deep_diff_path(_path, _lookup_model)
    'value_3'

    """
    keys = [
        int(k) if k.isdigit() else k
        for k in deep_diff_path.replace("root[", "").replace("'", "").removesuffix("]").split("][")
    ]
    try:
        item = reduce(operator.getitem, keys, lookup_model)
    except KeyError:
        item = f"Could not retrieve at {deep_diff_path}"
    return item


if __name__ == "__main__":
    doctest.testmod()
