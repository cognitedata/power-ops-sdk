import doctest
from pprint import pformat
from typing import Any, Union, Type

# from cognite.client.data_classes import LabelFilter

def isinstance_list(value: Any, type_: Type):
    return isinstance(value, list) and value and isinstance(value[0], type_)

# def _labels_contains_any(label_ext_ids: list[str]) -> LabelFilter:
#         return {"containsAny": [{"externalId": ext_id} for ext_id in label_ext_ids]}

def format_change_binary(
    deep_diff: dict[str, dict],
) -> list[str]:
    """
    Formats a dict of changes to a list of strings

    >>> deep_diff = {
    ...     "root[0][plants][name]": {
    ...         "old_value": "Old name",
    ...         "new_value": "New name",
    ...     },
    ... }
    >>> format_change_binary(deep_diff)
    [' * [0][plants][name]:\n', '\t- Old name\t', ' -->   ','New name\n', '\n']
    """
    str_builder = []
    for path_, change_dict in deep_diff.items():
        str_builder.extend(
            (
                f" * {path_.replace('root', '') }:\n",
                f'\t- {pformat(change_dict.get("old_value"))}\t',
                "  -->   ",
                f'{pformat(change_dict.get("new_value"))}\n',
                "\n",
            )
        )
    return str_builder


def format_change_unary(
    deep_diff: dict[str, list[Union[str, dict]]],
    is_iterable: bool,
) -> list[str]:
    """
    Formats a dict of changes to a list of strings

    TODO: doctest

    """
    str_builder = []
    for _path, change in deep_diff.items():
        # remove the index/key of which the item was added/removed in path
        _path = _path[: _path.rfind("[")] if is_iterable else _path

        str_builder.extend((f" * {_path[:_path.rfind('[')].replace('root', '')}:\n", f"\t- {pformat(change)}\n"))
    return str_builder


if __name__ == "__main__":
    doctest.testmod()
