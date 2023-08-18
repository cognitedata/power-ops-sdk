import doctest
import operator
from collections.abc import Iterable

from functools import reduce
from pprint import pformat
from deepdiff.model import PrettyOrderedSet
from typing import Any, Union, Type, get_args, get_origin
from types import GenericAlias


from cognite.client.data_classes import Relationship


def isinstance_list(value: Any, type_: Type):
    return isinstance(value, list) and value and isinstance(value[0], type_)


def match_field_from_relationship(model_fields: Iterable[str], relationship: Relationship) -> str:
    """Find the field on the model that matches the relationship using the label"""
    if len(relationship.labels) != 1:
        raise ValueError(f"Expected one label in {relationship.labels=}")
    label = relationship.labels[0].external_id.split(".")[-1]

    # Market labels sometimes have a suffix of _{target_type}
    candidates = [
        field for field in model_fields if label in field or label.removesuffix(f"_{relationship.target_type}") in field
    ]

    if len(candidates) != 1:
        raise ValueError(f"Could not match {relationship.external_id=} to {model_fields=}")

    return candidates[0]


def pydantic_model_class_candidate(class_: type = None) -> bool:
    """GenericAlias is a potential list, get origin checks an optional field"""
    return isinstance(class_, GenericAlias) or (get_origin(class_) is Union and type(None) in get_args(class_))


def format_change_binary(
    deep_diff: dict[str, dict],
) -> list[str]:
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
        str_builder.extend(
            (
                f" * {_path.replace('root', '')}:\n",
                f"\t- {pformat(removed)}\n",
            )
        )
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


def format_value_added(deep_diff: PrettyOrderedSet, lookup_model: dict) -> list[str]:
    """
    Formats a dict of values that were added to a list of strings
    The deep_diff does not contain the new value, so it is fetched from the lookup_model
    """
    str_builder = []
    _path: str = None  # type: ignore
    for _path in deep_diff:
        str_builder.extend(
            (
                f" * {_path.replace('root', '')}:\n",
                f"\t- {pformat(_get_from_deep_diff_path(_path, lookup_model))}\n",
            )
        )
    return str_builder


if __name__ == "__main__":
    doctest.testmod()
