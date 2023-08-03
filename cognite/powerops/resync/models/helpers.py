import doctest
import re
import operator

from functools import reduce
from pprint import pformat, pprint
from deepdiff.model import PrettyOrderedSet
from typing import Any, Union, Type, get_args, get_origin
from types import GenericAlias


from cognite.client.data_classes import Relationship


def isinstance_list(value: Any, type_: Type):
    return isinstance(value, list) and value and isinstance(value[0], type_)


def match_field_from_relationship(model_fields: list[str], relationship: Relationship) -> str:
    if len(relationship.labels) != 1:
        raise ValueError(f"Expected one label in {relationship.labels=}")
    label = relationship.labels[0].external_id.split(".")[-1]

    candidates = list(filter(lambda k: label in k, model_fields))

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

def _get_from_deep_diff_path(deep_diff_path:str, lookup_model:dict):
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
    # TODO:! fix look up
    keys = [int(k) if k.isdigit() else k for k in deep_diff_path.replace("root[", "").removesuffix("]").split("][")]
    return reduce(operator.getitem, keys, lookup_model)





def format_value_added(deep_diff: PrettyOrderedSet, lookup_model: dict) -> list[str]:
    str_builder = []
    pprint(lookup_model)
    for _path in deep_diff:

        print(1)
        pprint(diff)
        print(2)
        pprint(type(diff))
        print(3)
        pprint(lookup_model.get(diff))
        print(4)
        pprint(type(lookup_model.get(diff)))
        # str_builder.extend(
        #     (
        #         f" * {diff.replace('root', '')}:\n",
        #         f"\t- {pformat(lookup_model.get(diff))}\n",
        #     )
        # )
    # for _path, removed in deep_diff.items():
    #     str_builder.extend(
    #         (
    #             f" * {_path.replace('root', '')}:\n",
    #             f"\t- {pformat(removed)}\n",
    #         )
    #     )
    return str_builder


def format_change_unary(
    deep_diff: PrettyOrderedSet,
    is_iterable: bool,
    look_up: dict[str, Any] = None,
) -> list[str]:
    """
    Formats a dict of changes to a list of strings

    TODO: doctest

    """
    print(f"format_change_unary, {is_iterable=}")
    print("deep_diff")
    pprint(deep_diff)
    pprint(type(deep_diff))
    print()
    str_builder = []
    for _path in deep_diff:
        # remove the index/key of which the item was added/removed in path
        _path = _path[: _path.rfind("[")] if is_iterable else _path
        change = look_up.get(_path)

        str_builder.extend((f" * {_path[:_path.rfind('[')].replace('root', '')}:\n", f"\t- {pformat(change)}\n"))
    return str_builder


if __name__ == "__main__":
    doctest.testmod()

    # @classmethod
    # def _field_diff_str_builder(
    #     cls,
    #     field_name: str,
    #     field_deep_diff: dict,
    #     # only valid when the fields are lists, which they are in ProductionModel
    #     self_affected_field: list[dict],
    # ) -> list[str]:
    #     str_builder = ["\n\n============= ", *field_name.title().split("_"), " =============\n"]
    #     # Might need a better fallback for names
    #     names = [
    #         f'{i}:{d.get("display_name", False) or d.get("name", "")}, ' for i, d in enumerate(self_affected_field)
    #     ]
    #     str_builder.extend(names)
    #     print(f"{names=}")
    #     str_builder.append("\n\n")
    #     print()
    #     print(f"{self_affected_field=}")

    #     for diff_type, diffs in field_deep_diff.items():
    #         if diff_type in ("type_changes", "values_changed"):
    #             str_builder.extend(
    #                 (
    #                     f'The following values have changed {"type" if "type" in diff_type else ""}:\n',
    #                     *format_change_binary(diffs),
    #                     "\n",
    #                 ),
    #             )

    #         elif "added" in diff_type or "removed" in diff_type:
    #             action = "added" if "added" in diff_type else "removed"
    #             is_iterable = "iterable" in diff_type
    #             str_builder.extend(
    #                 (
    #                     f"The following {'values' if is_iterable else 'entries'} have been {action}:\n",
    #                     *format_change_unary(diffs, is_iterable),
    #                     "\n",
    #                 )
    #             )

    #         else:
    #             print(f"cannot handle {diff_type=}")

    #     return str_builder
