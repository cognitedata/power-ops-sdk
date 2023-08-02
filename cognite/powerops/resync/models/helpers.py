import doctest
from pprint import pformat
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
    Formats a dict of changes to a list of strings

    # >>> deep_diff = {
    # ...     "root[0][plants][name]": {
    # ...         "old_value": "Old name",
    # ...         "new_value": "New name",
    # ...     },
    # ... }
    # >>> format_change_binary(deep_diff)
    # [' * [0][plants][name]:\n', '\t- Old name\t', ' -->   ','New name\n', '\n']
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
