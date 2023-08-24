import abc
import inspect
import json
import re
from hashlib import md5
from typing import Any, Type, TypeVar

from cognite.powerops.clients.data_classes._core import DomainModelApply


def special_case_handle_gate_number(name: str) -> None:
    """Must handle any gate numbers above 1 if found in other sources than YAML"""
    # TODO: extend to handle special case if needed
    if re.search(pattern=r"L[2-9]", string=name):
        print_warning(f"Potential gate {name} not in YAML!")


def print_warning(s: str) -> None:
    """Adds some nice colors to the printed text :)"""
    print(f"\033[91m[WARNING] {s}\033[0m")


def make_ext_id(arg: Any, class_: Type[DomainModelApply]) -> str:
    hash_value = md5()
    if isinstance(arg, (str, int, float, bool)):
        hash_value.update(str(arg).encode())
    elif isinstance(arg, (list, dict, tuple)):
        hash_value.update(json.dumps(arg).encode())
    return f"{class_.__name__.removesuffix('Apply')}__{hash_value.hexdigest()}"


T_Type = TypeVar("T_Type", bound=type)


def all_subclasses(base: T_Type) -> list[T_Type]:
    """Returns a list (without duplicates) of all subclasses of a given class, sorted on import-path-name.
    Ignores classes not part of the main library, e.g. subclasses part of tests.
    """
    return sorted(
        filter(
            lambda sub: str(sub).startswith("<class 'cognite.powerops"),
            set(base.__subclasses__()).union(s for c in base.__subclasses__() for s in all_subclasses(c)),
        ),
        key=str,
    )


def all_concrete_subclasses(base: T_Type) -> list[T_Type]:
    return [
        sub
        for sub in all_subclasses(base)
        if all(base is not abc.ABC for base in sub.__bases__) and not inspect.isabstract(sub)
    ]
