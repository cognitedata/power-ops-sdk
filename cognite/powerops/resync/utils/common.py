import json
import re
from hashlib import md5
from typing import Any, Type, TypeVar, overload, Optional

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


_T = TypeVar("_T")


@overload
def require(value: Optional[_T]) -> _T:
    ...


@overload
def require(value: Any, as_type: Type[_T]) -> _T:
    ...


def require(value, as_type=None):
    if value is None:
        raise ValueError("Value is required")
    if as_type is not None and not isinstance(value, as_type):
        raise TypeError(f"Expected type '{as_type}', got '{type(value)}'")
    return value
