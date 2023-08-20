# TODO DRY up with power-ops-functions

from typing import overload, TypeVar, Optional, Any, Type

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
